from __future__ import annotations

import sqlite3
import threading

from orbitbridge import audit, capacity, catalog, handoff, ingest, protocol, quality, reconcile, store
from helpers import DatabaseCase, STAMP, observation, packet


def observation_n(identifier: str, *, severity: int = 4, confidence: str = "0.8", region: str = "north") -> dict:
    return observation(identifier, severity=severity, confidence=confidence, region=region)


class CapacityPolicyTests(DatabaseCase):
    def test_no_policy_means_unlimited_and_backward_compatible(self):
        result = ingest.ingest_bytes(self.db, packet(), when=STAMP)
        self.assertEqual((result.handoff_count, result.deferred_count), (1, 0))
        status = capacity.region_capacity_status(self.db, "north")
        self.assertIsNone(status["max_pending"])
        self.assertIsNone(status["available"])
        self.assertEqual(status["pending"], 1)

    def test_set_region_capacity_validates_operator_and_limit(self):
        with self.assertRaises(capacity.CapacityError) as error:
            capacity.set_region_capacity(self.db, "north", 2, "  ")
        self.assertEqual(error.exception.code, "operator")
        with self.assertRaises(capacity.CapacityError):
            capacity.set_region_capacity(self.db, "north", -1, "op")
        with self.assertRaises(capacity.CapacityError):
            capacity.set_region_capacity(self.db, "bad region!", 1, "op")

    def test_policy_change_is_recorded_in_activity_audit_trail(self):
        capacity.set_region_capacity(self.db, "north", 2, "值守员甲", when=STAMP)
        capacity.set_region_capacity(self.db, "north", 3, "值守员乙", when=STAMP)
        events = audit.list_activity(self.db, entity_type="region", entity_id="north")
        self.assertEqual([event["event_type"] for event in events],
                         ["region_capacity_changed", "region_capacity_changed"])
        self.assertEqual(events[1]["details"]["max_pending"], 3)
        self.assertEqual(events[1]["details"]["operator"], "值守员乙")

    def test_subscription_capacity_requires_existing_subscription(self):
        with self.assertRaises(capacity.CapacityError) as error:
            capacity.set_subscription_capacity(self.db, "missing", 1, "op")
        self.assertEqual(error.exception.code, "subscription_missing")

    def test_remove_policy_returns_to_unlimited_and_drains(self):
        capacity.set_region_capacity(self.db, "north", 0, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        self.assertEqual(store.counts(self.db)["handoffs"], 0)
        outcome = capacity.remove_region_capacity(self.db, "north", "op", when=STAMP)
        self.assertEqual(outcome["released"], 1)
        self.assertEqual(store.counts(self.db)["handoffs"], 1)
        self.assertIsNone(capacity.region_capacity_status(self.db, "north")["max_pending"])

    def test_remove_unknown_policy_is_rejected(self):
        with self.assertRaises(capacity.CapacityError) as error:
            capacity.remove_region_capacity(self.db, "north", "op")
        self.assertEqual(error.exception.code, "capacity_missing")


class DeferralTests(DatabaseCase):
    def _full_region(self, limit: int = 0):
        capacity.set_region_capacity(self.db, "north", limit, "op", when=STAMP)

    def test_full_capacity_accepts_packet_but_defers_routing(self):
        self._full_region()
        result = ingest.ingest_bytes(self.db, packet(), when=STAMP)
        self.assertEqual(result.status, "accepted")
        self.assertEqual((result.handoff_count, result.deferred_count), (0, 1))
        # 证据完整：批次、观测、游标全部正常提交
        self.assertEqual(store.counts(self.db)["batches"], 1)
        self.assertEqual(store.counts(self.db)["observations"], 1)
        self.assertEqual(catalog.station(self.db, "sat-a")["committed_sequence"], 1)
        self.assertEqual(store.counts(self.db)["handoffs"], 0)
        self.assertEqual(store.counts(self.db)["quarantine"], 0)
        waiting = capacity.list_deferred(self.db)
        self.assertEqual(len(waiting), 1)
        self.assertEqual(waiting[0]["status"], "waiting")
        self.assertEqual(waiting[0]["reason_code"], "region_full")
        self.assertEqual(waiting[0]["external_id"], "sim-1")
        self.assertEqual(waiting[0]["packet_id"], result.packet_id)

    def test_deferred_and_quarantined_packages_are_separate(self):
        self._full_region()
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        with self.assertRaises(ingest.IngestError):
            ingest.ingest_bytes(self.db, packet(3), when=STAMP)
        self.assertEqual(store.counts(self.db)["deferred_routings"], 1)
        self.assertEqual(store.counts(self.db)["quarantine"], 1)
        quarantined = audit.list_quarantine(self.db)
        self.assertEqual(quarantined[0]["reason_code"], "sequence_gap")
        self.assertEqual(capacity.list_deferred(self.db)[0]["status"], "waiting")

    def test_partial_capacity_fills_then_defers_within_one_batch(self):
        capacity.set_region_capacity(self.db, "north", 1, "op", when=STAMP)
        rows = [observation_n("low", severity=3, confidence="0.75"),
                observation_n("high", severity=5, confidence="0.99")]
        result = ingest.ingest_bytes(self.db, packet(records=rows), when=STAMP)
        self.assertEqual((result.handoff_count, result.deferred_count), (1, 1))
        handoff_row = handoff.list_handoffs(self.db)[0]
        self.assertEqual(handoff_row["external_id"], "high")
        deferred = capacity.list_deferred(self.db)[0]
        self.assertEqual(deferred["external_id"], "low")

    def test_high_risk_observation_is_not_lost_behind_lower_risk(self):
        capacity.set_region_capacity(self.db, "north", 1, "op", when=STAMP)
        # 先占满唯一名额的是低风险观测
        ingest.ingest_bytes(self.db, packet(records=[observation_n("low", severity=3, confidence="0.75")]),
                            when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [observation_n("high", severity=5, confidence="0.99")]),
                            when=STAMP)
        self.assertEqual(len(capacity.list_deferred(self.db, status="waiting")), 1)
        handoff.acknowledge(self.db, 1, "op", when=STAMP)
        released = capacity.list_deferred(self.db, status="dispatched")
        self.assertEqual(len(released), 1)
        self.assertEqual(released[0]["external_id"], "high")
        pending = handoff.list_handoffs(self.db, status="pending")
        self.assertEqual([row["external_id"] for row in pending], ["high"])

    def test_subscription_limit_and_region_limit_report_distinct_reasons(self):
        capacity.set_region_capacity(self.db, "north", 0, "op", when=STAMP)
        capacity.set_subscription_capacity(self.db, "north-watch", 0, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        self.assertEqual(capacity.list_deferred(self.db)[0]["reason_code"], "both_full")
        capacity.remove_region_capacity(self.db, "north", "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [observation("sim-2")]), when=STAMP)
        reasons = {row["external_id"]: row["reason_code"] for row in capacity.list_deferred(self.db)}
        self.assertEqual(reasons["sim-2"], "subscription_full")

    def test_zero_limit_blocks_new_handoffs_but_keeps_evidence(self):
        capacity.set_subscription_capacity(self.db, "north-watch", 0, "op", when=STAMP)
        result = ingest.ingest_bytes(self.db, packet(), when=STAMP)
        self.assertEqual(result.handoff_count, 0)
        self.assertEqual(audit.region_report(self.db, "north")["observations"], 1)

    def test_multiple_subscriptions_allocate_capacity_independently(self):
        catalog.register_subscription(self.db, "backup", "north", 1, "0.1", when=STAMP)
        capacity.set_subscription_capacity(self.db, "north-watch", 0, "op", when=STAMP)
        result = ingest.ingest_bytes(self.db, packet(), when=STAMP)
        # north-watch 关闭 -> 暂缓；backup 无限制 -> 正常交接，互不影响
        self.assertEqual((result.handoff_count, result.deferred_count), (1, 1))
        deferred = capacity.list_deferred(self.db)
        self.assertEqual([row["subscription_id"] for row in deferred], ["north-watch"])
        self.assertEqual(handoff.list_handoffs(self.db)[0]["subscription_id"], "backup")
        # 放开 north-watch 后暂缓项恢复，同观测两个订阅各有一条交接，无重复
        capacity.remove_subscription_capacity(self.db, "north-watch", "op", when=STAMP)
        self.assertEqual(len(handoff.list_handoffs(self.db)), 2)
        self.assertEqual(len(capacity.list_deferred(self.db, status="waiting")), 0)
        self.assertTrue(quality.check_all(self.db)["ok"])

    def test_replay_of_deferred_batch_is_still_idempotent(self):
        self._full_region()
        first = ingest.ingest_bytes(self.db, packet(), when=STAMP)
        second = ingest.ingest_bytes(self.db, packet(), when=STAMP)
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.batch_id, first.batch_id)
        self.assertEqual(store.counts(self.db)["deferred_routings"], 1)
        self.assertEqual(store.counts(self.db)["handoffs"], 0)


class RecoveryTests(DatabaseCase):
    def test_acknowledgement_releases_capacity_and_dispatches_waiting(self):
        capacity.set_region_capacity(self.db, "north", 1, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(records=[observation_n("a")]), when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [observation_n("b", severity=3)]), when=STAMP)
        self.assertEqual(len(capacity.list_deferred(self.db, status="waiting")), 1)
        ack = handoff.acknowledge(self.db, 1, "值守员甲", when=STAMP)
        self.assertEqual(ack["capacity_released"], 1)
        self.assertEqual(len(capacity.list_deferred(self.db, status="waiting")), 0)
        dispatched = capacity.list_deferred(self.db, status="dispatched")[0]
        self.assertEqual(dispatched["handoff_id"], 2)
        self.assertEqual(dispatched["released_at"], STAMP)
        self.assertEqual(handoff.list_handoffs(self.db, status="pending")[0]["handoff_id"], 2)

    def test_relaxed_policy_drains_waiting_routings(self):
        capacity.set_region_capacity(self.db, "north", 0, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [observation("sim-2")]), when=STAMP)
        outcome = capacity.set_region_capacity(self.db, "north", 5, "op", when=STAMP)
        self.assertEqual(outcome["released"], 2)
        self.assertEqual(outcome["waiting"], 0)
        self.assertEqual(store.counts(self.db)["handoffs"], 2)

    def test_drain_respects_risk_priority_then_arrival_fairness(self):
        capacity.set_region_capacity(self.db, "north", 0, "op", when=STAMP)
        # 同区域按序号连续到达：低风险先到、高风险后到
        ingest.ingest_bytes(self.db, packet(records=[observation_n("low-1", severity=3, confidence="0.75")]),
                            when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [observation_n("high", severity=5, confidence="0.95")]),
                            when=STAMP)
        ingest.ingest_bytes(self.db, packet(3, [observation_n("low-2", severity=3, confidence="0.75")]),
                            when=STAMP)
        capacity.set_region_capacity(self.db, "north", 2, "op", when=STAMP)
        pending = [row["external_id"] for row in handoff.list_handoffs(self.db, status="pending")]
        self.assertEqual(pending, ["high", "low-1"])
        remaining = [row["external_id"] for row in capacity.list_deferred(self.db, status="waiting")]
        self.assertEqual(remaining, ["low-2"])

    def test_explicit_drain_is_idempotent_when_blocked(self):
        capacity.set_region_capacity(self.db, "north", 0, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        first = capacity.drain_deferred(self.db, when=STAMP)
        second = capacity.drain_deferred(self.db, when=STAMP)
        self.assertEqual((first["released"], second["released"]), (0, 0))
        self.assertEqual(first["waiting_before"], 1)

    def test_drain_scoped_to_subscription_does_not_touch_others(self):
        catalog.register_subscription(self.db, "backup", "north", 1, "0.1", when=STAMP)
        # 区域不限，仅两个订阅组各自关闭：同一条观测对两个订阅的路由都暂缓
        capacity.set_subscription_capacity(self.db, "north-watch", 0, "op", when=STAMP)
        capacity.set_subscription_capacity(self.db, "backup", 0, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        self.assertEqual(len(capacity.list_deferred(self.db, status="waiting")), 2)
        # 只放开 backup 的容量（策略变更会按该订阅作用域自动派单），north-watch 不受影响
        capacity.set_subscription_capacity(self.db, "backup", 5, "op", when=STAMP)
        waiting = capacity.list_deferred(self.db, status="waiting")
        self.assertEqual([row["subscription_id"] for row in waiting], ["north-watch"])
        # 显式按 backup 作用域再派单：没有属于它的等待项，返回 0，证明作用域不会越界
        self.assertEqual(capacity.drain_deferred(self.db, subscription_id="backup", when=STAMP)["released"], 0)
        # 再放开 north-watch：移除策略本身会按该订阅作用域自动派单
        removed = capacity.remove_subscription_capacity(self.db, "north-watch", "op", when=STAMP)
        self.assertEqual(removed["released"], 1)
        self.assertEqual(len(capacity.list_deferred(self.db, status="waiting")), 0)

    def test_inactive_subscription_keeps_routing_waiting_until_reactivated(self):
        capacity.set_region_capacity(self.db, "north", 0, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        catalog.set_subscription_active(self.db, "north-watch", False)
        capacity.remove_region_capacity(self.db, "north", "op", when=STAMP)
        self.assertEqual(len(capacity.list_deferred(self.db, status="waiting")), 1)
        catalog.set_subscription_active(self.db, "north-watch", True)
        outcome = capacity.drain_deferred(self.db, when=STAMP)
        self.assertEqual(outcome["released"], 1)

    def test_multiple_stations_share_region_capacity_fairly(self):
        catalog.register_station(self.db, "sat-b", "合成观测站 B", when=STAMP)
        capacity.set_region_capacity(self.db, "north", 1, "op", when=STAMP)
        # 站点 A 先占满名额；站点 B 的同区域观测必须暂缓而非拒收，游标照常推进
        ingest.ingest_bytes(self.db, packet(station="sat-a"), when=STAMP)
        result_b = ingest.ingest_bytes(self.db, packet(1, station="sat-b"), when=STAMP)
        self.assertEqual(result_b.status, "accepted")
        self.assertEqual(result_b.deferred_count, 1)
        self.assertEqual(catalog.station(self.db, "sat-b")["committed_sequence"], 1)
        handoff.acknowledge(self.db, 1, "op", when=STAMP)
        self.assertEqual(len(handoff.list_handoffs(self.db, status="pending")), 1)
        self.assertEqual(handoff.list_handoffs(self.db, status="pending")[0]["station_id"], "sat-b")


class CapacityAuditTests(DatabaseCase):
    def test_quality_flags_duplicate_routing_and_stuck_deferral(self):
        capacity.set_region_capacity(self.db, "north", 0, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        # 干净状态：暂缓是容量造成的，活性检查不报错
        self.assertTrue(quality.check_all(self.db)["ok"])
        # 手工破坏：容量已空闲却不恢复，应被活性不变量捕获
        self.db.execute("DELETE FROM region_capacities")
        report = quality.check_all(self.db)
        self.assertFalse(report["ok"])
        self.assertIn("deferred_stuck", {item["code"] for item in report["issues"]})

    def test_quality_detects_duplicate_dispatch(self):
        capacity.set_region_capacity(self.db, "north", 0, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        # 绕过应用层：暂缓项仍在等待时手工插入交接项，制造重复派单
        self.db.execute("INSERT INTO handoffs(observation_id,subscription_id,status,created_at) "
                        "SELECT observation_id,subscription_id,'pending',created_at FROM deferred_routings")
        codes = {item["code"] for item in quality.check_all(self.db)["issues"]}
        self.assertIn("routing_duplicate", codes)

    def test_verify_lineage_links_dispatched_handoff_to_batch(self):
        capacity.set_region_capacity(self.db, "north", 0, "op", when=STAMP)
        result = ingest.ingest_bytes(self.db, packet(), when=STAMP)
        capacity.remove_region_capacity(self.db, "north", "op", when=STAMP)
        issues = audit.verify_lineage(self.db)
        self.assertEqual(issues, [])
        detail = audit.batch_detail(self.db, result.batch_id)
        self.assertEqual(len(detail["handoffs"]), 1)

    def test_reconcile_reports_waiting_and_dispatched_routings(self):
        capacity.set_region_capacity(self.db, "north", 1, "op", when=STAMP)
        first = ingest.ingest_bytes(self.db, packet(records=[observation_n("a")]), when=STAMP)
        ingest.ingest_bytes(self.db, packet(2, [observation_n("b")]), when=STAMP)
        batch_report = reconcile.batch_counts(self.db, first.batch_id)
        self.assertEqual(batch_report["routed_total"], 1)
        sub_report = reconcile.subscription_counts(self.db, "north-watch")
        self.assertEqual(sub_report["waiting"], 1)
        handoff.acknowledge(self.db, 1, "op", when=STAMP)
        sub_report = reconcile.subscription_counts(self.db, "north-watch")
        self.assertEqual((sub_report["waiting"], sub_report["dispatched"]), (0, 1))
        day = reconcile.day_counts(self.db, "2026-10-28")
        self.assertEqual(day["deferred_created"], 1)

    def test_region_and_subscription_status_show_available_capacity(self):
        capacity.set_region_capacity(self.db, "north", 3, "op", when=STAMP)
        capacity.set_subscription_capacity(self.db, "north-watch", 2, "op", when=STAMP)
        ingest.ingest_bytes(self.db, packet(), when=STAMP)
        region = capacity.region_capacity_status(self.db, "north")
        self.assertEqual((region["pending"], region["available"], region["waiting"]), (1, 2, 0))
        sub = capacity.subscription_capacity_status(self.db, "north-watch")
        self.assertEqual((sub["pending"], sub["available"]), (1, 1))
        summary = handoff.subscription_summary(self.db, "north-watch")
        self.assertEqual(summary["waiting"], 0)

    def test_capacity_policies_listing_includes_both_layers(self):
        capacity.set_region_capacity(self.db, "north", 3, "op", when=STAMP)
        capacity.set_subscription_capacity(self.db, "north-watch", 2, "op", when=STAMP)
        policies = capacity.list_capacity_policies(self.db)
        self.assertEqual(policies["regions"][0]["region"], "north")
        self.assertEqual(policies["subscriptions"][0]["max_pending"], 2)
        self.assertEqual(policies["subscriptions"][0]["region"], "north")


class CapacityConcurrencyTests(DatabaseCase):
    def test_concurrent_ingests_serialize_without_losing_or_duplicating_routings(self):
        capacity.set_region_capacity(self.db, "north", 2, "op", when=STAMP)
        errors: list[Exception] = []

        def worker(station: str) -> None:
            try:
                conn = store.open_database(self.db_path)
                try:
                    catalog.register_station(conn, station, f"站点 {station}", when=STAMP)
                    for sequence in range(1, 4):
                        ingest.ingest_bytes(
                            conn, packet(sequence, [observation_n(f"{station}-{sequence}")], station=station),
                            when=STAMP)
                finally:
                    conn.close()
            except Exception as exc:  # pragma: no cover - 失败时由断言呈现
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(name,))
                   for name in ("sat-b", "sat-c", "sat-d")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        # 6 个包全部提交，每个站点游标都推进到 3，无游标倒退
        for station in ("sat-b", "sat-c", "sat-d"):
            self.assertEqual(catalog.station(self.db, station)["committed_sequence"], 3)
        total_routes = store.counts(self.db)["handoffs"] + store.counts(self.db)["deferred_routings"]
        self.assertEqual(total_routes, 9)
        # 容量上限在任何时刻都未被突破
        self.assertLessEqual(
            self.db.execute("SELECT COUNT(*) FROM handoffs WHERE status='pending'").fetchone()[0], 2)
        # 无重复路由：waiting 暂缓项不得与交接项共存
        duplicates = self.db.execute(
            "SELECT d.deferred_id FROM deferred_routings d JOIN handoffs h "
            "ON h.observation_id=d.observation_id AND h.subscription_id=d.subscription_id "
            "WHERE d.status='waiting'").fetchall()
        self.assertEqual(duplicates, [])
        # 恢复后所有暂缓都能派单，证据零丢失
        capacity.remove_region_capacity(self.db, "north", "op", when=STAMP)
        self.assertEqual(store.counts(self.db)["deferred_routings"],
                         len(capacity.list_deferred(self.db, status="dispatched")))
        self.assertTrue(quality.check_all(self.db)["ok"])
