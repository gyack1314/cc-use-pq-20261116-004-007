"""可核对的容量策略、暂缓交接路由与恢复派单。

容量边界有两层：区域待办上限（region_capacities）与订阅组待办上限
（subscription_capacities）。没有策略行表示该层不限；max_pending=0 表示显式
关闭。容量只统计 handoffs 中 status='pending' 的交接项。

容量不足绝不拒收观测包：批次、观测与站点游标照常提交，匹配但拿不到名额的
路由写入 deferred_routings（status='waiting'），与 quarantine 中的真正拒收
物理分离。容量恢复后由 drain_deferred 按风险优先、同风险按到达次序派单。
"""
from __future__ import annotations

import sqlite3
from typing import Any

from . import catalog, store
from .normalize import REGIONS

# 风险优先排序：高等级、高置信度优先；同风险按观测时间与稳定主键保证确定性。
_PRIORITY_ORDER = ("o.severity DESC", "CAST(o.confidence AS REAL) DESC",
                   "o.observed_at", "o.observation_id", "s.subscription_id")
# 恢复队列：同风险按 deferred_id（到达次序）保持公平，先到先得。
_DRAIN_ORDER = ("o.severity DESC", "CAST(o.confidence AS REAL) DESC", "d.deferred_id")


class CapacityError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _operator(operator: object) -> str:
    if not isinstance(operator, str) or not 1 <= len(operator.strip()) <= 100:
        raise CapacityError("operator", "操作人无效")
    return operator.strip()


def _limit(limit: object) -> int:
    if type(limit) is not int or limit < 0:
        raise CapacityError("limit", "容量上限须为非负整数")
    return limit


def _region_identifier(value: object) -> str:
    if not isinstance(value, str) or not REGIONS.fullmatch(value):
        raise CapacityError("region", "区域标识无效")
    return value


def region_limit(connection: sqlite3.Connection, region: str) -> int | None:
    row = store.one(connection, "SELECT max_pending FROM region_capacities WHERE region=?", (region,))
    return None if row is None else int(row["max_pending"])


def subscription_limit(connection: sqlite3.Connection, subscription_id: str) -> int | None:
    row = store.one(connection, "SELECT max_pending FROM subscription_capacities WHERE subscription_id=?",
                    (subscription_id,))
    return None if row is None else int(row["max_pending"])


def set_region_capacity(connection: sqlite3.Connection, region: str, max_pending: int,
                        operator: str, *, when: str | None = None) -> dict[str, Any]:
    region = _region_identifier(region)
    max_pending = _limit(max_pending)
    operator = _operator(operator)
    timestamp = when or catalog.utc_now()
    with store.transaction(connection):
        connection.execute(
            "INSERT INTO region_capacities(region,max_pending,operator,updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(region) DO UPDATE SET max_pending=excluded.max_pending,"
            "operator=excluded.operator,updated_at=excluded.updated_at",
            (region, max_pending, operator, timestamp))
        store.record_activity(connection, "region_capacity_changed", "region", region, timestamp,
                              {"max_pending": max_pending, "operator": operator})
        released = _drain_locked(connection, timestamp, regions=(region,))
    return region_capacity_status(connection, region) | {"released": released}


def set_subscription_capacity(connection: sqlite3.Connection, subscription_id: str, max_pending: int,
                              operator: str, *, when: str | None = None) -> dict[str, Any]:
    max_pending = _limit(max_pending)
    operator = _operator(operator)
    if catalog.subscription(connection, subscription_id) is None:
        raise CapacityError("subscription_missing", "订阅不存在")
    timestamp = when or catalog.utc_now()
    with store.transaction(connection):
        connection.execute(
            "INSERT INTO subscription_capacities(subscription_id,max_pending,operator,updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(subscription_id) DO UPDATE SET max_pending=excluded.max_pending,"
            "operator=excluded.operator,updated_at=excluded.updated_at",
            (subscription_id, max_pending, operator, timestamp))
        store.record_activity(connection, "subscription_capacity_changed", "subscription", subscription_id,
                              timestamp, {"max_pending": max_pending, "operator": operator})
        released = _drain_locked(connection, timestamp, subscriptions=(subscription_id,))
    return subscription_capacity_status(connection, subscription_id) | {"released": released}


def remove_region_capacity(connection: sqlite3.Connection, region: str, operator: str,
                           *, when: str | None = None) -> dict[str, Any]:
    operator = _operator(operator)
    timestamp = when or catalog.utc_now()
    with store.transaction(connection):
        changed = connection.execute("DELETE FROM region_capacities WHERE region=?", (region,)).rowcount
        if not changed:
            raise CapacityError("capacity_missing", "区域容量策略不存在")
        store.record_activity(connection, "region_capacity_removed", "region", region, timestamp,
                              {"operator": operator})
        released = _drain_locked(connection, timestamp, regions=(region,))
    return {"region": region, "removed": True, "released": released}


def remove_subscription_capacity(connection: sqlite3.Connection, subscription_id: str, operator: str,
                                 *, when: str | None = None) -> dict[str, Any]:
    operator = _operator(operator)
    timestamp = when or catalog.utc_now()
    with store.transaction(connection):
        changed = connection.execute("DELETE FROM subscription_capacities WHERE subscription_id=?",
                                     (subscription_id,)).rowcount
        if not changed:
            raise CapacityError("capacity_missing", "订阅容量策略不存在")
        store.record_activity(connection, "subscription_capacity_removed", "subscription", subscription_id,
                              timestamp, {"operator": operator})
        released = _drain_locked(connection, timestamp, subscriptions=(subscription_id,))
    return {"subscription_id": subscription_id, "removed": True, "released": released}


def _pending_counts(connection: sqlite3.Connection) -> tuple[dict[str, int], dict[str, int]]:
    region_rows = store.many(
        connection,
        "SELECT o.region AS region,COUNT(*) AS amount FROM handoffs h "
        "JOIN observations o USING(observation_id) WHERE h.status='pending' GROUP BY o.region")
    sub_rows = store.many(
        connection,
        "SELECT subscription_id,COUNT(*) AS amount FROM handoffs WHERE status='pending' GROUP BY subscription_id")
    return ({row["region"]: int(row["amount"]) for row in region_rows},
            {row["subscription_id"]: int(row["amount"]) for row in sub_rows})


def _blocked_reason(region: str, subscription_id: str, region_usage: dict[str, int],
                    sub_usage: dict[str, int], region_max: int | None, sub_max: int | None) -> str | None:
    region_full = region_max is not None and region_usage.get(region, 0) >= region_max
    sub_full = sub_max is not None and sub_usage.get(subscription_id, 0) >= sub_max
    if region_full and sub_full:
        return "both_full"
    if region_full:
        return "region_full"
    if sub_full:
        return "subscription_full"
    return None


def _insert_handoff(connection: sqlite3.Connection, observation_id: int, subscription_id: str, when: str) -> int:
    cursor = connection.execute(
        "INSERT INTO handoffs(observation_id,subscription_id,status,created_at) VALUES(?,?,'pending',?)",
        (observation_id, subscription_id, when))
    handoff_id = int(cursor.lastrowid)
    store.record_activity(connection, "handoff_created", "handoff", str(handoff_id), when,
                          {"observation_id": observation_id, "subscription_id": subscription_id})
    return handoff_id


def _defer(connection: sqlite3.Connection, observation_id: int, subscription_id: str, region: str,
           reason: str, when: str) -> int:
    cursor = connection.execute(
        "INSERT INTO deferred_routings(observation_id,subscription_id,region,reason_code,status,created_at) "
        "VALUES(?,?,?,?,'waiting',?)", (observation_id, subscription_id, region, reason, when))
    deferred_id = int(cursor.lastrowid)
    store.record_activity(connection, "routing_deferred", "deferred", str(deferred_id), when,
                          {"observation_id": observation_id, "subscription_id": subscription_id,
                           "region": region, "reason_code": reason})
    return deferred_id


def route_batch(connection: sqlite3.Connection, batch_id: int, when: str) -> dict[str, int]:
    """为整批已提交观测规划交接路由，须在采集事务内调用。

    先尝试恢复同区域/订阅的暂缓队列（保证多站点并发时先到先得），再按风险
    优先处理本批匹配；拿不到容量的路由进入 deferred_routings，观测证据不受影响。
    """
    _drain_locked(connection, when)
    region_usage, sub_usage = _pending_counts(connection)
    candidates = store.many(
        connection,
        "SELECT o.observation_id,o.region,o.severity,o.confidence,o.observed_at,s.subscription_id "
        "FROM observations o JOIN subscriptions s ON s.region=o.region AND s.active=1 "
        "AND s.minimum_severity<=o.severity "
        "WHERE o.batch_id=? AND CAST(o.confidence AS REAL)>=CAST(s.minimum_confidence AS REAL) "
        f"ORDER BY {','.join(_PRIORITY_ORDER)}", (batch_id,))
    created = deferred = 0
    for row in candidates:
        region_max = region_limit(connection, row["region"])
        sub_max = subscription_limit(connection, row["subscription_id"])
        reason = _blocked_reason(row["region"], row["subscription_id"], region_usage, sub_usage,
                                 region_max, sub_max)
        if reason is None:
            _insert_handoff(connection, row["observation_id"], row["subscription_id"], when)
            region_usage[row["region"]] = region_usage.get(row["region"], 0) + 1
            sub_usage[row["subscription_id"]] = sub_usage.get(row["subscription_id"], 0) + 1
            created += 1
        else:
            _defer(connection, row["observation_id"], row["subscription_id"], row["region"], reason, when)
            deferred += 1
    return {"handoff_count": created, "deferred_count": deferred}


def _drain_locked(connection: sqlite3.Connection, when: str, *,
                  regions: tuple[str, ...] | None = None,
                  subscriptions: tuple[str, ...] | None = None) -> int:
    """按风险优先、同风险按到达次序释放暂缓路由；须在已开启的事务内调用。"""
    query = ("SELECT d.deferred_id,d.observation_id,d.subscription_id,d.region FROM deferred_routings d "
             "JOIN observations o USING(observation_id) "
             "JOIN subscriptions s ON s.subscription_id=d.subscription_id "
             "WHERE d.status='waiting' AND s.active=1")
    args: list[Any] = []
    if regions is not None:
        query += " AND d.region IN (%s)" % ",".join("?" for _ in regions)
        args.extend(regions)
    if subscriptions is not None:
        query += " AND d.subscription_id IN (%s)" % ",".join("?" for _ in subscriptions)
        args.extend(subscriptions)
    query += f" ORDER BY {','.join(_DRAIN_ORDER)}"
    waiting = store.many(connection, query, tuple(args))
    region_usage, sub_usage = _pending_counts(connection)
    released = 0
    for row in waiting:
        region_max = region_limit(connection, row["region"])
        sub_max = subscription_limit(connection, row["subscription_id"])
        reason = _blocked_reason(row["region"], row["subscription_id"], region_usage, sub_usage,
                                 region_max, sub_max)
        if reason is not None:
            continue
        handoff_id = _insert_handoff(connection, row["observation_id"], row["subscription_id"], when)
        connection.execute(
            "UPDATE deferred_routings SET status='dispatched',released_at=?,handoff_id=? WHERE deferred_id=?",
            (when, handoff_id, row["deferred_id"]))
        store.record_activity(connection, "routing_released", "deferred", str(row["deferred_id"]), when,
                              {"handoff_id": handoff_id, "subscription_id": row["subscription_id"]})
        region_usage[row["region"]] = region_usage.get(row["region"], 0) + 1
        sub_usage[row["subscription_id"]] = sub_usage.get(row["subscription_id"], 0) + 1
        released += 1
    return released


def drain_deferred(connection: sqlite3.Connection, *, when: str | None = None,
                   region: str | None = None, subscription_id: str | None = None) -> dict[str, Any]:
    timestamp = when or catalog.utc_now()
    with store.transaction(connection):
        before = store.one(connection, "SELECT COUNT(*) AS amount FROM deferred_routings WHERE status='waiting'")["amount"]
        released = _drain_locked(connection, timestamp,
                                 regions=(region,) if region is not None else None,
                                 subscriptions=(subscription_id,) if subscription_id is not None else None)
        after = store.one(connection, "SELECT COUNT(*) AS amount FROM deferred_routings WHERE status='waiting'")["amount"]
        if released:
            store.record_activity(connection, "capacity_drained", "deferred", "0", timestamp,
                                  {"released": released, "waiting_before": before, "waiting_after": after})
    return {"released": released, "waiting_before": before, "waiting_after": after}


def list_deferred(connection: sqlite3.Connection, *, status: str | None = None, region: str | None = None,
                  subscription_id: str | None = None, limit: int = 100, after_id: int = 0) -> list[dict[str, Any]]:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise CapacityError("limit", "查询上限须在 1 到 1000 之间")
    if type(after_id) is not int or after_id < 0:
        raise CapacityError("after_id", "游标无效")
    if status is not None and status not in {"waiting", "dispatched"}:
        raise CapacityError("status", "暂缓状态无效")
    query = ("SELECT d.*,o.station_id,o.external_id,o.observed_at,o.latitude,o.longitude,o.confidence,"
             "o.severity,o.source,o.fingerprint,o.batch_id,b.sequence,b.packet_id "
             "FROM deferred_routings d JOIN observations o USING(observation_id) "
             "JOIN batches b ON b.batch_id=o.batch_id WHERE d.deferred_id>?")
    args: list[Any] = [after_id]
    if status is not None:
        query += " AND d.status=?"
        args.append(status)
    if region is not None:
        query += " AND d.region=?"
        args.append(region)
    if subscription_id is not None:
        query += " AND d.subscription_id=?"
        args.append(subscription_id)
    query += " ORDER BY d.deferred_id LIMIT ?"
    args.append(limit)
    return store.many(connection, query, tuple(args))


def region_capacity_status(connection: sqlite3.Connection, region: str) -> dict[str, Any]:
    policy = store.one(connection, "SELECT * FROM region_capacities WHERE region=?", (region,))
    pending = store.one(
        connection,
        "SELECT COUNT(*) AS amount FROM handoffs h JOIN observations o USING(observation_id) WHERE o.region=?",
        (region,))["amount"]
    waiting = store.one(connection, "SELECT COUNT(*) AS amount FROM deferred_routings WHERE region=? AND status='waiting'",
                        (region,))["amount"]
    max_pending = None if policy is None else int(policy["max_pending"])
    available = None if max_pending is None else max(0, max_pending - int(pending))
    return {"region": region, "max_pending": max_pending, "pending": int(pending),
            "waiting": int(waiting), "available": available,
            "policy": dict(policy) if policy is not None else None}


def subscription_capacity_status(connection: sqlite3.Connection, subscription_id: str) -> dict[str, Any]:
    policy = store.one(connection, "SELECT * FROM subscription_capacities WHERE subscription_id=?",
                       (subscription_id,))
    pending = store.one(connection, "SELECT COUNT(*) AS amount FROM handoffs WHERE subscription_id=? AND status='pending'",
                        (subscription_id,))["amount"]
    waiting = store.one(
        connection,
        "SELECT COUNT(*) AS amount FROM deferred_routings WHERE subscription_id=? AND status='waiting'",
        (subscription_id,))["amount"]
    max_pending = None if policy is None else int(policy["max_pending"])
    available = None if max_pending is None else max(0, max_pending - int(pending))
    return {"subscription_id": subscription_id, "max_pending": max_pending, "pending": int(pending),
            "waiting": int(waiting), "available": available,
            "policy": dict(policy) if policy is not None else None}


def list_capacity_policies(connection: sqlite3.Connection) -> dict[str, Any]:
    return {"regions": store.many(connection, "SELECT * FROM region_capacities ORDER BY region"),
            "subscriptions": store.many(
                connection,
                "SELECT c.*,s.region FROM subscription_capacities c "
                "JOIN subscriptions s USING(subscription_id) ORDER BY c.subscription_id")}
