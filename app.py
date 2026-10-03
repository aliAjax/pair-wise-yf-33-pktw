#!/usr/bin/env python3
"""Multi-station satellite scheduling service using only Python's standard library."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PORT = 8204
ROLES = {"viewer", "requester", "operator", "commander", "auditor"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message); self.status, self.code, self.message, self.details = status, code, message, details


class _WindowRevisionConflict(Exception):
    def __init__(self, details: dict[str, Any], plan: dict[str, Any]): self.details, self.plan = details, plan


class _WindowChangeFailed(Exception):
    def __init__(self, plan: dict[str, Any], cause: Exception): self.plan, self.cause = plan, cause


def utcnow() -> datetime: return datetime.now(timezone.utc)
def iso(value: datetime | None = None) -> str: return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")
def parse_time(value: str | None) -> datetime:
    if not value: raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try: parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc: raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
def overlap(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool: return a_start < b_end and b_start < a_end


class Repository:
    def __init__(self, path: str | Path):
        self.conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._tx_lock = threading.RLock()
        self.conn.row_factory = sqlite3.Row; self.conn.execute("PRAGMA foreign_keys=ON"); self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS satellites(id TEXT PRIMARY KEY, name TEXT NOT NULL, data_rate_mbps REAL NOT NULL, priority INTEGER NOT NULL, storage_capacity_mb REAL NOT NULL, tenant TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS stations(id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', weather TEXT NOT NULL DEFAULT 'clear');
        CREATE TABLE IF NOT EXISTS antennas(id TEXT PRIMARY KEY, station_id TEXT NOT NULL REFERENCES stations(id), max_rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'active');
        CREATE TABLE IF NOT EXISTS maintenance(id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT REFERENCES antennas(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, reason TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS visibility_windows(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), station_id TEXT NOT NULL REFERENCES stations(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, max_rate_mbps REAL NOT NULL, revision INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY AUTOINCREMENT, satellite_id TEXT NOT NULL REFERENCES satellites(id), tenant TEXT NOT NULL, priority INTEGER NOT NULL, data_mb REAL NOT NULL, deadline TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_by TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS quotas(id INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL, station_id TEXT NOT NULL REFERENCES stations(id), daily_seconds INTEGER NOT NULL, UNIQUE(tenant,station_id));
        CREATE TABLE IF NOT EXISTS schedules(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER NOT NULL UNIQUE REFERENCES requests(id), window_id INTEGER NOT NULL REFERENCES visibility_windows(id), station_id TEXT NOT NULL REFERENCES stations(id), antenna_id TEXT NOT NULL REFERENCES antennas(id), satellite_id TEXT NOT NULL REFERENCES satellites(id), starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, rate_mbps REAL NOT NULL, status TEXT NOT NULL DEFAULT 'scheduled', revision INTEGER NOT NULL DEFAULT 1, disposition_reason TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, request_id INTEGER, schedule_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS window_dispositions(id INTEGER PRIMARY KEY AUTOINCREMENT, window_id INTEGER NOT NULL REFERENCES visibility_windows(id), base_revision INTEGER NOT NULL, final_revision INTEGER, status TEXT NOT NULL, actor TEXT NOT NULL, old_json TEXT NOT NULL, new_json TEXT NOT NULL, snapshot_json TEXT NOT NULL, impacts_json TEXT NOT NULL, error TEXT, created_at TEXT NOT NULL);
        """)

    @contextmanager
    def tx(self):
        # 进程内串行化写事务：同一连接上禁止嵌套 BEGIN，同时避免两线程并发窗口变更时后到者盲覆盖
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try: yield self.conn; self.conn.execute("COMMIT")
            except Exception: self.conn.execute("ROLLBACK"); raise

    @staticmethod
    def audit(conn: sqlite3.Connection, request_id: int | None, schedule_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(request_id,schedule_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                     (request_id, schedule_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class SatelliteSchedulingService:
    def __init__(self, path: str | Path): self.repo = Repository(path)

    @staticmethod
    def identity(headers: Any) -> tuple[str, str, str]:
        actor, role, tenant = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip(), headers.get("X-Tenant", "").strip()
        if not actor or role not in ROLES: raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        if role == "requester" and not tenant: raise ApiError(401, "tenant_required", "requester 必须提供 X-Tenant")
        return actor, role, tenant

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None: return dict(row) if row else None

    def create_satellite(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护卫星")
        sid, name, tenant = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("tenant", "")).strip()
        rate, priority, capacity = body.get("data_rate_mbps"), body.get("priority"), body.get("storage_capacity_mb")
        if not sid or not name or not tenant or not isinstance(rate, (int, float)) or float(rate) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10 or not isinstance(capacity, (int, float)) or float(capacity) <= 0:
            raise ApiError(400, "invalid_satellite", "卫星参数不完整")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO satellites(id,name,data_rate_mbps,priority,storage_capacity_mb,tenant,status) VALUES(?,?,?,?,?,?,?)", (sid, name, float(rate), priority, float(capacity), tenant, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM satellites WHERE id=?", (sid,)).fetchone())

    def create_station(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护地面站")
        sid, name = str(body.get("id", "")).strip(), str(body.get("name", "")).strip()
        weather = str(body.get("weather", "clear")).lower()
        if not sid or not name or weather not in {"clear", "rain", "storm", "closed"}: raise ApiError(400, "invalid_station", "地面站名称或天气状态无效")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO stations(id,name,status,weather) VALUES(?,?,?,?)", (sid, name, body.get("status", "active"), weather))
            return dict(conn.execute("SELECT * FROM stations WHERE id=?", (sid,)).fetchone())

    def create_antenna(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "resource_forbidden", "当前角色不能维护天线")
        aid, station, rate = str(body.get("id", "")).strip(), str(body.get("station_id", "")).strip(), body.get("max_rate_mbps")
        if not aid or not station or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_antenna", "天线参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT OR REPLACE INTO antennas(id,station_id,max_rate_mbps,status) VALUES(?,?,?,?)", (aid, station, float(rate), body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM antennas WHERE id=?", (aid,)).fetchone())

    def create_maintenance(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "maintenance_forbidden", "当前角色不能登记维护")
        station, start, end, reason = str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), str(body.get("reason", "")).strip()
        antenna = body.get("antenna_id")
        if not station or end <= start or not reason: raise ApiError(400, "invalid_maintenance", "维护参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            if antenna and not conn.execute("SELECT 1 FROM antennas WHERE id=? AND station_id=?", (antenna, station)).fetchone(): raise ApiError(400, "antenna_station_mismatch", "天线不属于该站")
            cur = conn.execute("INSERT INTO maintenance(station_id,antenna_id,starts_at,ends_at,reason) VALUES(?,?,?,?,?)", (station, antenna, iso(start), iso(end), reason))
            return dict(conn.execute("SELECT * FROM maintenance WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_window(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能维护可见窗口")
        satellite, station, start, end, rate = str(body.get("satellite_id", "")).strip(), str(body.get("station_id", "")).strip(), parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("max_rate_mbps")
        if not satellite or not station or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_window", "可见窗口参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM satellites WHERE id=?", (satellite,)).fetchone(): raise ApiError(404, "satellite_not_found", "卫星不存在")
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            cur = conn.execute("INSERT INTO visibility_windows(satellite_id,station_id,starts_at,ends_at,max_rate_mbps) VALUES(?,?,?,?,?)", (satellite, station, iso(start), iso(end), float(rate)))
            return dict(conn.execute("SELECT * FROM visibility_windows WHERE id=?", (cur.lastrowid,)).fetchone())

    def set_quota(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "quota_forbidden", "当前角色不能设置租户配额")
        tenant, station, seconds = str(body.get("tenant", "")).strip(), str(body.get("station_id", "")).strip(), body.get("daily_seconds")
        if not tenant or not station or not isinstance(seconds, int) or seconds <= 0: raise ApiError(400, "invalid_quota", "配额参数无效")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM stations WHERE id=?", (station,)).fetchone(): raise ApiError(404, "station_not_found", "地面站不存在")
            conn.execute("INSERT INTO quotas(tenant,station_id,daily_seconds) VALUES(?,?,?) ON CONFLICT(tenant,station_id) DO UPDATE SET daily_seconds=excluded.daily_seconds", (tenant, station, seconds))
            return dict(conn.execute("SELECT * FROM quotas WHERE tenant=? AND station_id=?", (tenant, station)).fetchone())

    def create_request(self, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"requester", "operator", "commander"}: raise ApiError(403, "request_forbidden", "当前角色不能创建数据请求")
        satellite = str(body.get("satellite_id", "")).strip(); data_mb = body.get("data_mb"); priority = body.get("priority"); deadline = parse_time(body.get("deadline"))
        owner = tenant if role == "requester" else str(body.get("tenant", "")).strip()
        if not satellite or not owner or not isinstance(data_mb, (int, float)) or float(data_mb) <= 0 or not isinstance(priority, int) or not 1 <= priority <= 10:
            raise ApiError(400, "invalid_request", "请求参数无效")
        with self.repo.tx() as conn:
            satellite_row = conn.execute("SELECT * FROM satellites WHERE id=?", (satellite,)).fetchone()
            if not satellite_row: raise ApiError(404, "satellite_not_found", "卫星不存在")
            if owner != satellite_row["tenant"]: raise ApiError(403, "tenant_satellite_forbidden", "租户不能申请不属于自己的卫星")
            if deadline <= utcnow(): raise ApiError(409, "deadline_expired", "请求截止时间已经过去")
            cur = conn.execute("INSERT INTO requests(satellite_id,tenant,priority,data_mb,deadline,created_by,created_at) VALUES(?,?,?,?,?,?,?)", (satellite, owner, priority, float(data_mb), iso(deadline), actor, iso()))
            request_id = cur.lastrowid; Repository.audit(conn, request_id, None, actor, role, "request_created", {"data_mb": float(data_mb), "deadline": iso(deadline)})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    def _used_quota(self, conn: sqlite3.Connection, tenant: str, station: str, day: str, exclude_schedule: int | None = None) -> int:
        sql = """SELECT COALESCE(SUM((julianday(s.ends_at)-julianday(s.starts_at))*86400),0) FROM schedules s JOIN requests r ON r.id=s.request_id
                 WHERE r.tenant=? AND s.station_id=? AND substr(s.starts_at,1,10)=? AND s.status IN ('scheduled','receiving','received')"""
        args: list[Any] = [tenant, station, day]
        if exclude_schedule is not None: sql += " AND s.id!=?"; args.append(exclude_schedule)
        return int(conn.execute(sql, args).fetchone()[0])

    def schedule_request(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"operator", "commander"}: raise ApiError(403, "schedule_forbidden", "只有排程员可以安排接收")
        window_id, antenna_id = body.get("window_id"), str(body.get("antenna_id", "")).strip()
        start, end, rate = parse_time(body.get("starts_at")), parse_time(body.get("ends_at")), body.get("rate_mbps")
        if not isinstance(window_id, int) or not antenna_id or end <= start or not isinstance(rate, (int, float)) or float(rate) <= 0: raise ApiError(400, "invalid_schedule", "排程参数无效")
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            antenna = conn.execute("SELECT * FROM antennas WHERE id=?", (antenna_id,)).fetchone()
            if not request or not window or not antenna: raise ApiError(404, "schedule_ref_not_found", "请求、窗口或天线不存在")
            if request["status"] not in {"pending", "preempted", "queued"}: raise ApiError(409, "request_closed", "请求当前不能排程")
            if request["satellite_id"] != window["satellite_id"] or window["station_id"] != antenna["station_id"]: raise ApiError(409, "window_mismatch", "卫星、窗口和天线不匹配")
            station = conn.execute("SELECT * FROM stations WHERE id=?", (window["station_id"],)).fetchone()
            satellite = conn.execute("SELECT * FROM satellites WHERE id=?", (request["satellite_id"],)).fetchone()
            if satellite["status"] != "active" or station["status"] != "active" or antenna["status"] != "active": raise ApiError(409, "resource_inactive", "卫星、地面站或天线不可用")
            if station["weather"] != "clear": raise ApiError(409, "weather_blocked", "天气条件不允许接收")
            w_start, w_end = parse_time(window["starts_at"]), parse_time(window["ends_at"])
            if start < w_start or end > w_end: raise ApiError(409, "outside_visibility", "排程超出可见窗口")
            if end > parse_time(request["deadline"]): raise ApiError(409, "deadline_missed", "预计结束时间超过请求截止时间")
            max_rate = min(float(satellite["data_rate_mbps"]), float(window["max_rate_mbps"]), float(antenna["max_rate_mbps"]))
            if float(rate) > max_rate: raise ApiError(409, "rate_exceeded", "请求速率超过可用上限", {"max_rate_mbps": max_rate})
            transferred = (end - start).total_seconds() * float(rate) / 8
            if transferred < float(request["data_mb"]): raise ApiError(409, "insufficient_capacity", "窗口内可接收数据量不足", {"capacity_mb": transferred, "required_mb": request["data_mb"]})
            maintenance = conn.execute("""SELECT * FROM maintenance WHERE station_id=? AND (antenna_id IS NULL OR antenna_id=?) AND starts_at<? AND ends_at>?""", (station["id"], antenna_id, iso(end), iso(start))).fetchone()
            if maintenance: raise ApiError(409, "maintenance_conflict", "天线或地面站处于维护期", dict(maintenance))
            equipment = conn.execute("SELECT id,status FROM schedules WHERE station_id=? AND antenna_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')", (station["id"], antenna_id, iso(end), iso(start))).fetchone()
            if equipment: raise ApiError(409, "antenna_conflict", "天线时段已被占用", {"schedule_id": equipment["id"]})
            satellite_conflict = conn.execute("SELECT id,status FROM schedules WHERE satellite_id=? AND starts_at<? AND ends_at>? AND status IN ('scheduled','receiving')", (request["satellite_id"], iso(end), iso(start))).fetchone()
            if satellite_conflict: raise ApiError(409, "satellite_conflict", "同一卫星时段已被其他站接收", {"schedule_id": satellite_conflict["id"]})
            quota = conn.execute("SELECT daily_seconds FROM quotas WHERE tenant=? AND station_id=?", (request["tenant"], station["id"])).fetchone()
            used = self._used_quota(conn, request["tenant"], station["id"], start.date().isoformat())
            duration = int((end - start).total_seconds())
            if quota and used + duration > quota["daily_seconds"]: raise ApiError(409, "tenant_quota_exceeded", "租户当日地面站配额不足", {"used_seconds": used, "requested_seconds": duration, "limit": quota["daily_seconds"]})
            cur = conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,created_by,created_at,updated_at)
                                  VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                               (request_id, window_id, station["id"], antenna_id, request["satellite_id"], iso(start), iso(end), float(rate), actor, iso(), iso()))
            schedule_id = cur.lastrowid
            conn.execute("UPDATE requests SET status='scheduled' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, schedule_id, actor, role, "schedule_created", {"window_id": window_id, "antenna_id": antenna_id, "capacity_mb": transferred})
            return self.get_schedule(schedule_id)

    def get_schedule(self, schedule_id: int) -> dict[str, Any]:
        row = self.repo.conn.execute("""SELECT s.*,r.tenant,r.data_mb,r.priority request_priority,r.deadline FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
        if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
        return dict(row)

    def transition(self, schedule_id: int, actor: str, role: str, tenant: str, target: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant,r.status request_status FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if target == "receiving":
                if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能开始接收")
                if row["status"] != "scheduled": raise ApiError(409, "invalid_transition", "只有已排程任务可以开始接收")
                conn.execute("UPDATE schedules SET status='receiving',revision=revision+1,updated_at=? WHERE id=?", (iso(), schedule_id))
            elif target == "received":
                if role not in {"operator", "commander"}: raise ApiError(403, "receive_forbidden", "当前角色不能完成接收")
                if row["status"] != "receiving": raise ApiError(409, "invalid_transition", "只有接收中任务可以完成")
                conn.execute("UPDATE schedules SET status='received',revision=revision+1,updated_at=? WHERE id=?", (iso(), schedule_id))
                conn.execute("UPDATE requests SET status='received' WHERE id=?", (row["request_id"],))
            else: raise ApiError(400, "invalid_transition", "未知状态")
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, f"receive_{target}", {})
            return self.get_schedule(schedule_id)

    def cancel_schedule(self, schedule_id: int, actor: str, role: str, tenant: str, body: dict[str, Any]) -> dict[str, Any]:
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if role == "requester":
                if row["tenant"] != tenant: raise ApiError(403, "tenant_forbidden", "不能取消其他租户排程")
                if row["status"] != "scheduled": raise ApiError(409, "cancel_not_allowed", "接收开始后租户不能取消")
            elif role not in {"operator", "commander"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消排程")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据不能取消或删除")
            if row["status"] == "canceled": return self.get_schedule(schedule_id)
            conn.execute("UPDATE schedules SET status='canceled',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), schedule_id))
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (row["request_id"],))
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "schedule_canceled", {"reason": reason})
            return self.get_schedule(schedule_id)

    def emergency_preempt(self, schedule_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "commander": raise ApiError(403, "commander_required", "只有任务指挥官可以执行紧急抢占")
        order_id, reason = str(body.get("order_id", "")).strip(), str(body.get("reason", "")).strip()
        if not order_id or not reason: raise ApiError(400, "emergency_details_required", "order_id 和 reason 必填")
        with self.repo.tx() as conn:
            row = conn.execute("""SELECT s.*,r.priority,r.tenant FROM schedules s JOIN requests r ON r.id=s.request_id WHERE s.id=?""", (schedule_id,)).fetchone()
            if not row: raise ApiError(404, "schedule_not_found", "排程不存在")
            if row["status"] == "received": raise ApiError(409, "received_data_protected", "已接收数据的排程不能被抢占")
            if row["status"] not in {"scheduled", "receiving"}: raise ApiError(409, "invalid_transition", "当前排程不可抢占")
            conn.execute("UPDATE schedules SET status='preempted',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?", (f"{order_id}: {reason}", iso(), schedule_id))
            conn.execute("UPDATE requests SET status='preempted' WHERE id=?", (row["request_id"],))
            Repository.audit(conn, row["request_id"], schedule_id, actor, role, "emergency_preemption", {"order_id": order_id, "reason": reason, "displaced_priority": row["priority"]})
            return {"schedule": self.get_schedule(schedule_id), "reschedule_required": True, "order_id": order_id}

    @staticmethod
    def _loads(value: str | None, default: Any) -> Any:
        if not value: return default
        try: return json.loads(value)
        except (TypeError, ValueError): return default

    def _window_snapshot(self, conn: sqlite3.Connection, window_id: int) -> list[dict[str, Any]]:
        rows = conn.execute("SELECT * FROM schedules WHERE window_id=? AND status IN ('scheduled','receiving','received') ORDER BY id", (window_id,)).fetchall()
        return [dict(r) for r in rows]

    def _plan_window_change(self, conn: sqlite3.Connection, window: sqlite3.Row, start: datetime, end: datetime, rate: float) -> dict[str, Any]:
        """依据窗口版本对应的排程快照，列出保留、压缩、抢占影响。已接收数据保留原样。"""
        start_s, end_s = iso(start), iso(end)
        snapshot = self._window_snapshot(conn, window["id"])
        station_id, satellite_id = window["station_id"], window["satellite_id"]
        rate_cap = min(float(rate), float(conn.execute("SELECT data_rate_mbps FROM satellites WHERE id=?", (satellite_id,)).fetchone()[0]))
        impacts: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        anchors: list[dict[str, Any]] = []
        for row in snapshot:
            if row["status"] == "received":
                anchors.append(row)
                sched_start, sched_end = parse_time(row["starts_at"]), parse_time(row["ends_at"])
                impacts.append({"schedule_id": row["id"], "request_id": row["request_id"], "action": "preserve_received_data",
                                "reason": "已接收数据不可回滚", "invalid": sched_start < start or sched_end > end})
                continue
            candidates.append(dict(row))

        def schedule_conflicts(c: dict[str, Any], n_start: datetime, n_end: datetime) -> bool:
            ns, ne = iso(n_start), iso(n_end)
            for anchor in anchors + placed:
                if anchor["antenna_id"] == c["antenna_id"] and overlap(n_start, n_end, parse_time(anchor["starts_at"]), parse_time(anchor["ends_at"])):
                    return True
                if anchor["satellite_id"] == c["satellite_id"] and overlap(n_start, n_end, parse_time(anchor["starts_at"]), parse_time(anchor["ends_at"])):
                    return True
            maintenance = conn.execute("""SELECT 1 FROM maintenance WHERE station_id=? AND (antenna_id IS NULL OR antenna_id=?) AND starts_at<? AND ends_at>?""",
                                       (station_id, c["antenna_id"], ne, ns)).fetchone()
            return maintenance is not None

        displaced: list[tuple[dict[str, Any], str]] = []
        # 先判定每个排程在新窗口内的自身可行性：无交集、速率或容量不足直接出局
        feasible: list[dict[str, Any]] = []
        for c in candidates:
            c_start, c_end = parse_time(c["starts_at"]), parse_time(c["ends_at"])
            n_start, n_end = max(c_start, start), min(c_end, end)
            req = conn.execute("SELECT priority,data_mb,deadline FROM requests WHERE id=?", (c["request_id"],)).fetchone()
            c["_priority"], c["_data_mb"], c["_deadline"], c["_new_start"], c["_new_end"] = req["priority"], float(req["data_mb"]), req["deadline"], n_start, n_end
            if n_end <= n_start:
                displaced.append((c, "新窗口与原排程无交集")); continue
            antenna_row = conn.execute("SELECT max_rate_mbps FROM antennas WHERE id=?", (c["antenna_id"],)).fetchone()
            c["_max_rate"] = min(rate_cap, float(antenna_row[0]) if antenna_row else rate_cap)
            seconds = (n_end - n_start).total_seconds()
            need_rate = c["_data_mb"] * 8 / seconds
            if n_end > parse_time(c["_deadline"]):
                displaced.append((c, "压缩后结束时间超过请求截止时间")); continue
            if need_rate - c["_max_rate"] > 1e-9:
                displaced.append((c, "新窗口容量不足")); continue
            c["_need_rate"] = need_rate
            feasible.append(c)
        # 容量/设备仍有争用时，按优先级（高者优先，其次截止时间、排程号）抢占
        feasible.sort(key=lambda c: (-int(c["_priority"]), c["_deadline"], c["id"]))
        placed: list[dict[str, Any]] = []
        for c in feasible:
            if schedule_conflicts(c, c["_new_start"], c["_new_end"]):
                displaced.append((c, "容量不足，按优先级被高优先级请求抢占")); continue
            old_rate = float(c["rate_mbps"])
            c["_new_rate"] = round(min(c["_max_rate"], max(old_rate, c["_need_rate"])), 6)
            placed.append(c)
            if iso(c["_new_start"]) != c["starts_at"] or iso(c["_new_end"]) != c["ends_at"]:
                impacts.append({"schedule_id": c["id"], "request_id": c["request_id"], "action": "compressed",
                                "reason": "窗口收缩，排程压缩到新窗口内", "old_start": c["starts_at"], "old_end": c["ends_at"],
                                "old_rate_mbps": old_rate, "new_start": iso(c["_new_start"]), "new_end": iso(c["_new_end"]),
                                "new_rate_mbps": c["_new_rate"], "priority": c["_priority"]})
            else:
                impacts.append({"schedule_id": c["id"], "request_id": c["request_id"], "action": "retained",
                                "reason": "新窗口仍覆盖排程", "new_rate_mbps": c["_new_rate"], "priority": c["_priority"]})
        # 未接收请求进入排队（按优先级排序，供后续重排）
        displaced.sort(key=lambda item: (-int(item[0]["_priority"]), item[0]["_deadline"], item[0]["id"]))
        queue: list[dict[str, Any]] = []
        for position, (c, why) in enumerate(displaced, start=1):
            impacts.append({"schedule_id": c["id"], "request_id": c["request_id"], "action": "preempted", "reason": why,
                            "old_start": c["starts_at"], "old_end": c["ends_at"], "priority": c["_priority"], "queued": True, "queue_position": position})
            queue.append({"schedule_id": c["id"], "request_id": c["request_id"], "priority": c["_priority"],
                          "data_mb": c["_data_mb"], "deadline": c["_deadline"], "reason": why, "queue_position": position})
        impacts.sort(key=lambda x: x["schedule_id"])
        new_window = {"id": window["id"], "satellite_id": satellite_id, "station_id": station_id,
                      "starts_at": start_s, "ends_at": end_s, "max_rate_mbps": float(rate), "revision": int(window["revision"]) + 1}
        counts = {"retained": 0, "compressed": 0, "preempted": 0, "preserved": 0}
        for item in impacts:
            counts["preserved" if item["action"] == "preserve_received_data" else item["action"]] += 1
        return {"impacts": impacts, "queue": queue, "old_window": dict(window), "new_window": new_window,
                "placed": [{"id": c["id"], "starts_at": iso(c["_new_start"]), "ends_at": iso(c["_new_end"]), "rate_mbps": c["_new_rate"]} for c in placed],
                "displaced_ids": [c["id"] for c, _ in displaced], "counts": counts, "snapshot": snapshot}

    def _record_disposition(self, window_id: int, base_revision: int | None, final_revision: int | None, status: str,
                            actor: str, role: str, plan: dict[str, Any] | None, error: str | None = None,
                            conn: sqlite3.Connection | None = None) -> int:
        """登记处置单。传入 conn 时随当前事务提交，否则用独立事务（冲突/失败留痕）。"""
        old_window = (plan or {}).get("old_window")
        new_window = (plan or {}).get("new_window")
        snapshot = (plan or {}).get("snapshot", [])
        impacts = (plan or {}).get("impacts", [])
        sql = """INSERT INTO window_dispositions(window_id,base_revision,final_revision,status,actor,old_json,new_json,snapshot_json,impacts_json,error,created_at)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?)"""
        def _dump(value: Any) -> str: return json.dumps(value, ensure_ascii=False, default=str)
        args = (window_id, base_revision, final_revision, status, actor,
                _dump(old_window), _dump(new_window), _dump(snapshot), _dump(impacts), error, iso())

        def _insert(c: sqlite3.Connection) -> int:
            disposition_id = c.execute(sql, args).lastrowid
            Repository.audit(c, None, None, actor, role, f"window_disposition_{status}",
                             {"window_id": window_id, "disposition_id": disposition_id})
            return disposition_id

        if conn is not None: return _insert(conn)
        with self.repo.tx() as c: return _insert(c)

    def change_window(self, window_id: int, actor: str, role: str, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        """窗口变更处置入口：事务回滚后再登记冲突/失败处置单。"""
        try:
            return self._change_window_tx(window_id, actor, role, body)
        except _WindowRevisionConflict as conflict:
            # 事务此时已随 with tx() 回滚，可以用独立事务登记冲突处置单
            plan = conflict.plan
            disposition_id = self._record_disposition(window_id, conflict.details["current_revision"], None, "conflicted",
                                                      actor, role, plan, error=f"expected_revision={conflict.details['expected_revision']}")
            # 冲突后读回上一版窗口、排程和请求（未写入任何变更）
            details = {**conflict.details, "disposition_id": disposition_id}
            raise ApiError(409, "window_revision_conflict",
                           f"窗口版本过期：基线 r{conflict.details['expected_revision']}，当前 r{conflict.details['current_revision']}，窗口、排程和请求保持上一版",
                           details) from None
        except _WindowChangeFailed as failed:
            # 事务已 ROLLBACK，提交前数值已恢复；在独立事务登记失败处置单
            plan = failed.plan
            disposition_id = self._record_disposition(window_id, plan["old_window"]["revision"], None, "failed",
                                                      actor, role, plan, error=repr(failed.cause))
            raise ApiError(500, "window_change_write_failed",
                           f"窗口变更写入失败，已恢复提交前数值: {failed.cause}",
                           {"disposition_id": disposition_id, "window": plan["old_window"],
                            "impacts": plan["impacts"], "queue": plan["queue"]}) from failed.cause

    def _change_window_tx(self, window_id: int, actor: str, role: str, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        if role not in {"operator", "commander"}: raise ApiError(403, "window_forbidden", "当前角色不能变更可见窗口")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_window", "窗口结束时间必须晚于开始时间")
        rate = body.get("max_rate_mbps")
        if rate is not None and (not isinstance(rate, (int, float)) or float(rate) <= 0):
            raise ApiError(400, "invalid_window", "窗口最大速率无效")
        expected = body.get("expected_revision", body.get("revision"))
        if expected is not None and not isinstance(expected, int): raise ApiError(400, "invalid_revision", "expected_revision 必须是整数")
        commit = bool(body.get("commit", True))
        with self.repo.tx() as conn:
            window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
            if not window: raise ApiError(404, "window_not_found", "可见窗口不存在")
            base_revision = int(window["revision"])
            if expected is not None and expected != base_revision:
                # 窗口版本过期：不写任何数据，回滚后读回上一版窗口与排程快照
                snapshot = self._window_snapshot(conn, window_id)
                plan = {"old_window": dict(window), "new_window": None, "impacts": [], "queue": [],
                        "snapshot": snapshot, "counts": {}}
                details = {"window_id": window_id, "expected_revision": expected, "current_revision": base_revision,
                           "window": dict(window), "schedules": snapshot, "plan": plan}
                raise _WindowRevisionConflict(details, plan)
            new_rate = float(rate) if rate is not None else float(window["max_rate_mbps"])
            plan = self._plan_window_change(conn, window, start, end, new_rate)
            if not commit:
                disposition_id = self._record_disposition(window_id, base_revision, None, "previewed", actor, role, plan, conn=conn)
                return 200, {"disposition_id": disposition_id, "committed": False, "window": dict(window),
                             "new_window": plan["new_window"], "impacts": plan["impacts"], "queue": plan["queue"],
                             "counts": plan["counts"]}
            now = iso()
            try:
                for cid in plan["displaced_ids"]:
                    conn.execute("UPDATE schedules SET status='preempted',disposition_reason=?,revision=revision+1,updated_at=? WHERE id=?",
                                 ("visibility_window_changed", now, cid))
                    conn.execute("UPDATE requests SET status='queued' WHERE id=(SELECT request_id FROM schedules WHERE id=?)", (cid,))
                for item in plan["placed"]:
                    conn.execute("UPDATE schedules SET starts_at=?,ends_at=?,rate_mbps=?,revision=revision+1,updated_at=? WHERE id=?",
                                 (item["starts_at"], item["ends_at"], float(item["rate_mbps"]), now, item["id"]))
                conn.execute("UPDATE visibility_windows SET starts_at=?,ends_at=?,max_rate_mbps=?,revision=revision+1 WHERE id=?",
                             (iso(start), iso(end), new_rate, window_id))
                final_window = conn.execute("SELECT * FROM visibility_windows WHERE id=?", (window_id,)).fetchone()
                final_revision = int(final_window["revision"])
                plan["new_window"] = dict(final_window)
                disposition_id = self._record_disposition(window_id, base_revision, final_revision, "applied", actor, role, plan, conn=conn)
            except ApiError: raise
            except Exception as exc:
                # 写入失败：ROLLBACK 恢复提交前的窗口、排程和请求数值，外层登记失败处置单
                raise _WindowChangeFailed(plan, exc) from exc
            Repository.audit(conn, None, None, actor, role, "visibility_window_changed",
                             {"window_id": window_id, "base_revision": base_revision, "final_revision": final_revision,
                              "disposition_id": disposition_id, "impacts": plan["impacts"], "queue": plan["queue"]})
            return 200, {"disposition_id": disposition_id, "committed": True, "window": dict(final_window),
                         "new_window": dict(final_window), "impacts": plan["impacts"], "queue": plan["queue"],
                         "counts": plan["counts"], "base_revision": base_revision, "final_revision": final_revision}

    def reschedule(self, request_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        with self.repo.tx() as conn:
            request = conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
            if not request: raise ApiError(404, "request_not_found", "请求不存在")
            if request["status"] not in {"preempted", "queued"}: raise ApiError(409, "reschedule_not_needed", "只有被抢占或排队中的请求需要重排")
            conn.execute("UPDATE requests SET status='pending' WHERE id=?", (request_id,))
            Repository.audit(conn, request_id, None, actor, role, "reschedule_requested", {"reason": body.get("reason", "")})
            return dict(conn.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone())

    def list_dispositions(self, role: str, tenant: str, limit: int = 50) -> list[dict[str, Any]]:
        conn = self.repo.conn
        rows = conn.execute("SELECT * FROM window_dispositions ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        items = []
        for row in rows:
            item = {"id": row["id"], "window_id": row["window_id"], "base_revision": row["base_revision"],
                    "final_revision": row["final_revision"], "status": row["status"], "actor": row["actor"],
                    "error": row["error"], "created_at": row["created_at"],
                    "old_window": self._loads(row["old_json"], None), "new_window": self._loads(row["new_json"], None),
                    "impacts": self._loads(row["impacts_json"], [])}
            if role == "requester":
                tenant_ids = {r["id"] for r in conn.execute("SELECT id FROM requests WHERE tenant=?", (tenant,))}
                item["impacts"] = [i for i in item["impacts"] if i.get("request_id") in tenant_ids]
                item["snapshot"] = [s for s in self._loads(row["snapshot_json"], []) if s.get("request_id") in tenant_ids]
            items.append(item)
        return items

    def get_disposition(self, disposition_id: int, role: str, tenant: str) -> dict[str, Any]:
        row = self.repo.conn.execute("SELECT * FROM window_dispositions WHERE id=?", (disposition_id,)).fetchone()
        if not row: raise ApiError(404, "disposition_not_found", "处置单不存在")
        impacts = self._loads(row["impacts_json"], [])
        snapshot = self._loads(row["snapshot_json"], [])
        if role == "requester":
            tenant_ids = {r["id"] for r in self.repo.conn.execute("SELECT id FROM requests WHERE tenant=?", (tenant,))}
            impacts = [i for i in impacts if i.get("request_id") in tenant_ids]
            snapshot = [s for s in snapshot if s.get("request_id") in tenant_ids]
        return {"id": row["id"], "window_id": row["window_id"], "base_revision": row["base_revision"],
                "final_revision": row["final_revision"], "status": row["status"], "actor": row["actor"],
                "error": row["error"], "created_at": row["created_at"], "old_window": self._loads(row["old_json"], None),
                "new_window": self._loads(row["new_json"], None), "snapshot": snapshot, "impacts": impacts}

    def state(self, role: str, tenant: str) -> dict[str, Any]:
        conn = self.repo.conn
        if role == "requester":
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests WHERE tenant=? ORDER BY id DESC", (tenant,))]
            schedules = [dict(r) for r in conn.execute("SELECT s.* FROM schedules s JOIN requests r ON r.id=s.request_id WHERE r.tenant=? ORDER BY s.id DESC", (tenant,))]
            stations = []
        elif role == "viewer":
            requests, schedules, stations = [], [dict(r) for r in conn.execute("SELECT id,status,starts_at,ends_at FROM schedules ORDER BY id DESC")], []
        else:
            requests = [dict(r) for r in conn.execute("SELECT * FROM requests ORDER BY id DESC")]
            schedules = [dict(r) for r in conn.execute("SELECT * FROM schedules ORDER BY id DESC")]
            stations = [dict(r) for r in conn.execute("SELECT * FROM stations ORDER BY id")]
        queue = conn.execute("""SELECT r.id request_id,r.tenant,r.priority,r.data_mb,r.deadline,s.id schedule_id,s.disposition_reason
                                FROM requests r JOIN schedules s ON s.request_id=r.id WHERE r.status='queued'
                                ORDER BY r.priority DESC,r.deadline,r.id""").fetchall()
        queue_rows = [dict(r) for r in queue]
        if role == "requester": queue_rows = [q for q in queue_rows if q["tenant"] == tenant]
        if role == "viewer": queue_rows = []
        dispositions = self.list_dispositions(role, tenant)
        if role == "viewer":
            for item in dispositions:
                item.pop("old_window", None)
        return {"requests": requests, "schedules": schedules, "stations": stations,
                "queue": queue_rows, "dispositions": dispositions, "server_time": iso()}


def send_json(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode(); handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: SatelliteSchedulingService; web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def body(self) -> dict[str, Any]:
        size = int(self.headers.get("Content-Length", "0"))
        if not size: return {}
        try: value = json.loads(self.rfile.read(size))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value
    def get_api(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "satellite-scheduling"}
        actor, role, tenant = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state(role, tenant)
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "schedules"] and parts[2].isdigit(): return 200, self.service.get_schedule(int(parts[2]))
        if len(parts) == 3 and parts[:2] == ["api", "dispositions"] and parts[2].isdigit():
            return 200, self.service.get_disposition(int(parts[2]), role, tenant)
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role, tenant = self.service.identity(self.headers); body = self.body(); parts = [p for p in path.split("/") if p]
        actions = {
            "/api/satellites": lambda: (201, self.service.create_satellite(actor, role, body)),
            "/api/stations": lambda: (201, self.service.create_station(actor, role, body)),
            "/api/antennas": lambda: (201, self.service.create_antenna(actor, role, body)),
            "/api/maintenance": lambda: (201, self.service.create_maintenance(actor, role, body)),
            "/api/visibility-windows": lambda: (201, self.service.create_window(actor, role, body)),
            "/api/quotas": lambda: (200, self.service.set_quota(actor, role, body)),
            "/api/requests": lambda: (201, self.service.create_request(actor, role, tenant, body)),
        }
        if path in actions: return actions[path]()
        if len(parts) == 4 and parts[:2] == ["api", "requests"] and parts[2].isdigit():
            rid, action = int(parts[2]), parts[3]
            if action == "schedule": return 201, self.service.schedule_request(rid, actor, role, body)
            if action == "reschedule": return 200, self.service.reschedule(rid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "schedules"] and parts[2].isdigit():
            sid, action = int(parts[2]), parts[3]
            if action in {"start", "complete"}: return 200, self.service.transition(sid, actor, role, tenant, "receiving" if action == "start" else "received", body)
            if action == "cancel": return 200, self.service.cancel_schedule(sid, actor, role, tenant, body)
            if action == "preempt": return 200, self.service.emergency_preempt(sid, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "visibility-windows"] and parts[2].isdigit() and parts[3] == "change": return self.service.change_window(int(parts[2]), actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path) if method == "GET" else self.post_api(parsed.path); send_json(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None: payload["details"] = exc.details
            send_json(self, exc.status, payload)
        except Exception as exc: print(f"unhandled error: {exc!r}"); send_json(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_request("GET")
    def do_POST(self) -> None: self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = SatelliteSchedulingService(db_path); handler = type("SatelliteHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"}); return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("SAT_DB", "satellite_scheduling.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"satellite-scheduling listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
