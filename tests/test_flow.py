import sys, sqlite3, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, SatelliteSchedulingService, iso, utcnow


class SatelliteFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db"); self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.window = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 7200})

    def tearDown(self): self.tmp.cleanup()

    def request(self, mb=35000):
        return self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": 7, "deadline": iso(self.now + timedelta(days=1))})

    def test_complete_receive_and_window_change_impact(self):
        req = self.request(); schedule = self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1, minutes=30)), "rate_mbps": 60})
        self.assertEqual(schedule["status"], "scheduled")
        self.svc.transition(schedule["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(schedule["id"], "op", "operator", "", "received", {})
        req2 = self.request(10000)
        schedule2 = self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(hours=1)), "ends_at": iso(self.now + timedelta(hours=1, minutes=30)), "rate_mbps": 50})
        _, changed = self.svc.change_window(self.window["id"], "op", "operator", {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=1, minutes=10))})
        impacts = {x["schedule_id"]: x for x in changed["impacts"]}
        self.assertEqual(impacts[schedule["id"]]["action"], "preserve_received_data")
        self.assertEqual(impacts[schedule2["id"]]["action"], "preempted")
        self.assertEqual(self.svc.get_schedule(schedule2["id"])["status"], "preempted")

    def test_conflicts_permissions_and_data_protection(self):
        req = self.request(10000); schedule = self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=30)), "rate_mbps": 50})
        req2 = self.request(10000)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req2["id"], "requester-t1", "requester", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=20)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ApiError) as ctx:
            self.svc.schedule_request(req2["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1", "starts_at": iso(self.now + timedelta(minutes=10)), "ends_at": iso(self.now + timedelta(minutes=40)), "rate_mbps": 50})
        self.assertEqual(ctx.exception.code, "antenna_conflict")
        self.svc.transition(schedule["id"], "op", "operator", "", "receiving", {})
        self.svc.transition(schedule["id"], "op", "operator", "", "received", {})
        with self.assertRaises(ApiError) as ctx:
            self.svc.cancel_schedule(schedule["id"], "op", "operator", "", {"reason": "测试"})
        self.assertEqual(ctx.exception.code, "received_data_protected")

class WindowDispositionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = SatelliteSchedulingService(Path(self.tmp.name) / "test.db")
        self.now = utcnow() + timedelta(hours=1)
        self.svc.create_satellite("op", "operator", {"id": "SAT1", "name": "遥感一号", "data_rate_mbps": 100, "priority": 8, "storage_capacity_mb": 100000, "tenant": "T1"})
        self.svc.create_station("op", "operator", {"id": "GS1", "name": "北京站", "weather": "clear"})
        self.svc.create_antenna("op", "operator", {"id": "ANT1", "station_id": "GS1", "max_rate_mbps": 80})
        self.window = self.svc.create_window("op", "operator", {"satellite_id": "SAT1", "station_id": "GS1", "starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(hours=2)), "max_rate_mbps": 70})
        self.svc.set_quota("op", "operator", {"tenant": "T1", "station_id": "GS1", "daily_seconds": 100000})

    def tearDown(self): self.tmp.cleanup()

    def make(self, mb, priority, minutes_start, minutes_end, rate=50):
        req = self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": mb, "priority": priority, "deadline": iso(self.now + timedelta(days=1))})
        sched = self.svc.schedule_request(req["id"], "op", "operator", {"window_id": self.window["id"], "antenna_id": "ANT1",
                                                                        "starts_at": iso(self.now + timedelta(minutes=minutes_start)),
                                                                        "ends_at": iso(self.now + timedelta(minutes=minutes_end)), "rate_mbps": rate})
        return req, sched

    def change(self, minutes_end, **extra):
        body = {"starts_at": iso(self.now), "ends_at": iso(self.now + timedelta(minutes=minutes_end))}
        body.update(extra)
        return self.svc.change_window(self.window["id"], "op", "operator", body)

    def test_stale_revision_conflicts_and_reads_back_previous_version(self):
        req, sched = self.make(10000, 5, 0, 30)
        before_window = dict(self.svc.repo.conn.execute("SELECT * FROM visibility_windows WHERE id=?", (self.window["id"],)).fetchone())
        with self.assertRaises(ApiError) as ctx:
            self.change(60, expected_revision=999)
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(ctx.exception.code, "window_revision_conflict")
        self.assertEqual(ctx.exception.details["current_revision"], 1)
        self.assertEqual(ctx.exception.details["window"]["starts_at"], before_window["starts_at"])
        # 窗口、排程和请求都保持在上一版
        after = dict(self.svc.repo.conn.execute("SELECT * FROM visibility_windows WHERE id=?", (self.window["id"],)).fetchone())
        self.assertEqual(after, before_window)
        self.assertEqual(self.svc.get_schedule(sched["id"])["status"], "scheduled")
        self.assertEqual(dict(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone())["status"], "scheduled")
        # 冲突处置单留痕，可在状态页数据中查到
        disp = self.svc.get_disposition(ctx.exception.details["disposition_id"], "operator", "")
        self.assertEqual(disp["status"], "conflicted")
        self.assertIsNone(disp["final_revision"])
        self.assertEqual(self.svc.state("operator", "")["dispositions"][0]["status"], "conflicted")

    def test_compression_updates_schedule_times_and_rate(self):
        req, sched = self.make(3000, 7, 10, 30, rate=50)  # 裁到 10~20 分钟需 3000*8/600=40Mbps
        status, changed = self.change(20)
        impacts = {i["schedule_id"]: i for i in changed["impacts"]}
        self.assertEqual(impacts[sched["id"]]["action"], "compressed")
        self.assertEqual(impacts[sched["id"]]["new_rate_mbps"], 50)  # 原速率已够用，不抬高
        updated = self.svc.get_schedule(sched["id"])
        self.assertEqual(updated["starts_at"], iso(self.now + timedelta(minutes=10)))
        self.assertEqual(updated["ends_at"], iso(self.now + timedelta(minutes=20)))
        self.assertEqual(changed["window"]["revision"], 2)

    def test_insufficient_capacity_preempts_and_queues(self):
        req, sched = self.make(10000, 7, 0, 30, rate=50)  # 裁到 10 分钟时需 ~133Mbps，上限 70
        status, changed = self.change(10)
        impacts = {i["schedule_id"]: i for i in changed["impacts"]}
        self.assertEqual(impacts[sched["id"]]["action"], "preempted")
        self.assertIn("容量不足", impacts[sched["id"]]["reason"])
        self.assertTrue(impacts[sched["id"]]["queued"])
        self.assertEqual(self.svc.get_schedule(sched["id"])["status"], "preempted")
        self.assertEqual(dict(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone())["status"], "queued")
        queue = self.svc.state("operator", "")["queue"]
        self.assertEqual([q["request_id"] for q in queue], [req["id"]])
        # 排队请求可以重排
        self.svc.reschedule(req["id"], "op", "operator", {})
        self.assertEqual(dict(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone())["status"], "pending")

    def test_priority_preemption_when_capacity_contended(self):
        # 高优先级排 0~15 分钟，低优先级排 14~35 分钟（直接布置到快照中，模拟窗口收缩前两者无重叠校验的情形）；
        # 窗口裁到 20 分钟后，低优先级自身仍可行（6 分钟需 66.7Mbps≤70），但在 14~15 分钟与高优先级争用同一天线
        _, hi = self.make(3000, 9, 0, 15, rate=40)
        lo_req = self.svc.create_request("requester-t1", "requester", "T1", {"satellite_id": "SAT1", "data_mb": 3000, "priority": 3, "deadline": iso(self.now + timedelta(days=1))})
        cur = self.svc.repo.conn.execute("""INSERT INTO schedules(request_id,window_id,station_id,antenna_id,satellite_id,starts_at,ends_at,rate_mbps,created_by,created_at,updated_at)
                                            VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                                         (lo_req["id"], self.window["id"], "GS1", "ANT1", "SAT1",
                                          iso(self.now + timedelta(minutes=14)), iso(self.now + timedelta(minutes=35)), 40.0, "op", iso(), iso()))
        lo_id = cur.lastrowid
        status, changed = self.change(20)
        actions = {i["schedule_id"]: i for i in changed["impacts"]}
        self.assertEqual(actions[hi["id"]]["action"], "retained")
        self.assertEqual(actions[lo_id]["action"], "preempted")
        self.assertIn("优先级", actions[lo_id]["reason"])
        self.assertEqual(self.svc.get_schedule(lo_id)["status"], "preempted")
        self.assertEqual(self.svc.get_schedule(hi["id"])["ends_at"], iso(self.now + timedelta(minutes=15)))

    def test_write_failure_restores_pre_commit_values_and_records_failed_disposition(self):
        req, sched = self.make(3000, 7, 0, 30, rate=50)
        original = dict(self.svc.repo.conn.execute("SELECT * FROM schedules WHERE id=?", (sched["id"],)).fetchone())

        class FailingConn:
            def __init__(self, real): self.__dict__["_real"] = real
            def execute(self, sql, params=()):
                if sql.lstrip().upper().startswith("UPDATE VISIBILITY_WINDOWS"):
                    raise sqlite3.OperationalError("simulated disk failure")
                return self._real.execute(sql, params)
            def __getattr__(self, name): return getattr(self._real, name)
            def __setattr__(self, name, value): setattr(self._real, name, value)

        self.svc.repo.conn = FailingConn(self.svc.repo.conn)
        with self.assertRaises(ApiError) as ctx:
            self.change(20)
        self.assertEqual(ctx.exception.status, 500)
        self.assertEqual(ctx.exception.code, "window_change_write_failed")
        self.svc.repo.conn = self.svc.repo.conn._real
        # 窗口、排程与请求恢复提交前数值
        win = dict(self.svc.repo.conn.execute("SELECT * FROM visibility_windows WHERE id=?", (self.window["id"],)).fetchone())
        self.assertEqual(win["revision"], 1)
        self.assertEqual(win["ends_at"], iso(self.now + timedelta(hours=2)))
        restored = dict(self.svc.repo.conn.execute("SELECT * FROM schedules WHERE id=?", (sched["id"],)).fetchone())
        self.assertEqual(restored["status"], original["status"])
        self.assertEqual(restored["ends_at"], original["ends_at"])
        self.assertEqual(dict(self.svc.repo.conn.execute("SELECT status FROM requests WHERE id=?", (req["id"],)).fetchone())["status"], "scheduled")
        # 失败处置单留痕，含规划出的影响明细
        disp = self.svc.get_disposition(ctx.exception.details["disposition_id"], "operator", "")
        self.assertEqual(disp["status"], "failed")
        self.assertIn("simulated disk failure", disp["error"])
        self.assertTrue(disp["impacts"])

    def test_preview_lists_impacts_without_writing(self):
        req, sched = self.make(3000, 7, 0, 30, rate=50)
        status, changed = self.change(20, commit=False)
        self.assertEqual(status, 200)
        self.assertFalse(changed["committed"])
        self.assertEqual(changed["impacts"][0]["action"], "compressed")
        self.assertEqual(dict(self.svc.repo.conn.execute("SELECT revision FROM visibility_windows WHERE id=?", (self.window["id"],)).fetchone())["revision"], 1)
        self.assertEqual(self.svc.get_schedule(sched["id"])["ends_at"], iso(self.now + timedelta(minutes=30)))
        self.assertEqual(self.svc.state("operator", "")["dispositions"][0]["status"], "previewed")

    def test_commit_with_correct_revision_records_applied_disposition(self):
        req, sched = self.make(3000, 7, 0, 30, rate=50)
        status, changed = self.change(20, expected_revision=1)
        self.assertEqual(status, 200)
        self.assertEqual(changed["base_revision"], 1)
        self.assertEqual(changed["final_revision"], 2)
        state = self.svc.state("operator", "")
        self.assertEqual(state["dispositions"][0]["status"], "applied")
        self.assertEqual(len(state["dispositions"][0]["impacts"]), 1)
        # 租户只能看到自己请求的影响明细
        own = self.svc.state("requester", "T1")
        self.assertTrue(all(i.get("request_id") == req["id"] for d in own["dispositions"] for i in d["impacts"]))
        # 版本推进后，旧基线再次提交即冲突（模拟两个排程员后提交者）
        with self.assertRaises(ApiError) as ctx:
            self.change(15, expected_revision=1)
        self.assertEqual(ctx.exception.code, "window_revision_conflict")


if __name__ == "__main__": unittest.main()
