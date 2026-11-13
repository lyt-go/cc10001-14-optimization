"""告警判定引擎的完整场景测试。

覆盖需求：
1. 静默窗口不新增通知，但保留最后越阈时间与原始指标；
2. 连续越阈达持续周期才升级，恢复后经稳定周期才关闭，瞬时波动不开关；
3. 诊断报告随告警同步，恢复写结案结论，再次升级不覆盖已结案历史报告；
4. 阈值/持续周期调整升版本，后续按新口径判断，已结案报告保留旧版本口径；
5. 同采集批次并发/重复上报只记一次；
6. 静默结束后首次触发汇总被合并告警数量，值班渠道只收到一条升级通知；
7. 告警与诊断相互印证校验。
"""
from __future__ import annotations

import threading
import unittest

from app.seed import SEED_ROWS
from app.services.alertrule import AlertruleService
from app.services.alertengine import (
    EPISODE_MODULE,
    NOTIFY_MODULE,
    STATE_ALERTING,
    STATE_NORMAL,
    STATE_PENDING,
    STATE_RECOVERING,
    AlertEngine,
    in_silence,
)
from app.services.diagnose import DiagnoseService
from app.store import store


def _reset_store() -> None:
    """把内存仓库恢复成种子数据，并清空引擎运行期表与批次记录。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store.rows(EPISODE_MODULE)
    store.rows(NOTIFY_MODULE)
    engine = AlertEngine()
    engine._seen_batches.clear()
    # 预置 DIAG-219，让本次场景里引擎生成的首份报告自然落在 DIAG-220
    DiagnoseService().create_entry(
        {"报告编号": "DIAG-219", "关联任务": "历史任务", "异常类型": "历史异常"}
    )
    report = store.find("diagnose", max(r["id"] for r in store.rows("diagnose")))
    report["status"] = "已结案"
    report["pending"] = False
    report["结案时间"] = "2026-11-01T10:00:00"


class AlertEngineScenarioTest(unittest.TestCase):
    def setUp(self) -> None:
        _reset_store()
        self.engine = AlertEngine()
        self.rule_service = AlertruleService()
        self.rule = next(r for r in store.rows("alertrule") if r["告警编号"] == "ALERT-06")
        self.rule_id = self.rule["id"]

    # ------------------------------------------------------------ 1. 主生命周期
    def test_alert06_full_lifecycle_with_silence_and_diagnose(self) -> None:
        # 23:00 处于静默窗口（22:00-07:00），三次越阈只记录不通知
        r1 = self.engine.ingest("ALERT-06", 60, "2026-11-13 23:00", batch_id="B1")
        r2 = self.engine.ingest("ALERT-06", 70, "2026-11-13 23:30", batch_id="B1")
        r3 = self.engine.ingest("ALERT-06", 80, "2026-11-14 06:30", batch_id="B1")
        self.assertTrue(all(r["silenced"] for r in (r1, r2, r3)))
        self.assertTrue(all(not r["notified"] for r in (r1, r2, r3)))
        runtime = self.rule["runtime"]
        self.assertEqual(runtime["merged_count"], 3)
        self.assertEqual(runtime["last_breach_at"], "2026-11-14T06:30:00")
        self.assertEqual(runtime["last_metric"], 80.0)
        self.assertEqual(len(store.rows(NOTIFY_MODULE)), 0)

        # 07:10 静默结束后首次越阈：连续越阈已达 3 周期，升级并汇总合并次数
        r4 = self.engine.ingest("ALERT-06", 90, "2026-11-14 07:10", batch_id="B2")
        self.assertFalse(r4["silenced"])
        self.assertTrue(r4["notified"])
        self.assertEqual(runtime["state"], STATE_ALERTING)
        notifications = store.rows(NOTIFY_MODULE)
        self.assertEqual(len(notifications), 1)  # 值班渠道只收到一条升级通知
        self.assertEqual(notifications[0]["静默合并次数"], 3)
        self.assertEqual(notifications[0]["告警级别"], "严重")
        self.assertEqual(notifications[0]["通知渠道"], "值班渠道")
        self.assertIn("合并 3 次", notifications[0]["通知内容"])
        self.assertEqual(runtime["merged_count"], 0)

        # 诊断报告随升级创建，且编号落在 DIAG-220，处于分析中并冻结 v1 口径
        reports = [r for r in store.rows("diagnose") if r.get("告警编号") == "ALERT-06"]
        self.assertEqual(len(reports), 1)
        report = reports[0]
        self.assertEqual(report["报告编号"], "DIAG-220")
        self.assertEqual(report["status"], "分析中")
        self.assertEqual(report["告警级别"], "严重")
        self.assertEqual(report["阈值版本"], "v1")
        self.assertEqual(report["阈值快照"]["阈值数值"], 50)

        # 告警中继续越阈不重复通知
        r5 = self.engine.ingest("ALERT-06", 95, "2026-11-14 08:00", batch_id="B3")
        self.assertFalse(r5["notified"])
        self.assertEqual(len(store.rows(NOTIFY_MODULE)), 1)

        # 恢复第 1 周期：进入待恢复，报告仍分析中
        r6 = self.engine.ingest("ALERT-06", 20, "2026-11-14 09:00", batch_id="B4")
        self.assertEqual(runtime["state"], STATE_RECOVERING)
        self.assertEqual(report["status"], "分析中")
        self.assertIsNone(report["结案时间"])

        # 恢复第 2 周期（稳定周期=2）：关闭告警，报告写入结案时间与原阈值版本
        r7 = self.engine.ingest("ALERT-06", 18, "2026-11-14 10:00", batch_id="B5")
        self.assertEqual(runtime["state"], STATE_NORMAL)
        self.assertEqual(r7["episode"], "ALERT-06-E01")
        self.assertEqual(report["status"], "已结案")
        self.assertIsNotNone(report["结案时间"])
        self.assertIn("v1", report["结案结论"])
        self.assertIn("级别=严重", report["结案结论"])
        episode = store.rows(EPISODE_MODULE)[0]
        self.assertEqual(episode["状态"], "已关闭")
        self.assertIsNotNone(episode["关闭时间"])

        # 再次越阈升级：新建第二份报告，已结案的 DIAG-220 不得被覆盖
        for idx, (val, hhmm) in enumerate(
            [(55, "11:00"), (60, "11:30"), (65, "12:00")], start=6
        ):
            self.engine.ingest("ALERT-06", val, f"2026-11-14 {hhmm}", batch_id=f"B{idx}")
        self.assertEqual(runtime["state"], STATE_ALERTING)
        reports = [r for r in store.rows("diagnose") if r.get("告警编号") == "ALERT-06"]
        self.assertEqual(len(reports), 2)
        self.assertEqual(reports[0]["报告编号"], "DIAG-220")
        self.assertEqual(reports[0]["status"], "已结案")  # 历史报告原样保留
        self.assertEqual(reports[1]["status"], "分析中")
        self.assertEqual(reports[1]["报告编号"], "DIAG-221")
        episodes = store.rows(EPISODE_MODULE)
        self.assertEqual(len(episodes), 2)
        self.assertEqual(episodes[1]["告警实例编号"], "ALERT-06-E02")

        # 三方相互印证应全部通过
        verdict = self.engine.verify()
        self.assertTrue(verdict["ok"], verdict)

    # ------------------------------------------------------------ 2. 版本化口径
    def test_threshold_version_change_and_closed_report_preservation(self) -> None:
        # 先用 v1（阈值 50、持续 3）打出一个完整告警并结案
        for idx, (val, hhmm) in enumerate(
            [(60, "08:00"), (62, "08:30"), (64, "09:00"), (20, "09:30"), (18, "10:00")], start=1
        ):
            self.engine.ingest("ALERT-06", val, f"2026-11-14 {hhmm}", batch_id=f"V{idx}")
        closed = [r for r in store.rows("diagnose") if r.get("告警编号") == "ALERT-06"][0]
        self.assertEqual(closed["status"], "已结案")
        self.assertEqual(closed["阈值版本"], "v1")

        # 调整阈值 50 -> 40、持续周期 3 -> 2，版本升到 v2
        _, message = self.rule_service.update_entry(
            self.rule_id, {"阈值数值": 40, "持续周期": 2}
        )
        self.assertIn("v2", message)
        self.assertEqual(self.rule["规则版本"], "v2")
        self.assertEqual(len(self.rule["版本历史"]), 2)

        # 值 45：对新阈值 40 越阈，对旧阈值 50 不越阈；连续 2 周期即升级
        r = self.engine.ingest("ALERT-06", 45, "2026-11-14 11:00", batch_id="N1")
        self.assertTrue(r["breached"])
        self.assertEqual(r["state"], STATE_PENDING)
        r = self.engine.ingest("ALERT-06", 46, "2026-11-14 11:30", batch_id="N2")
        self.assertTrue(r["notified"])
        new_report = [r for r in store.rows("diagnose") if r.get("告警编号") == "ALERT-06"][-1]
        self.assertEqual(new_report["阈值版本"], "v2")
        self.assertEqual(new_report["阈值快照"]["阈值数值"], 40)

        # 已结案旧报告保留 v1 口径，不被新结论覆盖；版本冲突以报告内版本为准
        self.assertEqual(closed["阈值版本"], "v1")
        self.assertEqual(closed["阈值快照"]["阈值数值"], 50)
        self.assertNotEqual(closed["报告编号"], new_report["报告编号"])
        self.assertTrue(self.engine.verify()["ok"])

    # ------------------------------------------------------------ 3. 批次幂等
    def test_concurrent_same_batch_only_recorded_once(self) -> None:
        samples = [
            {"rule": "ALERT-06", "value": 60, "ts": "2026-11-14 08:00"},
            {"rule": "ALERT-06", "value": 65, "ts": "2026-11-14 08:30"},
        ]
        results: list[dict] = []

        def worker() -> None:
            results.append(self.engine.ingest_batch("BATCH-DUP", samples))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        first_pass = [r for r in results if not r["deduped"]]
        deduped = [r for r in results if r["deduped"]]
        self.assertEqual(len(first_pass), 1)
        self.assertEqual(len(deduped), 4)
        # 两条样本各只计一次，越阈计数没有被并发放大
        self.assertEqual(self.rule["runtime"]["breach_streak"], 2)

        # 再次显式上报同批次仍然幂等
        again = self.engine.ingest_batch("BATCH-DUP", samples)
        self.assertTrue(again["deduped"])
        self.assertEqual(self.rule["runtime"]["breach_streak"], 2)

    # ------------------------------------------------------------ 4. 瞬时波动
    def test_transient_flap_does_not_switch_alert(self) -> None:
        r1 = self.engine.ingest("ALERT-06", 80, "2026-11-14 08:00", batch_id="F1")
        r2 = self.engine.ingest("ALERT-06", 90, "2026-11-14 08:30", batch_id="F2")
        self.assertEqual(r2["state"], STATE_PENDING)
        # 第 3 周期恢复，未达持续周期 3，不升级、不产生报告/通知
        r3 = self.engine.ingest("ALERT-06", 10, "2026-11-14 09:00", batch_id="F3")
        self.assertEqual(r3["state"], STATE_NORMAL)
        self.assertIn("瞬时波动", r3["message"])
        self.assertEqual(len(store.rows(EPISODE_MODULE)), 0)
        self.assertEqual(len(store.rows(NOTIFY_MODULE)), 0)
        self.assertFalse(any(r.get("告警编号") == "ALERT-06" for r in store.rows("diagnose")))

    # ------------------------------------------------------------ 5. 静默窗口解析
    def test_silence_window_boundaries_and_overnight(self) -> None:
        silence = "22:00-07:00"
        from datetime import datetime

        self.assertTrue(in_silence(datetime(2026, 11, 13, 22, 0), silence))
        self.assertTrue(in_silence(datetime(2026, 11, 14, 0, 0), silence))
        self.assertTrue(in_silence(datetime(2026, 11, 14, 6, 59), silence))
        self.assertFalse(in_silence(datetime(2026, 11, 14, 7, 0), silence))
        self.assertFalse(in_silence(datetime(2026, 11, 14, 12, 0), silence))
        self.assertFalse(in_silence(datetime(2026, 11, 14, 21, 59), silence))

    # ------------------------------------------------------------ 6. 恢复中断
    def test_recover_interrupted_keeps_single_episode_and_notification(self) -> None:
        # 连续 3 周期越阈升级
        for idx, hhmm in enumerate(["08:00", "08:30", "09:00"], start=1):
            self.engine.ingest("ALERT-06", 70, f"2026-11-14 {hhmm}", batch_id=f"R{idx}")
        self.assertEqual(self.rule["runtime"]["state"], STATE_ALERTING)
        # 恢复 1 周期后再次越阈：回到告警中，沿用同一实例且不新增通知
        self.engine.ingest("ALERT-06", 20, "2026-11-14 09:30", batch_id="R4")
        self.assertEqual(self.rule["runtime"]["state"], STATE_RECOVERING)
        self.engine.ingest("ALERT-06", 88, "2026-11-14 10:00", batch_id="R5")
        self.assertEqual(self.rule["runtime"]["state"], STATE_ALERTING)
        self.assertEqual(len(store.rows(EPISODE_MODULE)), 1)
        self.assertEqual(len(store.rows(NOTIFY_MODULE)), 1)
        # 之后稳定两个周期才真正关闭
        self.engine.ingest("ALERT-06", 15, "2026-11-14 10:30", batch_id="R6")
        self.assertEqual(self.rule["runtime"]["state"], STATE_RECOVERING)
        self.engine.ingest("ALERT-06", 12, "2026-11-14 11:00", batch_id="R7")
        self.assertEqual(self.rule["runtime"]["state"], STATE_NORMAL)
        self.assertEqual(store.rows(EPISODE_MODULE)[0]["状态"], "已关闭")
        self.assertTrue(self.engine.verify()["ok"])

    # ------------------------------------------------------------ 7. 未生效规则
    def test_inactive_rule_is_skipped(self) -> None:
        # 种子里的 ALER-0001 是草稿态
        result = self.engine.ingest("ALER-0001", 999, "2026-11-14 08:00", batch_id="X1")
        self.assertFalse(result["ok"])
        self.assertIn("未生效", result["message"])


if __name__ == "__main__":
    unittest.main()
