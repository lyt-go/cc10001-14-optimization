"""告警判定接口层测试：直接调用路由函数，验证入参模型与端到端串联。"""
from __future__ import annotations

import unittest

from app.routers import alertengine as engine_router
from app.routers import alertrule as rule_router
from app.routers import diagnose as diagnose_router
from app.schemas import EntryPayload
from app.seed import SEED_ROWS
from app.services.alertengine import EPISODE_MODULE, NOTIFY_MODULE
from app.services.diagnose import DiagnoseService
from app.store import store


class AlertApiTest(unittest.TestCase):
    def setUp(self) -> None:
        store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
        store.rows(EPISODE_MODULE)
        store.rows(NOTIFY_MODULE)
        DiagnoseService().create_entry(
            {"报告编号": "DIAG-219", "关联任务": "历史任务", "异常类型": "历史异常"}
        )
        self.rule = next(r for r in store.rows("alertrule") if r["告警编号"] == "ALERT-06")

    def test_http_style_full_flow(self) -> None:
        # 静默期三条 + 静默后一条，走 ingest 接口模型
        for value, ts, batch in [
            (60, "2026-11-13 23:00", "H1"),
            (70, "2026-11-13 23:30", "H2"),
            (80, "2026-11-14 06:30", "H3"),
        ]:
            r = engine_router.ingest(
                engine_router.IngestPayload(rule="ALERT-06", value=value, ts=ts, batch_id=batch)
            )
            self.assertFalse(r["notified"])
        r = engine_router.ingest(
            engine_router.IngestPayload(rule="ALERT-06", value=90, ts="2026-11-14 07:10", batch_id="H4")
        )
        self.assertTrue(r["notified"])

        # 通知接口：值班渠道仅一条升级通知，且带静默合并数量
        notifications = engine_router.notifications("ALERT-06")["items"]
        self.assertEqual(len(notifications), 1)
        self.assertEqual(notifications[0]["静默合并次数"], 3)

        # 批次接口幂等：同一批次重复上报只处理一次
        payload = engine_router.BatchPayload(
            batch_id="BATCH-1",
            samples=[
                {"rule": "ALERT-06", "value": 20, "ts": "2026-11-14 09:00"},
            ],
        )
        first = engine_router.ingest_batch(payload)
        second = engine_router.ingest_batch(payload)
        self.assertFalse(first["deduped"])
        self.assertTrue(second["deduped"])
        # 稳定周期第 2 个正常样本走另一个批次，触发结案
        engine_router.ingest(
            engine_router.IngestPayload(rule="ALERT-06", value=15, ts="2026-11-14 10:00", batch_id="BATCH-2")
        )

        # 诊断列表接口能查到已结案报告，结案结论含原阈值版本
        diag_page = diagnose_router.list_entries(keyword=None, status=None, size=100)
        closed = [
            item for item in diag_page.items
            if item.get("告警编号") == "ALERT-06" and item["status"] == "已结案"
        ]
        self.assertEqual(len(closed), 1)
        self.assertTrue(closed[0]["结案时间"])
        self.assertEqual(closed[0]["阈值版本"], "v1")

        # 相互印证接口通过
        self.assertTrue(engine_router.verify()["ok"])

    def test_rule_update_route_bumps_version(self) -> None:
        result = rule_router.update_entry(
            self.rule["id"],
            EntryPayload(values={"阈值数值": 45, "持续周期": 5}),
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.entry["规则版本"], "v2")
        self.assertEqual(result.entry["阈值数值"], 45)
        self.assertEqual(len(result.entry["版本历史"]), 2)
        # 无实质变化不升版本
        again = rule_router.update_entry(self.rule["id"], EntryPayload(values={"阈值数值": 45}))
        self.assertTrue(again.ok)
        self.assertEqual(again.entry["规则版本"], "v2")


if __name__ == "__main__":
    unittest.main()
