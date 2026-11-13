"""采集告警判定引擎。

把告警规则、采集上报和诊断报告串成一条完整的状态机，核心口径：

1. 静默窗口内只记录越阈（保留最后一次越阈时间与原始指标），不新增通知；
   静默结束后的第一次触发，需要在通知里说明被合并的告警数量。
2. 连续越阈达到「持续周期」才升级告警；指标恢复后再连续正常「稳定周期」
   个周期才关闭告警，避免瞬时波动反复开关。
3. 诊断报告随告警状态同步创建/结案；已经结案的历史报告不会被再次升级覆盖，
   再次升级时新建一份报告。
4. 规则阈值、持续周期等调整后版本号递增，后续采集按新口径判断；已结案报告
   保留生成时记录的告警级别与阈值版本，结论冲突以报告内版本为准。
5. 同一采集批次并发/重复上报只处理一次（批次幂等）。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.services.diagnose import DiagnoseService
from app.store import store

ALERT_MODULE = "alertrule"
EPISODE_MODULE = "alert_episode"
NOTIFY_MODULE = "alert_notification"

# 告警运行态：正常 -> 待确认（连续越阈计数中）-> 告警中 -> 待恢复（连续正常计数中）
STATE_NORMAL = "正常"
STATE_PENDING = "待确认"
STATE_ALERTING = "告警中"
STATE_RECOVERING = "待恢复"

ACTIVE_RULE_STATUS = "已生效"

# 比较运算符：兼容符号与中文写法
_OPERATORS = {
    ">": lambda a, b: a > b,
    ">=": lambda a, b: a >= b,
    "<": lambda a, b: a < b,
    "<=": lambda a, b: a <= b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
    "大于": lambda a, b: a > b,
    "大于等于": lambda a, b: a >= b,
    "小于": lambda a, b: a < b,
    "小于等于": lambda a, b: a <= b,
    "等于": lambda a, b: a == b,
    "不等于": lambda a, b: a != b,
}

# 生成报告/告警实例/通知编号时的起始序号（演示环境里让首个报告落在 DIAG-220）
DIAG_SEQ_START = 220
EPISODE_SEQ_START = 1
NOTIFY_SEQ_START = 1


def _to_number(value: Any) -> float:
    """把上报值/阈值转成数值；无法解析时抛 ValueError，由调用方决定口径。"""
    if isinstance(value, bool):  # bool 是 int 的子类，单独拦一下
        raise ValueError(f"布尔值不能作为指标值：{value!r}")
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().rstrip("%").strip()
    if not text:
        raise ValueError("指标值为空")
    return float(text)


def _parse_ts(ts: Any) -> datetime:
    """解析上报时间，支持 ISO8601、「YYYY-MM-DD HH:MM[:SS]」与时间戳。"""
    if isinstance(ts, datetime):
        return ts
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(float(ts))
    text = str(ts).strip()
    if text.isdigit():
        return datetime.fromtimestamp(int(text))
    return datetime.fromisoformat(text.replace("Z", ""))


def parse_silence_window(silence: Any) -> tuple[int, int] | None:
    """解析「22:00-07:00」这类静默时段，返回 (起分钟, 止分钟)；跨零点起止反转。"""
    if not silence:
        return None
    text = str(silence).strip().replace("~", "-").replace("—", "-")
    if "-" not in text:
        return None
    start_text, end_text = (part.strip() for part in text.split("-", 1))
    try:
        sh, sm = (int(x) for x in start_text.split(":"))
        eh, em = (int(x) for x in end_text.split(":"))
    except ValueError:
        return None
    start, end = sh * 60 + sm, eh * 60 + em
    if not (0 <= start < 1440 and 0 <= end < 1440) or start == end:
        return None
    return start, end


def in_silence(ts: datetime, silence: Any) -> bool:
    """判断上报时刻是否落在静默窗口内，支持跨零点窗口（如 22:00-07:00）。"""
    window = parse_silence_window(silence)
    if window is None:
        return False
    start, end = window
    minute = ts.hour * 60 + ts.minute
    if start < end:
        return start <= minute < end
    # 跨零点：22:00 之后或 07:00 之前都算静默
    return minute >= start or minute < end


class AlertEngine:
    """告警判定引擎，单例使用；内部加锁保证并发上报的批次幂等与状态一致。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._seen_batches: set[str] = set()

    # ------------------------------------------------------------------ 基础工具
    def _table(self, module: str) -> list[dict[str, Any]]:
        return store.rows(module)

    def _find_rule(self, rule_ref: Any) -> dict[str, Any] | None:
        """按规则主键 id 或告警编号（如 ALERT-06）定位规则。"""
        rows = self._table(ALERT_MODULE)
        if isinstance(rule_ref, int) or (isinstance(rule_ref, str) and rule_ref.isdigit()):
            return store.find(ALERT_MODULE, int(rule_ref))
        for row in rows:
            if row.get("告警编号") == rule_ref:
                return row
        return None

    @staticmethod
    def _runtime(rule: dict[str, Any]) -> dict[str, Any]:
        runtime = rule.get("runtime")
        if not isinstance(runtime, dict):
            runtime = {
                "state": STATE_NORMAL,
                "breach_streak": 0,
                "recover_streak": 0,
                "last_breach_at": None,
                "last_metric": None,
                "merged_count": 0,
                "active_episode_code": None,
                "episode_seq": 0,
                "was_silenced": False,
            }
            rule["runtime"] = runtime
        return runtime

    @staticmethod
    def _version_of(rule: dict[str, Any]) -> str:
        return str(rule.get("规则版本") or "v1")

    def _snapshot(self, rule: dict[str, Any]) -> dict[str, Any]:
        """读取规则当前口径，作为告警实例/报告里冻结的阈值版本。"""
        return {
            "规则版本": self._version_of(rule),
            "监控指标": rule.get("监控指标"),
            "比较运算符": rule.get("比较运算符"),
            "阈值数值": rule.get("阈值数值"),
            "持续周期": rule.get("持续周期"),
            "稳定周期": rule.get("稳定周期", 2),
            "告警级别": rule.get("告警级别"),
            "静默时段": rule.get("静默时段"),
        }

    def _is_breach(self, rule: dict[str, Any], value: float) -> bool:
        operator = _OPERATORS.get(str(rule.get("比较运算符") or "").strip())
        if operator is None:
            raise ValueError(f"规则 {rule.get('告警编号')} 的比较运算符无法识别：{rule.get('比较运算符')!r}")
        return operator(value, _to_number(rule.get("阈值数值")))

    # ------------------------------------------------------------------ 上报入口
    def ingest_batch(self, batch_id: str, samples: list[dict[str, Any]]) -> dict[str, Any]:
        """采集批次上报：同批次只处理一次，批次内每条样本串行判定。"""
        with self._lock:
            if batch_id in self._seen_batches:
                return {
                    "batch_id": batch_id,
                    "deduped": True,
                    "processed": 0,
                    "results": [],
                    "message": f"采集批次 {batch_id} 已处理过，本次上报已合并去重",
                }
            results = [
                self.ingest(
                    rule_ref=sample.get("rule") or sample.get("rule_id") or sample.get("告警编号"),
                    value=sample.get("value", sample.get("metric")),
                    ts=sample.get("ts") or sample.get("time"),
                    batch_id=batch_id,
                )
                for sample in samples
            ]
            self._seen_batches.add(batch_id)
            return {
                "batch_id": batch_id,
                "deduped": False,
                "processed": len(results),
                "results": results,
                "message": f"采集批次 {batch_id} 处理完成，共判定 {len(results)} 条样本",
            }

    def ingest(
        self,
        rule_ref: Any,
        value: Any,
        ts: Any,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """单条采集样本上报，驱动规则状态机并同步诊断报告。"""
        with self._lock:
            if batch_id and batch_id in self._seen_batches:
                return {"rule": rule_ref, "deduped": True, "message": f"批次 {batch_id} 已处理，样本已去重"}
            rule = self._find_rule(rule_ref)
            if rule is None:
                return {"rule": rule_ref, "ok": False, "message": f"告警规则 {rule_ref} 不存在"}
            if rule.get("status") != ACTIVE_RULE_STATUS:
                return {
                    "rule": rule.get("告警编号"),
                    "ok": False,
                    "message": f"规则状态为「{rule.get('status')}」，未生效不参与判定",
                }

            moment = _parse_ts(ts)
            metric = _to_number(value)
            silenced = in_silence(moment, rule.get("静默时段"))
            runtime = self._runtime(rule)
            runtime["last_metric"] = metric

            try:
                breached = self._is_breach(rule, metric)
            except ValueError as exc:
                return {"rule": rule.get("告警编号"), "ok": False, "message": str(exc)}

            if breached:
                # 静默窗口内不新增通知，但最后一次越阈时间与原始指标必须保留
                runtime["last_breach_at"] = moment.isoformat()
                runtime["breach_streak"] += 1
                runtime["recover_streak"] = 0
                result = self._handle_breach(rule, metric, moment, silenced, batch_id)
            else:
                runtime["recover_streak"] += 1
                runtime["breach_streak"] = 0
                result = self._handle_recover(rule, metric, moment, silenced, batch_id)

            result.update(
                {
                    "ok": True,
                    "rule": rule.get("告警编号"),
                    "batch_id": batch_id,
                    "metric": metric,
                    "ts": moment.isoformat(),
                    "silenced": silenced,
                    "breached": breached,
                    "state": runtime["state"],
                    "last_breach_at": runtime["last_breach_at"],
                    "merged_count": runtime["merged_count"],
                }
            )
            return result

    # ------------------------------------------------------------------ 越阈分支
    def _handle_breach(
        self,
        rule: dict[str, Any],
        metric: float,
        moment: datetime,
        silenced: bool,
        batch_id: str | None,
    ) -> dict[str, Any]:
        runtime = self._runtime(rule)
        duration = int(_to_number(rule.get("持续周期")))
        notifications: list[dict[str, Any]] = []

        if runtime["state"] == STATE_RECOVERING:
            # 稳定周期没走完又越阈：回到告警中，沿用未关闭的告警实例，不重复通知
            runtime["state"] = STATE_ALERTING
            episode = self._find_active_episode(runtime)
            if episode is not None:
                episode["状态"] = STATE_ALERTING
                episode["状态轨迹"].append(f"{moment.isoformat()} 稳定周期内再次越阈（指标={metric}），恢复中断，告警持续")
            return {"notified": False, "message": "恢复稳定期间再次越阈，告警持续，不重复通知"}

        if silenced:
            runtime["merged_count"] += 1
            runtime["was_silenced"] = True
            if runtime["state"] == STATE_NORMAL:
                runtime["state"] = STATE_PENDING
            return {
                "notified": False,
                "message": f"静默窗口内越阈已记录（原始指标={metric}），不新增通知，累计合并 {runtime['merged_count']} 次",
            }

        if runtime["state"] == STATE_ALERTING:
            # 同一告警周期内持续越阈，值班渠道只收到一条升级通知
            return {"notified": False, "message": "告警持续中，不重复通知"}

        if runtime["breach_streak"] < duration:
            runtime["state"] = STATE_PENDING
            return {
                "notified": False,
                "message": f"连续越阈 {runtime['breach_streak']}/{duration} 周期，尚未达到持续周期",
            }

        # 连续越阈达到持续周期：升级告警（静默结束后的首次触发需汇总被合并次数）
        episode, report = self._escalate(rule, metric, moment, silenced)
        notification = self._notify(rule, episode, report, moment, batch_id)
        notifications.append(notification)
        return {
            "notified": True,
            "episode": episode["告警实例编号"],
            "report": report["报告编号"],
            "merged_count_flushed": runtime["merged_count"],
            "notifications": notifications,
            "message": "连续越阈达到持续周期，告警升级并发送通知",
        }

    def _escalate(
        self, rule: dict[str, Any], metric: float, moment: datetime, silenced: bool
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """升级告警：创建/复用告警实例，并同步新建一份分析中的诊断报告。"""
        runtime = self._runtime(rule)
        snapshot = self._snapshot(rule)

        active = self._find_active_episode(runtime)
        if active is not None and active.get("状态") in (STATE_ALERTING, STATE_RECOVERING):
            # 待恢复期间重新升级：沿用实例，但报告必须新开，已结案报告不动
            episode = active
            episode["状态"] = STATE_ALERTING
            episode["最近越阈时间"] = moment.isoformat()
            episode["状态轨迹"].append(f"{moment.isoformat()} 指标再次越阈（{metric}），告警重新升级，口径 {snapshot['规则版本']}")
        else:
            runtime["episode_seq"] += 1
            episode_code = f"{rule.get('告警编号')}-E{runtime['episode_seq']:02d}"
            episode = {
                "id": self._next_id(EPISODE_MODULE),
                "告警实例编号": episode_code,
                "告警编号": rule.get("告警编号"),
                "监控指标": rule.get("监控指标"),
                "状态": STATE_ALERTING,
                "首次越阈时间": runtime["last_breach_at"],
                "最近越阈时间": moment.isoformat(),
                "关闭时间": None,
                "阈值版本": snapshot["规则版本"],
                "告警级别": snapshot["告警级别"],
                "阈值快照": snapshot,
                "当前报告编号": None,
                "状态轨迹": [
                    f"{moment.isoformat()} 连续 {rule.get('持续周期')} 周期越阈，升级为{snapshot['告警级别']}告警（口径 {snapshot['规则版本']}，指标={metric}）"
                ],
            }
            self._table(EPISODE_MODULE).append(episode)
            runtime["active_episode_code"] = episode_code

        # 诊断报告随告警同步创建；已经结案的历史报告保持原样，绝不覆盖
        report = DiagnoseService().open_report(
            rule=rule,
            episode=episode,
            metric=metric,
            moment=moment,
            seq_start=DIAG_SEQ_START,
        )
        episode["当前报告编号"] = report["报告编号"]
        runtime["state"] = STATE_ALERTING
        return episode, report

    # ------------------------------------------------------------------ 恢复分支
    def _handle_recover(
        self,
        rule: dict[str, Any],
        metric: float,
        moment: datetime,
        silenced: bool,
        batch_id: str | None,
    ) -> dict[str, Any]:
        runtime = self._runtime(rule)

        if runtime["state"] == STATE_PENDING:
            # 还没升级就恢复正常：瞬时波动，直接回到正常，不产生告警/报告
            runtime["state"] = STATE_NORMAL
            return {"notified": False, "message": "越阈未达持续周期即恢复，按瞬时波动处理，不产生告警"}

        if runtime["state"] == STATE_NORMAL:
            return {"notified": False, "message": "指标正常，规则保持正常态"}

        # 告警中 / 待恢复：需要连续正常达到稳定周期才关闭
        stable_cycles = int(_to_number(rule.get("稳定周期", 2)))
        if runtime["state"] == STATE_ALERTING:
            runtime["state"] = STATE_RECOVERING
            episode = self._find_active_episode(runtime)
            if episode is not None:
                episode["状态"] = STATE_RECOVERING
                episode["状态轨迹"].append(f"{moment.isoformat()} 指标恢复正常（{metric}），进入 {stable_cycles} 周期稳定观察")

        if runtime["recover_streak"] < stable_cycles:
            return {
                "notified": False,
                "message": f"指标恢复 {runtime['recover_streak']}/{stable_cycles} 周期，稳定周期未满，告警暂不关闭",
            }

        episode = self._find_active_episode(runtime)
        if episode is None:
            runtime["state"] = STATE_NORMAL
            return {"notified": False, "message": "未找到活动告警实例，状态复位为正常"}
        self._close_episode(rule, episode, moment)
        return {"notified": False, "episode": episode["告警实例编号"], "message": "稳定周期已满，告警关闭并写入诊断结案结论"}

    def _close_episode(self, rule: dict[str, Any], episode: dict[str, Any], moment: datetime) -> None:
        runtime = self._runtime(rule)
        episode["状态"] = "已关闭"
        episode["关闭时间"] = moment.isoformat()
        snapshot = episode.get("阈值快照") or self._snapshot(rule)
        episode["状态轨迹"].append(
            f"{moment.isoformat()} 连续 {rule.get('稳定周期', 2)} 周期恢复正常，告警关闭（结案口径 {snapshot.get('规则版本')}）"
        )
        # 诊断报告同步结案，写入结案时间与当时阈值版本；已结案报告不会被二次写入
        DiagnoseService().close_active_report(
            episode=episode,
            moment=moment,
            conclusion=(
                f"指标连续 {rule.get('稳定周期', 2)} 个周期恢复正常，告警关闭。"
                f"结案口径以报告生成时的 {snapshot.get('规则版本')} 为准："
                f"级别={snapshot.get('告警级别')}，阈值={snapshot.get('比较运算符')}{snapshot.get('阈值数值')}，"
                f"持续周期={snapshot.get('持续周期')}，静默时段={snapshot.get('静默时段') or '无'}。"
            ),
        )
        runtime["state"] = STATE_NORMAL
        runtime["recover_streak"] = 0
        runtime["active_episode_code"] = None
        runtime["merged_count"] = 0
        runtime["was_silenced"] = False

    # ------------------------------------------------------------------ 通知
    def _notify(
        self,
        rule: dict[str, Any],
        episode: dict[str, Any],
        report: dict[str, Any],
        moment: datetime,
        batch_id: str | None,
    ) -> dict[str, Any]:
        runtime = self._runtime(rule)
        merged = runtime["merged_count"]
        seq = self._next_id(NOTIFY_MODULE, NOTIFY_SEQ_START)
        text = (
            f"{rule.get('告警编号')} 升级为{rule.get('告警级别')}告警：{rule.get('监控指标')} "
            f"连续 {rule.get('持续周期')} 周期越阈（最近指标={runtime['last_metric']}，口径 {self._version_of(rule)}），"
            f"诊断报告 {report['报告编号']}。"
        )
        if merged > 0:
            text += f" 静默窗口内合并 {merged} 次越阈告警，本次为静默结束后首次触发，一并汇总上报。"
        notification = {
            "id": seq,
            "通知编号": f"NOTIFY-{seq:04d}",
            "告警编号": rule.get("告警编号"),
            "告警实例编号": episode["告警实例编号"],
            "报告编号": report["报告编号"],
            "通知渠道": rule.get("通知渠道"),
            "告警级别": rule.get("告警级别"),
            "阈值版本": self._version_of(rule),
            "发送时间": moment.isoformat(),
            "采集批次": batch_id,
            "静默合并次数": merged,
            "通知内容": text,
        }
        self._table(NOTIFY_MODULE).append(notification)
        episode["状态轨迹"].append(
            f"{moment.isoformat()} 向{rule.get('通知渠道')}发送 1 条升级通知"
            + (f"（含静默合并 {merged} 次）" if merged else "")
        )
        # 合并次数随通知落账后清零，下一轮静默重新累计
        runtime["merged_count"] = 0
        runtime["was_silenced"] = False
        return notification

    # ------------------------------------------------------------------ 查询/校验
    def list_runtime(self) -> list[dict[str, Any]]:
        """返回每条规则的运行态（含最后越阈时间、原始指标、合并次数等）。"""
        result = []
        for rule in self._table(ALERT_MODULE):
            runtime = self._runtime(rule)
            result.append(
                {
                    "告警编号": rule.get("告警编号"),
                    "规则版本": self._version_of(rule),
                    "状态": runtime["state"],
                    "连续越阈周期": runtime["breach_streak"],
                    "连续恢复周期": runtime["recover_streak"],
                    "最后越阈时间": runtime["last_breach_at"],
                    "最后原始指标": runtime["last_metric"],
                    "静默合并次数": runtime["merged_count"],
                    "活动告警实例": runtime["active_episode_code"],
                }
            )
        return result

    def list_episodes(self, rule_ref: Any | None = None) -> list[dict[str, Any]]:
        episodes = self._table(EPISODE_MODULE)
        if rule_ref is None:
            return episodes
        rule = self._find_rule(rule_ref)
        if rule is None:
            return []
        return [ep for ep in episodes if ep.get("告警编号") == rule.get("告警编号")]

    def list_notifications(self, rule_ref: Any | None = None) -> list[dict[str, Any]]:
        notifications = self._table(NOTIFY_MODULE)
        if rule_ref is None:
            return notifications
        rule = self._find_rule(rule_ref)
        if rule is None:
            return []
        return [n for n in notifications if n.get("告警编号") == rule.get("告警编号")]

    def verify(self) -> dict[str, Any]:
        """告警与诊断相互印证：实例状态、报告状态、运行态三方必须对得上。"""
        issues: list[str] = []
        checks: list[str] = []
        diagnose_rows = self._table("diagnose")
        report_by_code = {row.get("报告编号"): row for row in diagnose_rows}

        for episode in self._table(EPISODE_MODULE):
            code = episode.get("当前报告编号")
            report = report_by_code.get(code) if code else None
            if report is None:
                issues.append(f"告警实例 {episode.get('告警实例编号')} 缺少关联诊断报告 {code}")
                continue
            if episode.get("状态") in (STATE_ALERTING, STATE_RECOVERING):
                if report.get("status") != "分析中":
                    issues.append(
                        f"告警实例 {episode.get('告警实例编号')} 未关闭，但报告 {code} 状态为 {report.get('status')}，应为分析中"
                    )
                else:
                    checks.append(f"{episode.get('告警实例编号')} 与 {code} 均处于处理中，状态一致")
            elif episode.get("状态") == "已关闭":
                if report.get("status") != "已结案":
                    issues.append(f"告警实例 {episode.get('告警实例编号')} 已关闭，但报告 {code} 未结案")
                elif not report.get("结案时间"):
                    issues.append(f"报告 {code} 已结案但缺少结案时间")
                else:
                    checks.append(
                        f"{episode.get('告警实例编号')} 已关闭，{code} 已结案（口径 {report.get('阈值版本')}），相互印证"
                    )
                if report.get("告警级别") != episode.get("告警级别"):
                    issues.append(f"报告 {code} 的告警级别与实例 {episode.get('告警实例编号')} 不一致")
                if report.get("阈值版本") != episode.get("阈值版本"):
                    issues.append(f"报告 {code} 的阈值版本与实例 {episode.get('告警实例编号')} 不一致")

        # 反向校验：引擎生成且仍在分析中的报告，必须挂在一个未关闭的实例上
        active_episode_reports = {ep.get("当前报告编号") for ep in self._table(EPISODE_MODULE)}
        for report in diagnose_rows:
            if (
                report.get("告警实例编号")
                and report.get("status") == "分析中"
                and report.get("报告编号") not in active_episode_reports
            ):
                issues.append(f"报告 {report.get('报告编号')} 处于分析中，但没有对应的活动告警实例")

        # 运行态一致性
        for rule in self._table(ALERT_MODULE):
            runtime = self._runtime(rule)
            if runtime["state"] in (STATE_ALERTING, STATE_RECOVERING) and not runtime["active_episode_code"]:
                issues.append(f"规则 {rule.get('告警编号')} 处于 {runtime['state']}，但缺少活动告警实例")

        return {"ok": not issues, "checks": checks, "issues": issues}

    def on_rule_updated(self, rule_ref: Any) -> None:
        """规则口径调整后调用：后续采集按新口径重新计数，活动告警不强行关闭。"""
        with self._lock:
            rule = self._find_rule(rule_ref)
            if rule is None:
                return
            runtime = self._runtime(rule)
            runtime["breach_streak"] = 0
            runtime["recover_streak"] = 0
            if runtime["state"] == STATE_PENDING:
                runtime["state"] = STATE_NORMAL
            elif runtime["state"] == STATE_RECOVERING:
                # 稳定周期按新配置重新计；实例保留，回到告警中等待重新观察恢复
                runtime["state"] = STATE_ALERTING
                episode = self._find_active_episode(runtime)
                if episode is not None:
                    episode["状态"] = STATE_ALERTING
                    episode["状态轨迹"].append(
                        f"{datetime.now().isoformat()} 规则口径调整为 {self._version_of(rule)}，持续/稳定计数按新口径重新累计"
                    )

    # ------------------------------------------------------------------ 内部辅助
    def _find_active_episode(self, runtime: dict[str, Any]) -> dict[str, Any] | None:
        code = runtime.get("active_episode_code")
        if not code:
            return None
        for episode in self._table(EPISODE_MODULE):
            if episode.get("告警实例编号") == code:
                return episode
        return None

    def _next_id(self, module: str, start: int = 1) -> int:
        rows = self._table(module)
        return max((int(row.get("id", 0)) for row in rows), default=start - 1) + 1


alert_engine = AlertEngine()
