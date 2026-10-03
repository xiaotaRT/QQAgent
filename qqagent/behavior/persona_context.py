#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""V19 PersonaContext 人格层上下文（从 src_m_reality.py 迁移）。

汇总各层现实世界状态，生成可注入人格/LLM 的环境片段。
"""
import os, sys, json, time, logging, threading
from qqagent.core import CONFIG, logger, context
from qqagent.tools.reality import RiskLevel, RikuDevice, DeviceType
from qqagent.core.commands import register_command

_PER_CFG = CONFIG.get("persona_config") or {}

class PersonaContext:
    """人格层上下文：汇总各层现实世界状态，生成可注入人格/LLM 的环境片段"""

    def __init__(self):
        self._proactive = []
        self._post_actions = []
        self._last_audit_len = 0

    # ---------- 环境摘要（人格层整合入口） ----------
    def build_environment_fragment(self):
        parts = []
        try:
            if environment_hub is not None:
                parts.append(environment_hub.build_awareness_summary())
        except Exception:
            pass
        try:
            ha = getattr(context, "_smart_home_agent", None)
            if ha is not None and ha.client.available:
                env = ha.query_env({})
                if env.get("ok"):
                    e = env.get("environment", {})
                    t = e.get("temperature", [])[:1]
                    h = e.get("humidity", [])[:1]
                    parts.append("家居环境：温度%s、湿度%s" %
                                 (t[0][1] if t else "?", h[0][1] if h else "?"))
        except Exception:
            pass
        try:
            vm = getattr(context, "_visual_memory", None)
            if vm is not None and vm._entries:
                last = vm._entries[-1]
                parts.append("最近视觉：%s（%s）" %
                             (last.get("summary", "")[:60], last.get("ts", "")))
        except Exception:
            pass
        return "\n".join(parts) if parts else "（现实世界感知暂无可报告内容）"

    # ---------- Trust → 权限调整（十九：Agent 根据 Trust 调整设备权限） ----------
    def trust_max_risk(self, trust):
        """信任分 → Agent 可自动执行的最大风险等级
        未知信任（None）→ 不额外限制（维持原策略，避免误伤既有行为）"""
        if trust is None:
            return None
        if trust >= 800:
            return RiskLevel.HIGH
        if trust >= 500:
            return RiskLevel.MEDIUM
        return RiskLevel.LOW

    # ---------- 主动行为触发（十九） ----------
    def check_proactive(self):
        """主动行为/观察/设备操作触发条件评估（只产生建议，不擅自执行）"""
        now = datetime.now().strftime("%H:%M")
        out = []
        try:
            ev = getattr(context, "_vis_events", None)
            if ev is not None:
                recent = ev.recent(5)
                for e in recent:
                    if e["type"] in ("frame_change", "anomaly"):
                        out.append("摄像头 %s 近期有%s，建议主动观察" %
                                   (e["camera"], e["type"]))
                        break
        except Exception:
            pass
        try:
            ha = getattr(context, "_smart_home_agent", None)
            if ha is not None:
                for a in ha._anomaly_log[-3:]:
                    out.append("家居异常：%s（%s）" % (a.get("rule"), a.get("entity")))
                    break
        except Exception:
            pass
        if now in ("07:00", "12:00", "18:00", "21:00"):
            out.append("时段 %s：例行环境观察（低风险只读）" % now)
        self._proactive = out[:5]
        return out

    # ---------- 行为后记忆与情绪（十九） ----------
    def observe_post_actions(self):
        """扫描网关审计日志增量：成功操作 → 记忆 + 情绪状态变化记录"""
        try:
            log = context.tool_gateway._log
            if len(log) <= self._last_audit_len:
                self._last_audit_len = len(log)
                return []
            new = log[self._last_audit_len:]
            self._last_audit_len = len(log)
            records = []
            for item in new:
                if item.get("status") != "success":
                    continue
                rec = "执行了%s→%s（%s）" % (item.get("device_id"),
                                             item.get("operation"), item.get("ts"))
                records.append(rec)
                try:
                    if event_memory is not None and \
                            hasattr(event_memory, "record_event"):
                        event_memory.record_event("global", "device_action", rec,
                                                  importance=0.4)
                except Exception:
                    pass
                try:
                    if unified_emotion is not None and \
                            hasattr(unified_emotion, "tick"):
                        unified_emotion.tick()
                except Exception:
                    pass
            self._post_actions = (self._post_actions + records)[-100:]
            return records
        except Exception:
            return []

    # ---------- 汇总给 Agent / 人格 ----------
    def to_prompt_fragment(self):
        frag = []
        frag.append("[现实世界感知]")
        frag.append(self.build_environment_fragment())
        if self._proactive:
            frag.append("[主动行为建议] " + "；".join(self._proactive))
        if self._post_actions[-3:]:
            frag.append("[最近自主行动] " + "；".join(self._post_actions[-3:]))
        return "\n".join(frag)

_persona = PersonaContext()
context.persona_thread_alive = False

def _persona_loop():
    while context.persona_thread_alive:
        try:
            _persona.check_proactive()
            _persona.observe_post_actions()
        except Exception as e:
            logger.warning("[V19] 人格层整合巡检失败: %s" % e)
        time.sleep(int(_PER_CFG.get("scan_interval", 120)))

def _persona_start():
    if context.persona_thread_alive:
        return
    context.persona_thread_alive = True
    threading.Thread(target=_persona_loop, daemon=True).start()

# ---------- 安全补齐（十八） ----------
# 麦克风：默认关闭自主访问（agent_deny_all + forbidden），仅 owner 确认可用
_mic_device = RikuDevice("microphone", DeviceType.WINDOWS, name="麦克风",
                         capabilities=["record_audio", "audio_status"],
                         op_risks={"record_audio": RiskLevel.HIGH,
                                   "audio_status": RiskLevel.LOW},
                         forbidden_ops=["record_audio"])
_mic_device.agent_deny_all = True

def _mic_record(params, device=None):
    return {"ok": False, "reason": "mic_not_implemented",
            "message": "麦克风采集需 Riku Node 端实现（默认禁止自主访问）"}

_mic_device.local_handlers = {
    "record_audio": _mic_record,
    "audio_status": lambda p, device=None: {"ok": True,
                                            "status": "默认关闭自主访问"},
}
if context.device_manager is not None:
    context.device_manager.register_device(_mic_device)

# 高功率电器限制（十八：高功率电器设置限制）
_HIGH_POWER_WATTS = int(_PER_CFG.get("high_power_limit_watts", 1500))
_HIGH_POWER_ENTITIES = _PER_CFG.get("high_power_entities") or [
    "switch.heater", "switch.oven", "switch.water_heater",
    "switch.air_conditioner",
]

def _check_high_power(entity_id):
    low = (entity_id or "").lower()
    for p in _HIGH_POWER_ENTITIES:
        if low.startswith(p) or p in low:
            return True
    return False

# Trust → 网关风险上限挂钩（十九：Agent 根据 Trust 调整设备权限）
try:
    context.tool_gateway.trust_risk_cap_fn = _persona.trust_max_risk
    context.tool_gateway.high_power_check_fn = _check_high_power
    logger.info("[V19] 人格层整合已启用：Trust→权限上限 + 高功率限制 + 麦克风默认关闭")
except Exception:
    pass

_persona_start()

@register_command("private", ["人格层状态", "现实感知状态"], perm_required=2,
                  owner_only=True)
def _cmd_persona_status(msg):
    lines = [
        "【人格层整合】",
        "环境感知:",
    ]
    lines.append("  " + _persona.build_environment_fragment().replace("\n", "\n  "))
    lines.append("主动行为建议: %s" %
                 ("；".join(_persona._proactive) if _persona._proactive else "无"))
    lines.append("最近自主行动: %s" %
                 ("；".join(_persona._post_actions[-3:]) if _persona._post_actions else "无"))
    lines.append("高功率限制: ≥%dW 实体默认受限" % _HIGH_POWER_WATTS)
    lines.append("麦克风: 默认关闭自主访问")
    return "\n".join(lines)

# ============================================================
