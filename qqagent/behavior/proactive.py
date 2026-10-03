#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""主动行为系统（ActionEngine/PersonaContext）（从 src_n_proactive.py 迁移）。"""
import os
import re
import json
import time
import random
import threading
import logging
import hashlib
import copy
import math
import queue
import signal
import sys
import shutil
import socket
import struct
import urllib.request
import websocket
import requests
from datetime import datetime, timedelta
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed

from qqagent.core import CONFIG, logger, dm
from qqagent.core import context
from qqagent.core.utils import _FakeLock, _now_str, is_owner
from qqagent.core.commands import register_command
from qqagent.tools.reality import RiskLevel, ToolGateway

if CONFIG["modules"].get("proactive_actions", True) and \
        context.tool_gateway is not None and context.device_manager is not None:
    _PAC_CFG = CONFIG.get("proactive_config") or {}
    _PAC_SCAN_INTERVAL = int(_PAC_CFG.get("scan_interval", 30))
    _PAC_MIN_CONFIDENCE = float(_PAC_CFG.get("min_confidence", 0.55))
    _PAC_COOLDOWN = int(_PAC_CFG.get("default_cooldown", 600))
    _PAC_SAME_TARGET_COOLDOWN = int(_PAC_CFG.get("same_target_cooldown", 600))
    _PAC_DEDUP_WINDOW = int(_PAC_CFG.get("event_dedup_window", 30))
    _PAC_HYSTERESIS_TICKS = int(_PAC_CFG.get("hysteresis_ticks", 2))
    _PAC_MAX_ACTIONS = int(_PAC_CFG.get("max_actions_per_window", 10))
    _PAC_WINDOW_SECONDS = int(_PAC_CFG.get("window_seconds", 600))
    _PAC_ASK_ON_HIGH_RISK = bool(_PAC_CFG.get("ask_on_high_risk", True))
    _PAC_NOTIFY_SPEAK = bool(_PAC_CFG.get("notify_on_speak", True))
    _PAC_VERIFY_TIMEOUT = int(_PAC_CFG.get("verify_timeout", 5))
    _PAC_VERIFY_RETRIES = int(_PAC_CFG.get("verify_retries", 3))
    _PAC_REPORT_MIN_GAP = 60          # 主动行为报告最小间隔（秒）
    _PAC_REPORT_MAX_DAILY = 100       # 主动行为报告每日上限
    _PAC_MAX_AUTO_RISK = {"LOW": RiskLevel.LOW, "MEDIUM": RiskLevel.MEDIUM,
                          "HIGH": RiskLevel.HIGH}.get(
        str(_PAC_CFG.get("max_auto_risk", "MEDIUM")).upper(), RiskLevel.MEDIUM)

    # 决策集（需求 2）
    _ACT = "ACT"; _SPEAK = "SPEAK"; _OBSERVE = "OBSERVE"
    _WAIT = "WAIT"; _IGNORE = "IGNORE"; _ASK = "ASK"

    class ProactiveAction:
        """结构化行动提案：Riku 决策的最小可审计单元"""

        _SEQ = [0]

        def __init__(self, action, target, params=None, device_id="",
                     reason="", confidence=0.5, risk=RiskLevel.MEDIUM,
                     permission="agent", cooldown=_PAC_COOLDOWN,
                     expires_at=None, source_event=None,
                     expected_result=None, verification=None):
            ProactiveAction._SEQ[0] += 1
            self.id = "PA%d%04d" % (int(time.time()), ProactiveAction._SEQ[0])
            self.action = action            # 网关操作名（或 speak/observe/ask）
            self.target = target            # 动作目标（如 light.living_1）
            self.params = params or {}
            self.device_id = device_id      # 执行设备
            self.reason = reason
            self.confidence = float(confidence)
            self.risk = risk                # RiskLevel 值
            self.permission = permission    # agent=自主（仍过安全链）
            self.cooldown = int(cooldown)
            self.expires_at = expires_at or (time.time() + int(cooldown))
            self.source_event = source_event or {}
            self.expected_result = expected_result or {}
            self.verification = verification or {}
            self.status = "proposed"
            self.decision = None
            self.outcome = None
            self.created_ts = time.time()
            self.updated_ts = self.created_ts

        def touch(self):
            """状态流转时更新时间戳"""
            self.updated_ts = time.time()

        def to_dict(self):
            return {"id": self.id, "action": self.action, "target": self.target,
                    "params": self.params, "device_id": self.device_id,
                    "reason": self.reason, "confidence": self.confidence,
                    "risk": self.risk, "permission": self.permission,
                    "cooldown": self.cooldown, "expires_at": self.expires_at,
                    "source_event": self.source_event,
                    "expected_result": self.expected_result,
                    "verification": self.verification, "status": self.status,
                    "decision": self.decision, "outcome": self.outcome,
                    "created_ts": self.created_ts, "updated_ts": self.updated_ts}

    class ProactiveEventBus:
        """统一事件总线：事件标准化 + 去重（需求 4 去重）"""

        def __init__(self, dedup_window=30):
            self.dedup_window = dedup_window
            self._queue = []
            self._seen = {}
            self._events = []

        def emit(self, source, etype, device_id="", entity_id="",
                 state=None, prev_state=None, payload=None):
            dedup_key = "%s|%s|%s|%s" % (source, etype,
                                         entity_id or device_id, state)
            now = time.time()
            if now - self._seen.get(dedup_key, 0) < self.dedup_window:
                return False          # 去重：同源同键在窗口内只进一次
            self._seen[dedup_key] = now
            if len(self._seen) > 500:
                self._seen = dict(list(self._seen.items())[-300:])
            ev = {"ts": _now_str(), "time": now, "source": source,
                  "type": etype, "device_id": device_id or "",
                  "entity_id": entity_id or "", "state": state,
                  "prev_state": prev_state, "payload": payload or {},
                  "dedup_key": dedup_key}
            self._queue.append(ev)
            self._events.append(ev)
            self._queue = self._queue[-100:]
            self._events = self._events[-300:]
            return True

        def drain(self):
            q, self._queue = self._queue, []
            return q

        def recent(self, limit=20):
            return self._events[-limit:]

        def status(self):
            return {"queued": len(self._queue),
                    "total_events": len(self._events)}

    _proactive_bus = ProactiveEventBus(dedup_window=_PAC_DEDUP_WINDOW)

    class ActionHistory:
        """行为历史：决策+验证+结果，供习惯/偏好演化（非 if/else 硬编码）"""

        def __init__(self):
            self.entries = []
            self.stats = {}
            self._load()

        def _load(self):
            try:
                data = dm.load("proactive_actions_data", {})
                self.entries = data.get("history", [])
                self.stats = data.get("stats", {})
            except Exception:
                pass

        def append(self, proposal, decision, outcome, note=""):
            self.entries.append({
                "ts": _now_str(), "time": time.time(),
                "decision": decision, "action": proposal.action,
                "target": proposal.target, "device_id": proposal.device_id,
                "risk": proposal.risk, "confidence": proposal.confidence,
                "outcome": outcome, "note": note,
            })
            self.entries = self.entries[-2000:]
            key = "%s|%s" % (proposal.action, proposal.target)
            st = self.stats.setdefault(key, {"tried": 0, "ok": 0,
                                             "verified": 0, "failed": 0})
            st["tried"] += 1
            if outcome in ("VERIFIED", "SUCCESS"):
                st["ok"] += 1
            if outcome == "VERIFIED":
                st["verified"] += 1
            if outcome in ("FAILED", "NOT_VERIFIED", "TIMED_OUT"):
                st["failed"] += 1
            self._dirty = True

        def success_ratio(self, action, target):
            st = self.stats.get("%s|%s" % (action, target))
            if not st or st["tried"] == 0:
                return None
            return st["ok"] / st["tried"]

        def recent(self, limit=20):
            return self.entries[-limit:][::-1]

        def flush(self):
            try:
                dm.save("proactive_actions_data",
                        {"history": self.entries[-2000:], "stats": self.stats})
                dm.flush_all()
                self._dirty = False
            except Exception as e:
                logger.warning("[V20] 主动行为历史落盘失败: %s" % e)

    class BehaviorLearner:
        """行为偏好 / 自主学习器（V20+）

        信号源：
         - 显式反馈：owner 命令「行为偏好 <域> 喜欢/不喜欢」→ 行为键权重
         - 隐式反馈：Riku 动作后目标状态被外部改回（用户干预）→ 强负信号
         - 结果反馈：VERIFIED/SUCCESS +，FAILED/NOT_VERIFIED/TIMED_OUT -
        权重演化：指数时间衰减（旧反馈影响力下降）、软 clamp、可被新反馈翻转。
        这是"经验"而非规则：不写死任何禁止/偏好，只做统计偏移。
        """

        def __init__(self):
            self._prefs = {}        # key -> {"w": weight, "n": count, "ts": ts}
            self._lock = threading.RLock()
            self._load()

        def _load(self):
            try:
                data = dm.load("proactive_actions_data", {}).get("preferences") or {}
                self._prefs = {k: dict(v) for k, v in data.items()}
            except Exception:
                pass

        @staticmethod
        def _key(domain, action):
            return "%s:%s" % (domain or "?", action or "?")

        @staticmethod
        def _decay(pref, now):
            days = (now - pref.get("ts", now)) / 86400.0
            if days > 0:
                pref["w"] = pref.get("w", 0.0) * (0.96 ** min(days, 30.0))
            pref["ts"] = now

        def apply_feedback(self, domain, action, delta, source="owner"):
            key = self._key(domain, action)
            with self._lock:
                pref = self._prefs.setdefault(key, {"w": 0.0, "n": 0, "ts": time.time()})
                self._decay(pref, time.time())
                pref["w"] = max(-0.5, min(0.5, pref.get("w", 0.0) + float(delta)))
                pref["n"] = pref.get("n", 0) + 1
                pref["src"] = source
            return pref["w"]

        def observe_outcome(self, domain, action, outcome):
            """结果信号：成功 + / 失败 -"""
            if outcome in ("VERIFIED", "SUCCESS"):
                return self.apply_feedback(domain, action, 0.1, "result")
            if outcome in ("FAILED", "NOT_VERIFIED", "TIMED_OUT"):
                return self.apply_feedback(domain, action, -0.15, "result")
            return 0.0

        def observe_user_revert(self, domain, action):
            """隐式信号：用户把 Riku 设置的状态改回 → 强负反馈（自主学习）"""
            return self.apply_feedback(domain, action, -0.4, "user_revert")

        def bias(self, domain, action):
            with self._lock:
                pref = self._prefs.get(self._key(domain, action))
                if pref is None:
                    return 0.0
                self._decay(pref, time.time())
                return max(-0.5, min(0.5, pref.get("w", 0.0)))

        def summary(self, limit=10):
            with self._lock:
                rows = sorted(self._prefs.items(),
                              key=lambda x: x[1].get("w", 0.0), reverse=True)
                return [{"key": k, "w": round(v.get("w", 0.0), 3),
                         "n": v.get("n", 0)} for k, v in rows[:limit]]

        def to_dict(self):
            with self._lock:
                return {k: dict(v) for k, v in self._prefs.items()}

        def reset(self):
            """清空全部偏好（测试隔离 / owner 重置）"""
            with self._lock:
                self._prefs = {}

    class ActionEngine:
        """主动行为引擎（Riku Core 决策层 + 执行 + 验证 + 记忆闭环）

        决策集：ACT / SPEAK / OBSERVE / WAIT / IGNORE / ASK
        安全链：本引擎前置检查（紧急停止/风险上限/频率/冷却）
                → ToolGateway.execute（完整安全链，唯一执行通道）
        人格与安全分离：decide() 是"想不想做"，_safety() 是"能不能做"
        """

        # ---------- 能力域模板注册表（模块赋予能力，不是规则） ----------
        _DOMAIN_SPECS = {}

        def __init__(self, cfg):
            self.cfg = cfg
            self.max_auto_risk = _PAC_MAX_AUTO_RISK
            self.min_confidence = _PAC_MIN_CONFIDENCE
            self.cooldowns = {}        # target -> until(ts)
            self._suppressed = {}      # target -> expected_state（成功抑制）
            self._probe = {}           # target -> (state, stable_ticks)  Hysteresis
            self._freq = []            # 行动频率窗口（ts 列表）
            self._handled = set()      # 本次 tick 已处理事件 id
            self.decisions = []        # 最近决策记录（含结果）
            self.history = ActionHistory()
            self._alive = False
            self._lock = threading.RLock()
            self._stats = {"decided": 0, "acted": 0, "verified": 0,
                           "not_verified": 0, "failed": 0, "ignored": 0,
                           "blocked": 0, "hysteresis": 0, "expired": 0}
            self._seen_anomalies = {}      # (entity|rule) -> ts 长期去重水位
            self._snapshot_ticks = 0       # 环境快照降频计数器
            self._learner = BehaviorLearner()
            self._load_runtime()
            self._register_builtin_domains()
            self._register_event_feeds()

        # ---------- 内置能力域（示例：领域语义，非场景 if/else） ----------
        def _register_builtin_domains(self):
            # 灯光域：可自动开关，风险按实体域表
            self.register_domain("ha_light", device_id="homeassistant",
                                 action="control_device", risk=RiskLevel.MEDIUM,
                                 expected="on", verify_op="query_device",
                                 cooldown=_PAC_SAME_TARGET_COOLDOWN,
                                 controllable=lambda params: True)
            # 门锁域：高风险，不自动执行（OBSERVE/ASK）
            self.register_domain("ha_lock", device_id="homeassistant",
                                 action="control_device", risk=RiskLevel.CRITICAL,
                                 expected="locked", verify_op="query_device",
                                 cooldown=_PAC_SAME_TARGET_COOLDOWN,
                                 auto=False)
            # 视觉域：默认 OBSERVE（只读），异常才 SPEAK
            self.register_domain("vision", device_id="vision-gateway",
                                 action="visual_recent", risk=RiskLevel.LOW,
                                 expected=None, verify_op=None,
                                 cooldown=300, auto=True)

        def register_domain(self, key, device_id, action, risk,
                            expected=None, verify_op=None,
                            cooldown=_PAC_SAME_TARGET_COOLDOWN,
                            auto=True, controllable=None, params_builder=None):
            """注册一个能力域：事件实体域 → 候选动作模板"""
            self._DOMAIN_SPECS[key] = {
                "device_id": device_id, "action": action, "risk": risk,
                "expected": expected, "verify_op": verify_op,
                "cooldown": cooldown, "auto": auto,
                "controllable": controllable,
                "params_builder": params_builder,
            }

        def domain(self, key):
            return self._DOMAIN_SPECS.get(key)

        # ---------- 事件馈送：现有事件源 → 标准事件 ----------
        def _register_event_feeds(self):
            try:
                if context.get('_vis_events') is not None:
                    context.get('_vis_events').add_listener(self._feed_vision)
            except Exception:
                pass

        def _feed_vision(self, ev):
            try:
                _proactive_bus.emit("vision", ev.get("type", "frame_change"),
                                    device_id="vision-gateway",
                                    entity_id=ev.get("camera", ""),
                                    state=ev.get("type"),
                                    payload={"priority": ev.get("priority", 1),
                                             "camera": ev.get("camera", "")})
            except Exception:
                pass


        def ingest_event(self, source, etype, device_id="", entity_id="",
                         state=None, prev_state=None, payload=None):
            """外部/测试注入标准事件"""
            return _proactive_bus.emit(source, etype, device_id=device_id,
                                       entity_id=entity_id, state=state,
                                       prev_state=prev_state, payload=payload)

        # ---------- 轮询事件源增量（HA 异常、环境快照 diff） ----------
        def _poll_external(self):
            # HA 异常日志增量（长期去重水位：同实体同规则 TTL 内只处理一次）
            try:
                ha = getattr(context, "_smart_home_agent", None)
                if ha is not None:
                    anomalies = getattr(ha, "_anomaly_log", [])[-5:]
                    for a in anomalies:
                        rule = a.get("rule", "")
                        ent = a.get("entity", "")
                        if not self._anomaly_new(ent, rule):
                            continue
                        _proactive_bus.emit(
                            "home", "anomaly", device_id="homeassistant",
                            entity_id=ent, state=a.get("state", ""),
                            payload={"rule": rule, "ts": a.get("ts", "")})
            except Exception:
                pass
            # 环境摘要定期观察（每 10 个 tick 一次，避免噪音刷屏）
            self._snapshot_ticks += 1
            if self._snapshot_ticks < 10:
                return
            self._snapshot_ticks = 0
            try:
                if context.environment_hub is not None:
                    _proactive_bus.emit("environment", "snapshot",
                                        device_id="local-host",
                                        entity_id="environment",
                                        state="observed",
                                        payload={"summary":
                                                 context.environment_hub.build_awareness_summary()})
            except Exception:
                pass

        # ---------- Riku 决策：ACT/SPEAK/OBSERVE/WAIT/IGNORE/ASK ----------
        def decide(self, ev):
            """决策入口：上下文 → 候选 → 裁决（返回 decision, proposal）"""
            key = ev.get("dedup_key")
            if key in self._handled:
                return _IGNORE, None
            self._handled.add(key)
            self._stats["decided"] += 1

            # Hysteresis：目标状态未变化/未稳定 → 不触发（防抖动循环）
            if not self._hysteresis_ok(ev):
                return _IGNORE, None

            candidates = self._apply_guards(self._candidates_for(ev))
            if not candidates:
                self._stats["ignored"] += 1
                return _IGNORE, None

            if not self._freq_ok():
                self._stats["ignored"] += 1
                return _WAIT, None

            cand = max(candidates, key=lambda c: c.confidence)
            self._apply_history_bias(cand)
            cand.confidence = max(0.2, min(0.9,
                cand.confidence + self._context_bias(cand)))
            _dk = self._domain_key_for_cand(cand)
            cand.confidence = max(0.2, min(0.9,
                cand.confidence + self._learner.bias(_dk, cand.action)))

            # 置信度门槛：低置信不执行，只看/忽略
            if cand.confidence < self.min_confidence:
                if cand.risk <= RiskLevel.MEDIUM:
                    obs = self._make_observe(cand, ev)
                    return _OBSERVE, obs
                return _IGNORE, None

            # 风险上限：超出自主上限 → ASK（不自动执行）
            if cand.risk > self.max_auto_risk:
                if _PAC_ASK_ON_HIGH_RISK:
                    ask = self._make_ask(cand, ev)
                    return _ASK, ask
                self._stats["ignored"] += 1
                return _IGNORE, None

            # 安全前置：紧急停止（能力域级）
            if context.tool_gateway._emergency_stop:
                self._stats["blocked"] += 1
                return _IGNORE, None

            # 非 auto 域（如门锁）：只 ASK/OBSERVE
            spec = self._DOMAIN_SPECS.get(self._domain_key_for(ev))
            if spec and not spec.get("auto", True):
                ask = self._make_ask(cand, ev)
                return _ASK, ask

            cand.decision = _ACT
            cand.status = "proposed"
            return _ACT, cand

        # ---------- 候选构建（能力域 → 提案） ----------
        def _domain_key_for(self, ev):
            etype = (ev.get("entity_id") or ev.get("type") or "").lower()
            if etype.startswith("light") or "light" in etype:
                return "ha_light"
            if etype.startswith("lock") or "lock" in etype:
                return "ha_lock"
            if ev.get("source") == "vision":
                return "vision"
            return None

        def _candidates_for(self, ev):
            dk = self._domain_key_for(ev)
            spec = self._DOMAIN_SPECS.get(dk)
            if not spec:
                return []

            # 实体控制域：由状态推断期望动作
            return self._control_candidates(spec, ev, dk)

        def _control_candidates(self, spec, ev, dk):
            state = (ev.get("state") or "").lower()
            eid = ev.get("entity_id", "")
            controllable = spec.get("controllable")
            if controllable is not None and not controllable(ev.get("payload", {})):
                return []
            # 门锁域（ha_lock）：产生候选但 auto=False → 由 decide 判定为 ASK
            if dk == "ha_lock":
                if state in ("unlocked", "open", "off"):
                    p = {"entity_id": eid, "action": "lock"}
                    ex = {"state": "locked"}
                    _lp = ProactiveAction(
                        spec["action"], eid, params=p, device_id=spec["device_id"],
                        reason="门锁状态%s，建议锁定（风险高，需 owner 确认）" % state,
                        confidence=0.7, risk=spec["risk"], cooldown=spec["cooldown"],
                        source_event=ev, expected_result=ex,
                        verification={"device_id": spec["device_id"],
                                      "operation": spec["verify_op"],
                                      "params": {"entity_id": eid}, "expected": ex})
                    return [_lp]
                return []
            # 语义：暗/关 → 开；亮/开 → 关（仅当有明确期望状态时给 ACT 候选）
            if state in ("dark", "off", "unknown") and dk == "ha_light":
                p = {"entity_id": eid, "action": "turn_on"}
                ex = {"state": "on"}
            elif state in ("bright", "on") and dk == "ha_light":
                p = {"entity_id": eid, "action": "turn_off"}
                ex = {"state": "off"}
            else:
                return []
            p = ProactiveAction(spec["action"], eid, params=p,
                                device_id=spec["device_id"],
                                reason="状态%s→%s（%s 域自动候选）" % (state, ex["state"], dk),
                                confidence=0.62, risk=spec["risk"],
                                cooldown=spec["cooldown"],
                                source_event=ev,
                                expected_result=ex,
                                verification={"device_id": spec["device_id"],
                                              "operation": spec["verify_op"],
                                              "params": {"entity_id": eid},
                                              "expected": ex})
            return [p]

        def _proposal_from_spec(self, spec, ev, dk):
            if dk == "vision":
                return ProactiveAction(
                    "visual_recent", ev.get("entity_id") or "vision-gateway",
                    params={"limit": 5}, device_id="vision-gateway",
                    reason="视觉事件（%s）先观察再决定" % ev.get("state"),
                    confidence=0.5, risk=RiskLevel.LOW, cooldown=300,
                    source_event=ev, expected_result={}, verification={})
            return None

        def _make_observe(self, cand, ev):
            obs = ProactiveAction("observe", cand.target, params={},
                                  device_id=cand.device_id,
                                  reason="置信不足(%0.2f)，先观察" % cand.confidence,
                                  confidence=cand.confidence, risk=RiskLevel.LOW,
                                  cooldown=cand.cooldown, source_event=ev,
                                  expected_result={}, verification={})
            obs.decision = _OBSERVE
            return obs

        def _make_ask(self, cand, ev):
            ask = ProactiveAction("ask", cand.target, params=cand.params,
                                  device_id=cand.device_id,
                                  reason="风险(%s)超出自主上限，需 owner 决定" %
                                  RiskLevel.name(cand.risk),
                                  confidence=cand.confidence, risk=cand.risk,
                                  cooldown=cand.cooldown, source_event=ev,
                                  expected_result=cand.expected_result,
                                  verification=cand.verification)
            ask.decision = _ASK
            return ask

        # ---------- 防循环机制 ----------
        def _cooldown_active(self, target):
            return time.time() < self.cooldowns.get(target, 0)

        def _freq_ok(self):
            now = time.time()
            self._freq = [t for t in self._freq if now - t < _PAC_WINDOW_SECONDS]
            return len(self._freq) < _PAC_MAX_ACTIONS

        def _state_matches(self, target, expected):
            """目标当前状态是否已等于期望（成功抑制判据）"""
            try:
                probe = self._probe.get(target)
                if probe and isinstance(expected, dict):
                    for k, v in expected.items():
                        if str(probe[0]).lower() == str(v).lower():
                            return True
                return False
            except Exception:
                return False

        def _apply_history_bias(self, cand):
            ratio = self.history.success_ratio(cand.action, cand.target)
            if ratio is None:
                return
            if ratio >= 0.7:
                cand.confidence = min(0.85, cand.confidence + 0.15)
            elif ratio <= 0.3:
                cand.confidence = max(0.2, cand.confidence - 0.2)

        def _domain_key_for_cand(self, cand):
            """按目标实体推断能力域（实体优先，避免 device_id/action 撞名错配）"""
            t = (cand.target or "").lower()
            if t.startswith("light") or "light" in t:
                return "ha_light"
            if t.startswith("lock") or "lock" in t:
                return "ha_lock"
            if "vision" in t or "camera" in t:
                return "vision"
            for k, spec in self._DOMAIN_SPECS.items():
                if spec.get("device_id") == cand.device_id and \
                        spec.get("action") == cand.action:
                    return k
            return None

        # ---------- 执行（唯一通道 ToolGateway） ----------
        def _execute(self, proposal):
            """经安全链执行；SPEAK/OBSERVE/ASK 不走设备网关"""
            decision = proposal.decision or _ACT
            if decision == _SPEAK:
                return {"ok": True, "data": {"mode": "speak"}}
            if decision == _OBSERVE:
                if proposal.action == "observe":
                    # 观察：只读网关操作或记录
                    r = {"ok": True, "data": {"mode": "observe",
                                               "context": _build_cognition_snapshot()}}
                    return r
                return context.tool_gateway.execute(proposal.device_id, proposal.action,
                                            proposal.params, actor="agent")
            if decision == _ASK:
                return {"ok": False, "reason": "ask_owner",
                        "data": {"mode": "ask", "proposal_id": proposal.id}}
            # ACT：走统一网关（内部含紧急停止/风险/硬禁止/黑名单/权限/Trust/高功率/确认/审计）
            return context.tool_gateway.execute(proposal.device_id, proposal.action,
                                        proposal.params, actor="agent")

        # ---------- 结果验证：不轻信返回值，重读状态 ----------
        def _verify(self, proposal, exec_result):
            ver = proposal.verification or {}
            if not ver.get("operation"):
                # 无可验证操作：网关 ok 即视为已执行（结果分级 SUCCESS）
                return "SUCCESS" if exec_result.get("ok") else "FAILED"
            verify_device = ver.get("device_id", proposal.device_id)
            verify_op = ver.get("operation")
            verify_params = ver.get("params", {})
            expected = ver.get("expected", {})
            deadline = time.time() + _PAC_VERIFY_TIMEOUT
            for _ in range(_PAC_VERIFY_RETRIES):
                vr = context.tool_gateway.execute(verify_device, verify_op,
                                          verify_params, actor="agent")
                data = (vr.get("result") or {}).get("data", {})
                if vr.get("ok") and data.get("ok") is False:
                    time.sleep(0.5)
                    continue
                ok = True
                for k, v in expected.items():
                    got = data.get(k) if isinstance(data, dict) else None
                    if got is None:
                        ok = False            # 字段缺失 = 无法确认（不误判成功）
                    elif str(got).lower() != str(v).lower():
                        ok = False
                if ok:
                    return "VERIFIED"
                if time.time() > deadline:
                    return "TIMED_OUT"
                time.sleep(1)
            return "NOT_VERIFIED"

        # ---------- 记忆 / 情绪 / 世界状态 ----------
        def _record(self, proposal, decision, outcome, note=""):
            proposal.touch()
            try:
                _dk = self._domain_key_for_cand(proposal)
                if _dk:
                    self._learner.observe_outcome(_dk, proposal.action, outcome)
            except Exception:
                pass
            self.history.append(proposal, decision, outcome, note)
            self.decisions.append({
                "ts": _now_str(), "time": time.time(),
                "decision": decision, "action": proposal.action,
                "target": proposal.target, "device_id": proposal.device_id,
                "risk": proposal.risk, "confidence": proposal.confidence,
                "outcome": outcome, "note": note,
            })
            self.decisions = self.decisions[-200:]
            try:
                if event_memory is not None:
                    event_memory.record_event(
                        "global", "proactive_action",
                        "主动行为[%s] %s→%s 结果:%s %s" %
                        (decision, proposal.device_id, proposal.action,
                         outcome, note or ""),
                        {"score": 5 if outcome == "VERIFIED" else 2,
                         "reasons": ["proactive"]}, "proactive_action")
            except Exception:
                pass
            try:
                if unified_emotion is not None and outcome == "VERIFIED":
                    unified_emotion.trigger_emotion("满足", intensity=8,
                                                    reason="主动行为成功")
            except Exception:
                pass
            try:
                if context.environment_hub is not None:
                    context.environment_hub.note_event(
                        "主动行为 %s：%s（%s）" % (decision, proposal.action, outcome))
            except Exception:
                pass
            # 成功抑制 + 冷却 + 状态探针回灌（防"动作→事件→再动作"循环）
            if outcome in ("VERIFIED", "SUCCESS"):
                self._suppressed[proposal.target] = proposal.expected_result
                self.cooldowns[proposal.target] = time.time() + proposal.cooldown
                if isinstance(proposal.expected_result, dict):
                    for _v in proposal.expected_result.values():
                        self._probe[proposal.target] = (str(_v), 1)
                        break
            elif outcome in ("FAILED", "NOT_VERIFIED", "TIMED_OUT"):
                self.cooldowns[proposal.target] = time.time() + min(
                    proposal.cooldown, 120)
            elif decision == _ASK:
                self.cooldowns[proposal.target] = time.time() + 120   # ASK 冷却防轰炸
            elif decision == _SPEAK:
                self.cooldowns[proposal.target] = time.time() + 60    # SPEAK 节流

        # ---------- 主循环：事件→决策→执行→验证→记录 ----------
        def _report_to_owner(self, text, force=False):
            """统一主动出口：把主动行为结果/询问报告给主人（QQ 真发）

            与 V6 主动消息链接：走同一 send_private 通道；
            独立频控账本 proactive_report_log，防止与 V6 问候叠加刷屏。
            """
            try:
                if not (_PAC_CFG.get("report_to_owner", True)):
                    return False
                _ws_ref = getattr(context, "context.wsm", None)
                _ws = getattr(_ws_ref, "_ws", None) if _ws_ref is not None else None
                if _ws is None or not getattr(_ws, "sock", None) or \
                        not getattr(_ws.sock, "connected", False):
                    return False                    # 未连接 QQ：仅靠日志
                _owner = str(CONFIG.get("owner_qq", ""))
                if not _owner:
                    return False
                _log = dm.load("proactive_report_log", {})
                _now = time.time()
                _today = time.strftime("%Y-%m-%d")
                _gap = float(_PAC_CFG.get("report_min_gap", _PAC_REPORT_MIN_GAP))
                _max_d = int(_PAC_CFG.get("report_max_daily", _PAC_REPORT_MAX_DAILY))
                if not force and _now - _log.get("last_send_ts", 0) < _gap:
                    return False                    # 频控：防刷屏
                if _log.get("daily", {}).get(_today, 0) >= _max_d:
                    return False
                send_private(_ws, int(_owner), "[主动报告] %s" % str(text)[:120])
                _log["last_send_ts"] = _now
                _log.setdefault("daily", {})[_today] = \
                    _log.get("daily", {}).get(_today, 0) + 1
                dm.save("proactive_report_log", _log)
                dm.mark_dirty("proactive_report_log")
                return True
            except Exception:
                return False

        def _process_event(self, ev):
            self._observe_external_change(ev)      # 自主学习：先读用户干预信号
            decision, proposal = self.decide(ev)
            if proposal is None:
                if ev.get("source") not in ("environment",):
                    self._record_decision_only(decision, ev)
                return decision, None
            if decision == _IGNORE:
                if ev.get("source") not in ("environment",):
                    self._record_decision_only(decision, ev)
                return decision, proposal

            proposal.decision = decision
            proposal.status = "approved"
            if decision == _SPEAK:
                # SPEAK：表达/通知（人格想说话，不走设备网关）
                text = proposal.reason
                if _PAC_NOTIFY_SPEAK:
                    try:
                        context.tool_gateway._notify("[主动行为·表达] %s" % text)
                    except Exception:
                        pass
                self._report_to_owner("表达：" + text)   # 链接 V6 消息通道
                proposal.status = "executed"
                proposal.outcome = "SPOKEN"
                self._record(proposal, _SPEAK, "SPOKEN", text)
                return decision, proposal

            if decision == _ASK:
                proposal.status = "pending_owner"
                proposal.outcome = "ASKED"
                try:
                    context.tool_gateway._notify("[主动行为·询问] %s（风险：%s）" %
                                         (proposal.reason,
                                          RiskLevel.name(proposal.risk)))
                except Exception:
                    pass
                self._report_to_owner(
                    "需要你决定（风险：%s）：%s" %
                    (RiskLevel.name(proposal.risk), proposal.reason),
                    force=True)                        # ASK 必须送达
                self._record(proposal, _ASK, "ASKED", proposal.reason)
                return decision, proposal

            if decision == _OBSERVE:
                proposal.status = "executed"
                r = self._execute(proposal)
                proposal.outcome = "OBSERVED"
                self._record(proposal, _OBSERVE, "OBSERVED",
                             str(r.get("reason", "observed"))[:80])
                return decision, proposal

            if decision == _WAIT:
                proposal.status = "waiting"
                proposal.outcome = "WAITING"
                return decision, proposal

            # ACT：执行 → 验证
            proposal.status = "executed"
            proposal.touch()
            self._freq.append(time.time())
            self._stats["acted"] += 1
            r = self._execute(proposal)
            if not r.get("ok"):
                reason = r.get("reason", "blocked")
                if reason in ("handler_error", "node_error", "task_failed"):
                    proposal.outcome = "FAILED"
                elif reason == "task_timeout":
                    proposal.outcome = "TIMED_OUT"
                elif reason in ("expired", "confirmation_unavailable"):
                    proposal.outcome = "CONFIRMATION_EXPIRED"
                else:
                    proposal.outcome = "BLOCKED"
                self._stats["blocked"] += 1
                self._report_to_owner("未能执行（%s）：%s" %
                                      (proposal.outcome, proposal.reason))
                self._record(proposal, _ACT, proposal.outcome, reason)
                return decision, proposal
            outcome = self._verify(proposal, r)
            proposal.outcome = outcome
            if outcome == "VERIFIED":
                self._stats["verified"] += 1
                self._report_to_owner("已完成（已验证）：%s" % proposal.reason)
            elif outcome == "NOT_VERIFIED":
                self._stats["not_verified"] += 1
                self._report_to_owner("已执行但验证未确认：%s" % proposal.reason)
            else:
                self._stats["failed"] += 1
                self._report_to_owner("执行结果异常（%s）：%s" %
                                      (outcome, proposal.reason))
            self._record(proposal, _ACT, outcome,
                         "网关 ok 但验证%s" % outcome if outcome == "NOT_VERIFIED" else "")
            return decision, proposal

        def _record_decision_only(self, decision, ev):
            self.decisions.append({
                "ts": _now_str(), "time": time.time(),
                "decision": decision, "action": "-",
                "target": ev.get("entity_id") or ev.get("type"),
                "device_id": ev.get("device_id", ""),
                "risk": 0, "confidence": 0.0, "outcome": "NONE",
                "note": "事件 %s/%s 决策为 %s" %
                        (ev.get("source"), ev.get("type"), decision),
            })
            self.decisions = self.decisions[-200:]

        # ---------- 状态探针（Hysteresis：变化需稳定若干 tick） ----------
        def probe_state(self, target, state):
            prev = self._probe.get(target)
            if prev and prev[0] == state:
                stable = prev[1] + 1
            else:
                stable = 1
            self._probe[target] = (state, stable)
            return stable

        def _note_state(self, target, state):
            """更新状态探针，返回是否已稳定（Hysteresis 事件流接入）"""
            prev = self._probe.get(target)
            if prev is None:
                self._probe[target] = (state, 1)
                return True            # 首次状态：允许处理（冷启动）
            if prev[0] != state:
                self._probe[target] = (state, 1)
                return _PAC_HYSTERESIS_TICKS <= 1   # 状态刚变化：需稳定若干 tick
            self._probe[target] = (state, prev[1] + 1)
            return prev[1] >= _PAC_HYSTERESIS_TICKS

        def _hysteresis_ok(self, ev):
            """事件级状态变化检测：无变化/未稳定 → 不触发（防抖动循环）"""
            target = ev.get("entity_id") or ev.get("device_id")
            state = ev.get("state")
            if not target or state is None:
                return True
            ok = self._note_state(target, state)
            if not ok:
                self._stats["hysteresis"] += 1
            return ok

        def _apply_guards(self, candidates):
            """候选守卫：冷却 / 成功抑制 / 提案过期（返回过滤后列表）"""
            kept = []
            for cand in candidates:
                tgt = cand.target
                if self._cooldown_active(tgt):
                    continue
                if self._suppressed.get(tgt) is not None and \
                        self._state_matches(tgt, self._suppressed[tgt]):
                    continue
                if time.time() > cand.expires_at:
                    self._stats["expired"] += 1
                    continue
                kept.append(cand)
            return kept

        def _anomaly_new(self, entity_id, rule, ttl=3600):
            """HA 异常长期去重水位：同实体同规则 TTL 内只处理一次"""
            key = "%s|%s" % (entity_id or "", rule or "")
            now = time.time()
            self._seen_anomalies = {k: v for k, v in
                                    self._seen_anomalies.items()
                                    if now - v < ttl}
            if key in self._seen_anomalies:
                return False
            self._seen_anomalies[key] = now
            return True

        def _context_bias(self, cand):
            """轻量上下文评分：时间/设备缓存/关系（±小幅置信调整）"""
            delta = 0.0
            try:
                hour = datetime.now().hour
                if cand.action in ("control_device", "set_state") and \
                        (hour >= 23 or hour < 6):
                    delta += 0.05       # 深夜自动控制更合理
            except Exception:
                pass
            try:
                ha = getattr(context, "_smart_home_agent", None)
                if ha is not None:
                    ent = getattr(ha, "_state_cache", {}).get(cand.target)
                    if ent and isinstance(cand.expected_result, dict):
                        want = str(next(iter(cand.expected_result.values()), ""))
                        have = str(ent.get("state", ""))
                        if want and have.lower() == want.lower():
                            delta -= 0.35    # 缓存已显示目标达成 → 强烈抑制
            except Exception:
                pass
            try:
                st = relationship_manager.get_stage(
                    str(CONFIG.get("owner_qq", "")))
                if isinstance(st, dict) and st.get("stage", 0) >= 4 and \
                        cand.action == "speak":
                    delta += 0.1
            except Exception:
                pass
            return max(-0.4, min(0.2, delta))

        def _load_runtime(self):
            """恢复冷却/抑制/探针（重启不丢防循环状态）"""
            try:
                data = dm.load("proactive_actions_data", {})
                for tgt, until in (data.get("cooldowns") or {}).items():
                    if until and until > time.time():
                        self.cooldowns[tgt] = until
                self._suppressed = dict(data.get("suppressed") or {})
                probe = data.get("probe") or {}
                self._probe = {t: (str(st), 1) for t, st in probe.items()}
            except Exception:
                pass

        def _domain_key_for_target(self, target):
            """按目标实体推断能力域（light→ha_light 等）"""
            t = (target or "").lower()
            if t.startswith("light") or "light" in t:
                return "ha_light"
            if t.startswith("lock") or "lock" in t:
                return "ha_lock"
            if "vision" in t or "camera" in t:
                return "vision"
            return None

        def _observe_external_change(self, ev):
            """自主学习：Riku 刚设定期望态，随后目标被外部改回 → 用户干预信号

            条件：suppressed 记录期望态 + 当前事件状态与期望相反 + 目标仍在冷却内
            （冷却内的外部变化 = 紧接着 Riku 动作的用户操作，而非正常使用）
            """
            try:
                _key = ev.get("dedup_key")
                if _key and _key in self._handled:
                    return    # 重复投递/回显事件，不是外部干预
                target = ev.get("entity_id") or ev.get("device_id")
                state = ev.get("state")
                if not target or state is None:
                    return
                sup = self._suppressed.get(target)
                if sup is None or not isinstance(sup, dict):
                    return
                want = str(next(iter(sup.values()), ""))
                if not want or str(state).lower() == want.lower():
                    return
                if not self._cooldown_active(target):
                    return
                dk = self._domain_key_for_target(target)
                if dk:
                    act = self._DOMAIN_SPECS.get(dk, {}).get("action", "control_device")
                    self._learner.observe_user_revert(dk, act)
            except Exception:
                pass

        # ---------- 生命周期 ----------
        def tick(self):
            self._handled.clear()
            self._poll_external()
            events = _proactive_bus.drain()
            for ev in events:
                try:
                    self._process_event(ev)
                except Exception as e:
                    logger.warning("[V20] 主动行为处理失败: %s" % e)
            # 低频落盘
            if self.decisions and int(time.time()) % 60 < 3:
                self.flush()

        def start(self):
            if self._alive:
                return
            self._alive = True

            def _loop():
                while self._alive:
                    try:
                        self.tick()
                    except Exception as e:
                        logger.warning("[V20] 主动行为引擎巡检异常: %s" % e)
                    time.sleep(_PAC_SCAN_INTERVAL)

            threading.Thread(target=_loop, daemon=True).start()

        def shutdown(self):
            self._alive = False
            try:
                self.flush()            # 完整落盘（含 cooldowns/suppressed/probe）
            except Exception:
                pass

        def flush(self):
            try:
                self.history.flush()
                dm.save("proactive_actions_data",
                        {"history": self.history.entries[-2000:],
                         "stats": self.history.stats,
                         "recent_decisions": self.decisions,
                         "cooldowns": {k: v for k, v in self.cooldowns.items()
                                       if v > time.time()},
                         "suppressed": self._suppressed,
                         "probe": {k: v[0] for k, v in self._probe.items()},
                         "preferences": self._learner.to_dict()})
                dm.flush_all()
            except Exception as e:
                logger.warning("[V20] 主动行为数据落盘失败: %s" % e)

        def status(self):
            return {
                "alive": self._alive,
                "scan_interval": _PAC_SCAN_INTERVAL,
                "max_auto_risk": RiskLevel.name(self.max_auto_risk),
                "min_confidence": self.min_confidence,
                "stats": dict(self._stats),
                "cooldowns": {k: "%ds" % int(v - time.time())
                              for k, v in self.cooldowns.items()
                              if v > time.time()},
                "suppressed": list(self._suppressed)[:20],
                "domains": list(self._DOMAIN_SPECS),
                "recent": self.decisions[-6:][::-1],
                "history": self.history.recent(5),
            }

    def _build_cognition_snapshot():
        """认知快照：device_state/environment/vision/emotion/memory/relationship
        聚合为同一份上下文（供决策与人格，不新增 LLM 模型）"""
        parts = []
        try:
            if context.environment_hub is not None:
                parts.append(context.environment_hub.build_awareness_summary())
        except Exception:
            pass
        try:
            ve = getattr(context, "_vis_events", None)
            if ve is not None:
                recent = ve.recent(3)
                if recent:
                    parts.append("最近视觉事件:" +
                                 "、".join(e["type"] for e in recent))
        except Exception:
            pass
        try:
            if event_memory is not None:
                imp = event_memory.get_important_events(limit=3) \
                    if hasattr(event_memory, "get_important_events") else []
                if imp:
                    parts.append("相关记忆:" + "；".join(
                        str(x)[:60] for x in imp))
        except Exception:
            pass
        try:
            ha = getattr(context, "_smart_home_agent", None)
            if ha is not None:
                cache = getattr(ha, "_state_cache", {})
                if cache:
                    _top = list(cache.items())[:5]
                    parts.append("家居关键状态:" + "；".join(
                        "%s=%s" % (k, v.get("state", "?")) for k, v in _top))
        except Exception:
            pass
        try:
            if unified_emotion is not None and \
                    hasattr(unified_emotion, "get_mood_summary"):
                _ms = unified_emotion.get_mood_summary()
                if _ms:
                    parts.append("当前心境:" + str(_ms)[:60])
        except Exception:
            pass
        try:
            rel = relationship_manager.get_stage(str(CONFIG.get("owner_qq", "")))
            if isinstance(rel, dict) and rel.get("stage") is not None:
                parts.append("关系阶段:%s" % rel.get("stage"))
        except Exception:
            pass
        return "\n".join(parts) if parts else "（暂无认知快照）"

    context.action_engine = ActionEngine(_PAC_CFG)
    context.action_engine.start()
    try:
        _DIRTY_MODULES["proactive_actions_data"] = context.action_engine
    except NameError:
        pass
    logger.info("[V20] 主动行为系统已启用：决策集 %s，自主上限 %s，扫描 %ds" %
                ("ACT/SPEAK/OBSERVE/WAIT/IGNORE/ASK",
                 RiskLevel.name(_PAC_MAX_AUTO_RISK), _PAC_SCAN_INTERVAL))

    @register_command("private", ["行为偏好", "偏好反馈"], perm_required=2, owner_only=True)
    def cmd_pref_feedback(ws, uid, message, perm, is_owner, gid=None):
        """行为偏好：查看 / 反馈
        用法：
          行为偏好               → 查看当前偏好
          行为偏好 ha_light 不喜欢 → 该域控制行为权重 -0.4
          行为偏好 ha_light 喜欢  → 权重 +0.4
          行为偏好 全部 不喜欢     → 全部控制域 -0.4
        """
        parts = (message or "").strip().split()
        if len(parts) < 2 or parts[1] in ("查看", "list", "状态"):
            rows = context.action_engine._learner.summary(10)
            txt = "；".join("%s=%.2f(n%d)" % (r["key"], r["w"], r["n"])
                            for r in rows) if rows else "（暂无偏好数据，行为会在运行中自主学习）"
            send_private(ws, uid, "【行为偏好】%s" % txt)
            return
        dom = parts[1].lower()
        delta = 0.0
        if len(parts) >= 3:
            v = parts[2]
            if v in ("喜欢", "好", "赞", "+", "+1", "1"):
                delta = 0.4
            elif v in ("不喜欢", "不好", "烦", "停", "-", "-1", "0"):
                delta = -0.4
            else:
                try:
                    delta = max(-0.5, min(0.5, float(v)))
                except Exception:
                    delta = 0.0
        if delta == 0.0:
            send_private(ws, uid,
                         "用法：行为偏好 <域> <喜欢/不喜欢>（域：ha_light/ha_lock/vision/全部）")
            return
        if dom == "全部":
            for k in context.action_engine._DOMAIN_SPECS:
                context.action_engine._learner.apply_feedback(k, "control_device", delta, "owner")
            send_private(ws, uid, "已对全部控制域施加偏好 %+.2f" % delta)
            return
        w = context.action_engine._learner.apply_feedback(dom, "control_device", delta, "owner")
        send_private(ws, uid, "已记录偏好：%s control_device → 权重 %+.2f（影响后续自主决策置信度）" % (dom, w))


    @register_command("private", ["无线状态", "无线探测状态"], perm_required=2,
                      owner_only=True)
    def cmd_wireless_status(ws, uid, message, perm, is_owner, gid=None):
        """无线探测状态：查看 WiFi / 蓝牙 探测配置与能力"""
        try:
            wsrc = getattr(context, "_WIFI_SOURCE", "none")
            bsrc = getattr(context, "_BT_SOURCE", "none")
        except Exception:
            wsrc, bsrc = "none", "none"
        lines = [
            "【无线探测】",
            "WiFi 探测: %s（wifi_scan / wifi_lookup）" % wsrc,
            "蓝牙探测: %s（bt_scan / bt_lookup）" % bsrc,
            "用法示例：",
            "  寻找 WiFi 设备: 无线探测 wifi_lookup mac=AA:BB:CC:11:22:33",
            "  寻找蓝牙设备: 无线探测 bt_lookup name=RikuTag",
        ]
        if context.device_manager is not None:
            for did in ("wifi-probe", "bt-probe"):
                d = context.device_manager.get_device(did)
                if d is not None:
                    lines.append("· %s [%s] 能力: %s" % (d.name, d.device_id,
                                                         ", ".join(d.capabilities)))
        send_private(ws, uid, "\n".join(lines))

    @register_command("private", ["主动行为状态"], perm_required=2, owner_only=True)
    def _cmd_proactive_status(msg):
        st = context.action_engine.status()
        lines = [
            "【主动行为系统】",
            "运行: %s · 扫描: %ds · 自主上限: %s · 置信门槛: %0.2f" %
            ("运行中" if st["alive"] else "停止", st["scan_interval"],
             st["max_auto_risk"], st["min_confidence"]),
            "决策统计: %s" % " · ".join("%s=%d" % (k, v)
                                       for k, v in st["stats"].items()),
            "冷却中: %s" % ("、".join("%s(%s)" % (k, v)
                                     for k, v in st["cooldowns"].items())
                           if st["cooldowns"] else "无"),
            "成功抑制: %s" % ("、".join(st["suppressed"]) if st["suppressed"] else "无"),
            "能力域: %s" % "、".join(st["domains"]),
            "行为偏好: %s" % ("；".join("%s=%s" % (r["key"], r["w"])
                                       for r in context.action_engine._learner.summary(5))
                              if context.action_engine._learner.summary(5) else "无"),
            "最近决策:",
        ]
        for d in st["recent"][:5]:
            lines.append("  [%s] %s %s→%s 结果:%s" %
                         (d.get("ts", ""), d.get("decision"),
                          d.get("device_id"), d.get("action"), d.get("outcome")))
        return "\n".join(lines)

# ============================================================
# 四十三、主程序入口
# ============================================================

def boot_sequence():
    """
    启动加载序列——终端风格的进度条
    里克的「人格唤醒」流程

    效果：
      [SYSTEM] 正在导入人物记忆档案文件      [██░░░░░░░░] 20%
      完成后保留日志行，逐步推进到 100%
      最后显示「人格唤醒完成」+ ASCII art
    """
    import shutil
    try:
        tw = shutil.get_terminal_size((80, 20)).columns
    except Exception:
        tw = 80

    # ANSI 颜色
    C_RESET = "\033[0m"
    C_DIM   = "\033[2m"
    C_GREEN = "\033[32m"
    C_CYAN  = "\033[36m"
    C_YELLOW= "\033[33m"
    C_GRAY  = "\033[90m"
    C_BOLD  = "\033[1m"
    C_RED   = "\033[31m"

    bar_width = 20

    def _bar(pct):
        """生成 ASCII 进度条"""
        filled = int(bar_width * pct / 100)
        empty = bar_width - filled
        return f"[{C_GREEN}{'█' * filled}{C_GRAY}{'░' * empty}{C_RESET}]"

    def _print_line(text, pct=None, tag="SYSTEM", done=False):
        """打印一行加载日志"""
        tag_str = f"{C_DIM}[{tag}]{C_RESET}"
        if pct is not None:
            bar = _bar(pct)
            pct_str = f"{pct:3d}%"
            line = f"\r{tag_str} {text:<40} {bar} {pct_str}"
        else:
            status = f"{C_GREEN}✓{C_RESET}" if done else ""
            line = f"\r{tag_str} {text:<40} {status}   "
        sys.stdout.write(line + " " * max(0, tw - len(line) - 20))
        sys.stdout.flush()

    # 启动步骤（text, base_pct, increment, duration_seconds, is_final）
    steps = [
        ("正在初始化系统环境",            0,  8, 0.3),
        ("正在创建数据目录结构",          8,  7, 0.2),
        ("正在导入人物记忆档案文件",      15, 12, 0.8),
        ("正在解析性格特征样本",          27, 10, 0.6),
        ("性格特征样本载入完成",          37, 0,  0.2),
        ("正在加载 V10 统一情绪引擎",     37, 12, 0.8),
        ("情绪轮模型初始化",              49, 6,  0.4),
        ("情绪调节系统校准",              55, 5,  0.3),
        ("正在校准 V11 时间感知系统",     60, 8,  0.6),
        ("昼夜节律同步中",                68, 4,  0.4),
        ("正在同步天气环境数据",          72, 6,  0.5),
        ("梦境系统就绪检查",              78, 4,  0.2),
        ("正在加载心理学核心模块",        82, 8,  0.7),
        ("正在加载记忆增强套件",          90, 5,  0.4),
        ("正在检查联网搜索模块",          95, 2,  0.2),
        ("正在初始化现实世界能力层",      95, 2,  0.3),
        ("正在连接语言模型服务",          97, 2,  0.5),
    ]

    # === 开始 ===
    print()
    print(f"  {C_BOLD}{'─' * min(tw - 2, 56)}{C_RESET}")
    print(f"  {C_CYAN}{C_BOLD}  塔洛斯·里克 人格唤醒系统{C_RESET}")
    print(f"  {C_GRAY}  Personality Awakening Protocol v12.0{C_RESET}")
    print(f"  {C_BOLD}{'─' * min(tw - 2, 56)}{C_RESET}")
    print()

    for text, base_pct, increment, duration in steps:
        # 如果有增量，显示进度条动画
        if increment > 0:
            frames = max(3, int(duration / 0.05))
            for i in range(frames + 1):
                pct = int(base_pct + increment * (i / frames))
                pct = min(99, pct)
                _print_line(text, pct)
                time.sleep(duration / frames)
            # 完成后换行
            sys.stdout.write(f"\n")
            sys.stdout.flush()
        else:
            # 无进度的步骤，直接显示完成
            _print_line(text, done=True)
            time.sleep(duration)
            sys.stdout.write(f"\n")
            sys.stdout.flush()

    # 最终人格唤醒
    _print_line("人格唤醒进度", 99)
    time.sleep(0.3)
    _print_line("人格唤醒进度", 100)
    time.sleep(0.5)
    sys.stdout.write(f"\n")
    sys.stdout.flush()

    # 完成
    print()
    print(f"  {C_GREEN}{C_BOLD}  [✓] 人格唤醒完成{C_RESET}")
    print()

    # ASCII art
    art = f"""{C_DIM}
      ╔═══════════════════════════════╗
      ║  里克已上线。                  ║
      ║  「……嗯。我醒了。」            ║
      ╚═══════════════════════════════╝{C_RESET}
"""
    print(art)
    # V23-A：统一 Agent State 初始化（挂持久化 + 崩溃恢复）
    try:
        _init_agent_state()
    except Exception:
        pass
    print(f"  {C_GRAY}系统就绪，等待连接 NapCat...{C_RESET}")
    print()

# ============================================================
# V22 取餐码通知接入（手机通知 → webhook → 主动提醒）
# ------------------------------------------------------------
# 手机端（安卓 9~17）：Tasker + AutoNotification 监听外卖通知，
#   把通知内容 POST 到本服务即可，无需写 App。
#   请求：POST /notify
#         Header: X-Notify-Secret: <CONFIG 里的 notify_secret>
#         Body:  {"title": "取餐码：A12", "text": "...", "package": "com.xxx", "time": 123}
#   响应：{"ok": true, "matched": bool, "code": "A12(若有)"}
# 安全：未配置 secret 时只监听 127.0.0.1；配置后监听 0.0.0.0（供手机访问）。
# ============================================================
import http.server as _http_server
import threading as _threading

_NOTIFY_KEYWORDS = ("取餐码", "取餐号", "餐号", "凭取餐码", "出餐", "取餐")
_NOTIFY_CODE_RE = re.compile(r'(?i)(取餐码|取餐号|餐号)[:：]?\s*([A-Za-z]{0,2}\d{2,6})')
_NOTIFY_DEDUP_SECONDS = 900      # 同取餐码 15 分钟内不重复提醒
_NOTIFY_RATE_LIMIT_SECONDS = 30  # 全局 30 秒最多发 1 条提醒


class _NotifyHTTPServer:
    """取餐码通知接收服务（HTTP webhook）"""

    def __init__(self, port, secret):
        self.port = int(port or 8765)
        self.secret = str(secret or "")
        self._srv = None
        self._thread = None
        self._recent = {}      # 取餐码 -> 最近时间戳（去重）
        self._last_send = 0.0  # 最近一次发 QQ 的时间（频控）
        self._last = {}        # 最近一条通知（「取餐提醒 状态」可查）
        self._total = 0
        self._state_recent = {}  # 设备状态类型 -> 最近时间戳（每类 30 分钟去重）
        self._device_state = {}  # 手机最近上报的设备状态（电量/存储等）

    # ---- 通知处理核心（测试可直接调用） ----
    # 三路分流：取餐通知 → 设备状态通知 → 其他一律忽略（不打扰）
    def handle_notification(self, payload):
        try:
            title = str(payload.get("title") or "")
            text = str(payload.get("text") or "")
            package = str(payload.get("package") or "")
            content = title + " " + text
            now = time.time()
            # 1) 取餐类（出餐/取餐码）
            if any(k in content for k in _NOTIFY_KEYWORDS):
                return self._handle_takeout(title, text, package, content, now)
            # 2) 设备状态类（低电量/充满/存储不足）
            st = self._match_device_state(content)
            if st is not None:
                return self._handle_device_state(title, text, package, st, now)
            # 3) 其他通知 → 不打扰
            self._last = {"ts": now, "matched": False, "package": package}
            return {"ok": True, "matched": False}
        except Exception as e:
            logger.error("通知处理失败: %s", e)
            return {"ok": False, "error": str(e)}

    # ---- 取餐通知 ----
    def _handle_takeout(self, title, text, package, content, now):
        m = _NOTIFY_CODE_RE.search(content)
        code = m.group(2) if m else ""
        if code and self._recent.get(code, 0) and now - self._recent[code] < _NOTIFY_DEDUP_SECONDS:
            return {"ok": True, "matched": True, "code": code, "duplicated": True}
        if code:
            self._recent[code] = now
        self._total += 1
        entry = {"ts": now, "matched": True, "category": "takeout", "code": code,
                 "package": package, "title": title, "text": text}
        self._last = entry
        self._log(entry)
        self._remind_owner("takeout", {"code": code, "text": text})
        return {"ok": True, "matched": True, "code": code, "category": "takeout"}

    # ---- 设备状态识别 ----
    @staticmethod
    def _match_device_state(content):
        low = content.lower()
        if any(k in low for k in ("低电量", "电量不足", "电量只剩", "剩余电量", "battery low")):
            m = re.search(r'(\d{1,3})\s*%', content)
            return ("battery_low", int(m.group(1)) if m else None)
        if any(k in low for k in ("已充满", "充电完成", "已充满电", "battery full")):
            m = re.search(r'(\d{1,3})\s*%', content)
            return ("battery_full", int(m.group(1)) if m else None)
        if any(k in low for k in ("存储空间不足", "存储不足", "内存不足", "空间不足")):
            return ("storage_low", None)
        return None

    # ---- 设备状态通知（每类 30 分钟去重，防止系统重复轰炸） ----
    def _handle_device_state(self, title, text, package, st, now):
        kind, level = st
        if self._state_recent.get(kind, 0) and now - self._state_recent[kind] < 1800:
            return {"ok": True, "matched": True, "category": "device_state",
                    "state": kind, "suppressed": True}
        self._state_recent[kind] = now
        self._total += 1
        self._device_state = {"battery_level": level, "last_state": kind,
                              "ts": now, "package": package, "title": title, "text": text}
        entry = {"ts": now, "matched": True, "category": "device_state", "state": kind,
                 "level": level, "package": package, "title": title, "text": text}
        self._last = entry
        self._log(entry)
        self._remind_owner("device_state", {"state": kind, "level": level, "text": text})
        return {"ok": True, "matched": True, "category": "device_state", "state": kind,
                "level": level}

    # ---- 通用落盘（保留最近 200 条） ----
    def _log(self, entry):
        try:
            log = dm.load("notify_log_data", [])
            log.append(entry)
            dm.save("notify_log_data", log[-200:])
        except Exception:
            pass

    # ---- 主动提醒 owner（QQ 真发，全局 30 秒频控） ----
    def _remind_owner(self, category, info):
        try:
            _ref = getattr(context, "context.wsm", None)
            _ws = getattr(_ref, "_ws", None) if _ref is not None else None
            if _ws is None or not getattr(_ws, "sock", None):
                return  # 未连接 QQ：仅靠日志
            owner = str(CONFIG.get("owner_qq", ""))
            if not owner:
                return
            now = time.time()
            if now - self._last_send < _NOTIFY_RATE_LIMIT_SECONDS:
                return
            self._last_send = now
            if category == "takeout":
                code = info.get("code")
                msg = (f"【取餐提醒】主人，出餐啦～取餐码是 {code}，记得去拿哦喵！" if code
                       else f"【取餐提醒】主人～收到一条取餐通知：{(info.get('text') or '')[:40]}")
            elif category == "device_state":
                kind, level, text = info.get("state"), info.get("level"), info.get("text") or ""
                if kind == "battery_low":
                    msg = (f"【设备状态】主人，手机电量只剩 {level}% 啦，记得充电喵～" if level is not None
                           else "【设备状态】主人，手机低电量啦，记得充电喵～")
                elif kind == "battery_full":
                    msg = "【设备状态】手机充电完成，可以拔掉充电器啦喵～"
                else:
                    msg = f"【设备状态】主人，手机存储空间不足：{text[:40]}"
            else:
                return
            send_private(_ws, owner, msg)
        except Exception as e:
            logger.warning("通知提醒发送失败: %s", e)


class _NotifyHandler(_http_server.BaseHTTPRequestHandler):
    """POST /notify —— 接收手机端转发的通知"""

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length)
            payload = json.loads(body.decode("utf-8") or "{}")
            secret = self.headers.get("X-Notify-Secret") or ""
            srv = self.server.notify_srv
            if srv.secret and secret != srv.secret:
                self._resp(403, {"ok": False, "error": "bad_secret"})
                return
            self._resp(200, srv.handle_notification(payload))
        except Exception as e:
            self._resp(500, {"ok": False, "error": str(e)})

    def do_GET(self):
        self._resp(200, {"ok": True, "service": "notify",
                         "hint": "POST /notify with {title,text,package}"})

    def _resp(self, code, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass  # 静默访问日志


class _NotifyThreadingServer(_http_server.ThreadingHTTPServer):
    def __init__(self, *args, notify_srv=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.notify_srv = notify_srv


context.notify_instance = None


def start_notify_server():
    """启动取餐通知 webhook 服务（main 启动时调用；模块开关控制）"""
    if context.notify_instance is not None:
        return context.notify_instance
    if not CONFIG["modules"].get("notify_webhook", True):
        return None
    cfg = CONFIG.get("notify_webhook") or {}
    port = int(cfg.get("port") or 8765)
    secret = str(cfg.get("secret") or "")
    inst = _NotifyHTTPServer(port, secret)
    bind = "0.0.0.0" if secret else "127.0.0.1"
    srv = _NotifyThreadingServer((bind, port), _NotifyHandler, notify_srv=inst)
    inst._srv = srv
    inst._thread = _threading.Thread(target=srv.serve_forever,
                                     daemon=True, name="notify-webhook")
    inst._thread.start()
    context.notify_instance = inst
    logger.info("取餐通知服务已启动：%s:%s（secret %s）",
                bind, port, "已配置" if secret else "未配置，仅本机可访问")
    return inst


def stop_notify_server():
    """停止取餐通知服务（main 关停时调用）"""
    if context.notify_instance is not None:
        try:
            context.notify_instance._srv.shutdown()
            context.notify_instance._srv.server_close()
        except Exception:
            pass
        context.notify_instance = None


@register_command("private", ["取餐提醒"], perm_required=2, owner_only=True)
def _cmd_notify_status(msg):
    parts = (msg or "").split()
    sub = parts[0] if parts else ""
    inst = getattr(context, "context.notify_instance", None)
    if sub == "关闭":
        stop_notify_server()
        return "取餐提醒已关闭（重启后恢复）。"
    if sub == "开启":
        try:
            start_notify_server()
            return "取餐提醒已开启。"
        except Exception as e:
            return "开启失败：%s" % e
    if sub == "帮助":
        cfg = CONFIG.get("notify_webhook") or {}
        port = cfg.get("port", "8765")
        secret = cfg.get("secret", "")
        return ("【通知接入 手机端配置】\n"
                "1. 手机装 SMS Forwarder（短信转发器，开源免费）\n"
                "2. 发送通道 → Webhook：http://<bot地址>:%s/notify\n"
                "   请求体：{\"title\":\"{{TITLE}}\",\"text\":\"{{MSG}}\",\"package\":\"{{APP_NAME}}\"}\n"
                "   请求头：X-Notify-Secret: %s\n"
                "3. 转发规则：\n"
                "   - 取餐：类型 App 通知，勾选外卖 App，关键词“取餐”\n"
                "   - 设备状态：监听“Android 系统”通知（低电量/充满/存储不足）\n"
                "4. 用「取餐提醒 状态」「设备状态」确认" % (port, secret or "(未设置，保持留空)"))
    inst = getattr(context, "context.notify_instance", None)
    if inst is None:
        return "取餐提醒服务未启动。发送「取餐提醒 开启」启动。"
    last = inst._last or {}
    return ("通知服务在线（端口 %s）\n"
            "累计收到：%s 条有效通知\n"
            "最近一条：%s"
            % (inst.port, inst._total,
               last.get("text") or last.get("title") or "暂无"))


@register_command("private", ["设备状态"], perm_required=2, owner_only=True)
def _cmd_device_status(msg):
    inst = getattr(context, "context.notify_instance", None)
    if inst is None:
        return "通知服务未启动。发送「取餐提醒 开启」启动。"
    st = inst._device_state or {}
    if not st:
        return "还没有收到过设备状态通知。\n手机端配置见「取餐提醒 帮助」。"
    try:
        from datetime import datetime as _dt
        ts = _dt.fromtimestamp(st.get("ts", 0)).strftime("%m-%d %H:%M")
    except Exception:
        ts = "?"
    return ("手机最近状态（%s）：\n"
            "电量：%s%%\n"
            "最近事件：%s\n"
            "%s" % (ts, st.get("battery_level", "?"),
                    st.get("last_state", "?"),
                    (st.get("text") or "")[:50]))

