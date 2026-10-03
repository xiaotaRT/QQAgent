#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""现实世界能力层（从 src_m_reality.py 迁移）。

包含：RiskLevel、DeviceType、RikuDevice、RikuDeviceManager、GatewayTask、
GatewayDispatcher、ToolGateway、EnvironmentHub、PerceptionLoop、RikuNode 系列，
以及模块级初始化（device_manager / tool_gateway / environment_hub）。
"""
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
from qqagent.core.utils import _FakeLock, _now_str

class RiskLevel:
    """风险等级：0=低 1=中 2=高 3=严重"""
    LOW = 0
    MEDIUM = 1
    HIGH = 2
    CRITICAL = 3

    _NAMES = {0: "低", 1: "中", 2: "高", 3: "严重"}

    @classmethod
    def name(cls, level):
        return cls._NAMES.get(level, "未知")


class DeviceType:
    WINDOWS = "windows"
    ANDROID = "android"
    VISION = "vision"
    HOMEASSISTANT = "homeassistant"
    CAR = "car"
    WIFI = "wifi"                        # V21 WiFi 探测节点
    BLUETOOTH = "bluetooth"              # V21 蓝牙探测节点


# ---------- 设备模型 ----------
class RikuDevice:
    """现实世界设备统一模型（十六：设备统一管理）"""

    def __init__(self, device_id, device_type, name="", capabilities=None,
                 op_risks=None, forbidden_ops=None, metadata=None):
        self.device_id = str(device_id)
        self.device_type = device_type
        self.name = name or str(device_id)
        self.capabilities = list(capabilities or [])
        self.op_risks = dict(op_risks or {})
        self.forbidden_ops = set(forbidden_ops or [])
        self.hard_blocked_ops = set()
        self.metadata = dict(metadata or {})
        self.online = False
        self.enabled = True
        self.last_heartbeat = 0.0
        self.agent_deny_all = False
        self.driving_state = "parked"
        self.node = None                # Riku Node 适配器（远端）
        self.local_handlers = {}        # 本地能力处理器 {op: fn(params, device=None)}
        self._dirty = False
        self._register_ts = time.time()

    def supports(self, operation):
        return operation in self.capabilities

    def risk_of(self, operation):
        return self.op_risks.get(operation, RiskLevel.MEDIUM)

    def mark_dirty(self):
        self._dirty = True

    def to_dict(self):
        return {
            "device_id": self.device_id,
            "name": self.name,
            "device_type": self.device_type,
            "capabilities": sorted(self.capabilities),
            "forbidden_ops": sorted(self.forbidden_ops),
            "metadata": self.metadata,
            "online": self.online,
            "enabled": self.enabled,
            "driving_state": self.driving_state,
            "last_heartbeat": self.last_heartbeat,
            "agent_deny_all": self.agent_deny_all,
            "registered_at": self._register_ts,
        }


# ---------- 设备管理器 ----------
class RikuDeviceManager:
    """统一 Device Manager（十六）：注册/查询/在线状态/落盘"""

    def __init__(self):
        self._devices = {}
        self._events = []
        self._lock = _FakeLock() if "_FakeLock" in globals() else threading.Lock()
        self._dirty = False
        self._load()

    def _load(self):
        try:
            data = dm.load("device_registry", {"devices": {}, "events": []})
            for did, d in data.get("devices", {}).items():
                dev = RikuDevice(d.get("device_id", did), d.get("device_type", "windows"),
                                 name=d.get("name", ""),
                                 capabilities=d.get("capabilities", []),
                                 forbidden_ops=d.get("forbidden_ops", []),
                                 metadata=d.get("metadata", {}))
                dev.enabled = bool(d.get("enabled", True))
                dev.agent_deny_all = bool(d.get("agent_deny_all", False))
                dev._register_ts = d.get("registered_at", time.time())
                self._devices[dev.device_id] = dev
            self._events = data.get("events", [])[-50:]
        except Exception as e:
            logger.warning("[V13] 设备注册表加载失败: %s" % e)

    def register_device(self, device):
        with self._lock:
            self._devices[device.device_id] = device
            device.online = True
            device.last_heartbeat = time.time()
            self._events.append("设备已注册：%s" % device.name)
            self._events = self._events[-50:]
        self._dirty = True
        return {"ok": True, "device_id": device.device_id}

    def get_device(self, device_id):
        return self._devices.get(str(device_id))

    def all_devices(self):
        return list(self._devices.values())

    def list_devices(self, include_disabled=True):
        rows = []
        for d in self._devices.values():
            if not include_disabled and not d.enabled:
                continue
            rows.append({"device_id": d.device_id, "name": d.name,
                         "type": d.device_type, "online": d.online,
                         "enabled": d.enabled,
                         "capabilities": sorted(d.capabilities)[:20]})
        return rows

    def recent_events(self, limit=20):
        return self._events[-limit:]

    def flush(self):
        try:
            dm.save("device_registry", {
                "devices": {d.device_id: d.to_dict() for d in self._devices.values()},
                "events": self._events[-50:],
            })
            dm.flush_all()
            self._dirty = False
        except Exception as e:
            logger.warning("[V13] 设备注册表落盘失败: %s" % e)

    def shutdown(self):
        try:
            self.flush()
        except Exception:
            pass


# ---------- 任务与调度器 ----------
class GatewayTask:
    def __init__(self, device_id, operation, params, actor, priority=1,
                 timeout=30, max_retries=2, risk=RiskLevel.LOW):
        self.device_id = device_id
        self.operation = operation
        self.params = params or {}
        self.actor = actor
        self.priority = priority
        self.timeout = timeout
        self.max_retries = max_retries
        self.risk = risk
        self.handler = None
        self.result = None
        self.task_id = None
        self._event = threading.Event()


class GatewayDispatcher:
    """任务分发线程池：优先级/超时/退避重试/执行日志"""

    def __init__(self, config=None):
        self._cfg = config or {}
        self._workers = max(1, int(self._cfg.get("task_workers", 3)))
        self._pool = []
        self._queue = []
        self._lock = threading.Lock()
        self._alive = True
        self._task_counter = 0
        for _ in range(self._workers):
            t = threading.Thread(target=self._run_loop, daemon=True)
            t.start()
            self._pool.append(t)

    def submit(self, task):
        with self._lock:
            self._task_counter += 1
            task.task_id = "GT%d%04d" % (int(time.time() * 1000), self._task_counter)
            self._queue.append(task)
        return task.task_id

    def _run_loop(self):
        while self._alive:
            task = None
            with self._lock:
                if self._queue:
                    self._queue.sort(key=lambda t: t.priority, reverse=True)
                    task = self._queue.pop(0)
            if task is None:
                time.sleep(0.05)
                continue
            self._execute(task)

    def _execute(self, task):
        attempts = 0
        backoff = float(self._cfg.get("task_retry_backoff", 0.05))
        while attempts <= task.max_retries:
            attempts += 1
            try:
                task.result = task.handler(task)
                if task.result.get("retryable") and attempts <= task.max_retries:
                    time.sleep(backoff * attempts)
                    continue
                task._event.set()
                return
            except Exception as e:
                if attempts <= task.max_retries:
                    time.sleep(backoff * attempts)
                    continue
                task.result = {"ok": False, "reason": "handler_error", "error": str(e)}
                task._event.set()
                return
        task._event.set()

    def shutdown(self):
        self._alive = False


# ---------- 统一工具网关 ----------
class ToolGateway:
    """统一 Tool Gateway（一/十八）：Agent → 网关 → Device 标准调用链"""

    _SENSITIVE_KEYS = ("token", "password", "api_key", "secret", "cookie", "authorization")

    def __init__(self, dmgr, config=None):
        self._dm = dmgr
        self._cfg = config or {}
        self._emergency_stop = False
        self._log = []
        self._tickets = {}
        self._ticket_seq = 0
        self._tasks = {}
        self._lock = threading.Lock()
        self._ticket_lock = threading.Lock()
        self._dispatcher = GatewayDispatcher(self._cfg)
        self._dirty = False
        self.trust_risk_cap_fn = None          # V19 人格层钩子
        self.high_power_check_fn = None        # V19 高功率限制钩子

    # ---------- 紧急停止（十八） ----------
    def emergency_stop(self, reason=""):
        self._emergency_stop = True
        self._audit_log("*", "*", "system", RiskLevel.CRITICAL, "emergency_stop",
                        {"reason": reason})
        logger.warning("[V13] 紧急停止已启用：%s" % reason)

    def resume(self, reason=""):
        self._emergency_stop = False
        self._audit_log("*", "*", "system", RiskLevel.LOW, "resume", {"reason": reason})
        logger.info("[V13] 设备控制已恢复：%s" % reason)

    def is_emergency_stopped(self):
        return self._emergency_stop

    # ---------- 主入口 ----------
    def execute(self, device_id, operation, params=None, actor="agent",
                priority=None, require_confirm=False):
        """标准调用链：紧急停止→设备检查→风险→行驶限制→硬禁止→黑名单→权限→确认→分发
        返回: {"ok", "reason", "risk", "ticket_id", "task_id", "result", "message"}"""
        params = params or {}
        if self._emergency_stop:
            self._audit_log(device_id, operation, actor, RiskLevel.LOW,
                            "denied_emergency_stop", params)
            return {"ok": False, "reason": "emergency_stop",
                    "message": "紧急停止已启用，所有设备操作被冻结。"}
        dev = self._dm.get_device(device_id)
        if dev is None:
            self._audit_log(device_id, operation, actor, RiskLevel.LOW,
                            "device_not_found", params)
            return {"ok": False, "reason": "device_not_found",
                    "message": "未注册设备：%s" % device_id}
        if not dev.enabled:
            self._audit_log(device_id, operation, actor, RiskLevel.LOW,
                            "device_disabled", params)
            return {"ok": False, "reason": "device_disabled",
                    "message": "设备已被禁用。"}
        if not dev.supports(operation):
            self._audit_log(device_id, operation, actor, RiskLevel.LOW,
                            "op_not_supported", params)
            return {"ok": False, "reason": "op_not_supported",
                    "message": "设备 %s 不支持操作：%s" % (device_id, operation)}
        risk = dev.risk_of(operation)

        # 行驶安全限制（二十一）：行驶中禁止高风险自动操作
        if getattr(dev, "driving_state", "parked") == "driving" and risk >= RiskLevel.HIGH:
            self._audit_log(device_id, operation, actor, risk,
                            "denied_driving_restriction", params)
            return {"ok": False, "reason": "driving_restriction",
                    "message": "行驶中禁止高风险操作：%s" % operation}

        # 硬禁止：任何角色不可执行（V14+ 危险系统操作）
        if getattr(dev, "hard_blocked_ops", None) and operation in dev.hard_blocked_ops:
            self._audit_log(device_id, operation, actor, RiskLevel.CRITICAL,
                            "hard_blocked", params)
            return {"ok": False, "reason": "hard_blocked",
                    "risk": RiskLevel.CRITICAL,
                    "message": "操作 %s 属于硬禁止范围（危险系统操作），任何角色不可执行。" % operation}

        # 设备级黑名单（十八）：默认禁止；仅 owner 二次确认可临时放行
        if operation in dev.forbidden_ops:
            if actor != "owner":
                self._audit_log(device_id, operation, actor, RiskLevel.CRITICAL,
                                "forbidden_operation", params)
                return {"ok": False, "reason": "forbidden_operation",
                        "risk": RiskLevel.CRITICAL,
                        "message": "操作 %s 属于设备黑名单（默认禁止）。" % operation}
            ticket = self._create_ticket(dev, operation, params, actor,
                                         RiskLevel.CRITICAL, priority)
            if ticket is not None:
                ok = self._wait_ticket(ticket)
                if not ok:
                    return {"ok": False, "reason": ticket["status"],
                            "risk": RiskLevel.CRITICAL, "ticket_id": ticket["id"],
                            "message": "确认%s，操作未执行。" % ticket["status"]}
            else:
                return {"ok": False, "reason": "confirmation_unavailable",
                        "message": "无法创建确认单。"}

        # 权限检查（LLM 不直接持有设备权限，只通过网关按策略放行）
        perm = self._check_permission(dev, operation, risk, actor)
        if not perm["ok"]:
            self._audit_log(device_id, operation, actor, risk,
                            "permission_denied", params)
            return {"ok": False, "reason": perm.get("reason", "permission_denied"),
                    "risk": risk, "message": perm.get("message", "权限不足。")}

        # 人格层整合（V19）：Trust→风险上限 + 高功率限制（不降低原有安全策略）
        cap_fn = getattr(self, "trust_risk_cap_fn", None)
        if cap_fn is not None and actor == "agent":
            cap = cap_fn(None)
            if cap is not None and risk > cap:
                self._audit_log(device_id, operation, actor, risk,
                                "trust_risk_cap", params)
                return {"ok": False, "reason": "trust_risk_cap", "risk": risk,
                        "message": "信任度风险上限 %s 低于操作风险 %s，拒绝自主执行。"
                                   % (cap, risk)}
        hp_fn = getattr(self, "high_power_check_fn", None)
        if hp_fn is not None and hp_fn(params.get("entity_id", "")) and risk >= RiskLevel.HIGH:
            if actor != "owner":
                self._audit_log(device_id, operation, actor, RiskLevel.CRITICAL,
                                "high_power_limited", params)
                return {"ok": False, "reason": "high_power_limited",
                        "risk": RiskLevel.CRITICAL,
                        "message": "高功率电器默认限制自主控制，需 owner 确认。"}
            ticket = self._create_ticket(dev, operation, params, actor,
                                         RiskLevel.CRITICAL, priority)
            if ticket is not None:
                ok = self._wait_ticket(ticket)
                if not ok:
                    return {"ok": False, "reason": ticket["status"],
                            "risk": RiskLevel.CRITICAL, "ticket_id": ticket["id"],
                            "message": "确认%s，操作未执行。" % ticket["status"]}
            else:
                return {"ok": False, "reason": "confirmation_unavailable",
                        "message": "无法创建确认单。"}

        # 单一确认闸门：按风险等级二次确认（高风险强制确认）
        if self._needs_confirmation(dev, operation, risk, actor, require_confirm):
            ticket = self._create_ticket(dev, operation, params, actor, risk, priority)
            if ticket is not None:
                ok = self._wait_ticket(ticket)
                if not ok:
                    return {"ok": False, "reason": ticket["status"],
                            "risk": risk, "ticket_id": ticket["id"],
                            "message": "确认%s，操作未执行。" % ticket["status"]}
            else:
                return {"ok": False, "reason": "confirmation_unavailable",
                        "message": "无法创建确认单。"}

        # 任务分发（优先级/超时/重试/执行日志）
        task = GatewayTask(device_id=device_id, operation=operation, params=params,
                           actor=actor,
                           priority=priority if priority is not None
                           else self._risk_priority(risk),
                           timeout=self._cfg.get("task_timeout", 30),
                           max_retries=self._cfg.get("task_max_retries", 2),
                           risk=risk)
        task.handler = self._make_handler(dev, operation, params)
        task_id = self._dispatcher.submit(task)
        with self._lock:
            self._tasks[task_id] = task
        result = self._wait_task(task)
        status = "success" if result.get("ok") else "failed"
        self._audit_log(device_id, operation, actor, risk, status, params,
                        task_id=task_id, result=result)
        return {"ok": result.get("ok", False),
                "reason": result.get("reason", ""),
                "risk": risk, "task_id": task_id, "result": result}

    # ---------- 权限 / 风险 / 确认 ----------
    def _check_permission(self, dev, operation, risk, actor):
        """权限系统（十八）：owner/admin/agent 三档"""
        if actor == "owner":
            return {"ok": True}
        if actor == "admin":
            return {"ok": True}
        if actor == "agent":
            if dev.agent_deny_all:
                return {"ok": False, "reason": "permission_denied",
                        "message": "该设备默认禁止 Agent 自主访问。"}
            if risk <= RiskLevel.LOW and self._cfg.get("agent_auto_allow_low", True):
                return {"ok": True}
            return {"ok": True}   # MEDIUM/HIGH 在确认闸门处理
        return {"ok": False, "reason": "unknown_actor"}

    def _needs_confirmation(self, dev, operation, risk, actor, require_confirm):
        if require_confirm:
            return True
        if self._cfg.get("require_confirm_high_risk", True) and risk >= RiskLevel.HIGH:
            return True
        return False

    def _risk_priority(self, risk):
        return {RiskLevel.LOW: 1, RiskLevel.MEDIUM: 2,
                RiskLevel.HIGH: 3, RiskLevel.CRITICAL: 4}.get(risk, 1)

    # ---------- 确认单 ----------
    def _create_ticket(self, dev, operation, params, actor, risk, priority):
        with self._ticket_lock:
            self._ticket_seq += 1
            t = {
                "id": "TK%d%03d" % (int(time.time()), self._ticket_seq),
                "device_id": dev.device_id,
                "device_name": dev.name,
                "operation": operation,
                "params": self._sanitize_params(params),
                "actor": actor,
                "risk": risk,
                "risk_name": RiskLevel.name(risk),
                "priority": priority if priority is not None else self._risk_priority(risk),
                "status": "pending",
                "created_at": _now_str(),
                "_event": threading.Event(),
            }
            self._tickets[t["id"]] = t
        self._dirty = True
        return t

    def _wait_ticket(self, ticket, timeout=None):
        to = timeout if timeout is not None else int(self._cfg.get("confirmation_timeout", 120))
        ticket["_event"].wait(timeout=to)
        if ticket["status"] == "pending":
            ticket["status"] = "expired"
        return ticket["status"] == "approved"

    def list_pending_tickets(self):
        return [t for t in self._tickets.values() if t["status"] == "pending"]

    def resolve_ticket(self, ticket_id, approve, actor):
        t = self._tickets.get(ticket_id)
        if t is None:
            return {"ok": False, "reason": "ticket_not_found"}
        if t["status"] != "pending":
            return {"ok": False, "reason": "ticket_not_pending", "status": t["status"]}
        t["status"] = "approved" if approve else "rejected"
        t["resolved_by"] = actor
        t["resolved_at"] = _now_str()
        t["_event"].set()
        self._dirty = True
        return {"ok": True, "status": t["status"], "ticket_id": ticket_id}

    # ---------- 执行器 ----------
    def _make_handler(self, dev, operation, params):
        if dev.node is not None:
            node = dev.node

            def handler(task):
                if not node.connected:
                    return {"ok": False, "reason": "node_not_connected",
                            "message": "Riku Node 未连接。", "retryable": True}
                try:
                    data = node.send_task(operation, params, timeout=task.timeout)
                    if isinstance(data, dict) and "data" in data:
                        return {"ok": bool(data.get("ok", True)),
                                "data": data["data"]}
                    return {"ok": True, "data": data}
                except Exception as e:
                    return {"ok": False, "reason": "node_error",
                            "error": str(e), "retryable": True}
            return handler
        local = dev.local_handlers.get(operation)
        if local is not None:
            def handler(task):
                try:
                    data = local(params, device=dev)
                    return {"ok": True, "data": data}
                except Exception as e:
                    return {"ok": False, "reason": "handler_error", "error": str(e)}
            return handler

        def handler(task):
            return {"ok": False, "reason": "node_not_connected",
                    "message": "该设备尚未接入 Riku Node。"}
        return handler

    def _wait_task(self, task):
        if task._event.wait(timeout=self._cfg.get("task_timeout", 30) + 15):
            return task.result or {"ok": False, "reason": "no_result"}
        return {"ok": False, "reason": "task_timeout"}

    # ---------- 审计 ----------
    def _sanitize_params(self, params):
        out = {}
        for k, v in (params or {}).items():
            if any(s in str(k).lower() for s in self._SENSITIVE_KEYS):
                out[k] = "***"
            else:
                out[k] = v
        return out

    def _audit_log(self, device_id, operation, actor, risk, status, params,
                   task_id=None, result=None):
        entry = {
            "ts": _now_str(),
            "device_id": device_id,
            "operation": operation,
            "actor": actor,
            "risk": risk,
            "risk_name": RiskLevel.name(risk),
            "status": status,
            "params": self._sanitize_params(params),
            "task_id": task_id,
            "result": result,
        }
        self._log.append(entry)
        self._log = self._log[-1000:]
        self._dirty = True

    def get_audit(self, limit=50):
        return self._log[-limit:][::-1]

    def _notify(self, text, level="info"):
        """通知出口：写审计 + 记日志（V15/V16/V17 事件通知用）"""
        logger.info("[V13] 通知(%s): %s" % (level, text))
        try:
            self._audit_log("*", "*notify*", "system", RiskLevel.LOW,
                            "notify", {"text": text[:200]})
        except Exception:
            pass

    # ---------- 落盘 / 关闭 ----------
    def flush(self):
        try:
            dm.save("gateway_audit", {
                "log": self._log[-1000:],
                "tickets": [{k: v for k, v in t.items() if k != "_event"}
                            for t in self._tickets.values()],
            })
            dm.flush_all()
            self._dirty = False
        except Exception as e:
            logger.warning("[V13] 网关审计落盘失败: %s" % e)

    def shutdown(self):
        try:
            self.flush()
        except Exception:
            pass
        self._dispatcher.shutdown()


# ---------- 环境感知中枢（十九） ----------
class EnvironmentHub:
    """环境感知中枢：汇总设备/事件/状态，供人格层与 Agent 使用"""

    def __init__(self, dmgr):
        self._dm = dmgr
        self._notes = []
        self._dirty = False

    def note_event(self, text):
        self._notes.append({"ts": _now_str(), "text": text})
        self._notes = self._notes[-50:]
        self._dirty = True

    def build_awareness_summary(self):
        online = [d.name for d in self._dm.all_devices() if d.online and d.enabled]
        offline = [d.name for d in self._dm.all_devices() if not d.online or not d.enabled]
        lines = ["【环境感知】"]
        lines.append("在线设备：%s" % ("、".join(online) if online else "无"))
        lines.append("离线/禁用：%s" % ("、".join(offline) if offline else "无"))
        if self._notes:
            lines.append("最近事件：%s" % "；".join(n["text"] for n in self._notes[-3:]))
        return "\n".join(lines)

    def flush(self):
        try:
            dm.save("environment_awareness_data", {"notes": self._notes})
            dm.flush_all()
            self._dirty = False
        except Exception as e:
            logger.warning("[V13] 环境感知落盘失败: %s" % e)


# ---------- 感知闭环（十五） ----------
class PerceptionLoop:
    """看→理解→决定→执行→再看 闭环：对设备执行操作并验证"""

    def __init__(self, gateway):
        self._gw = gateway

    def run_cycle(self, device_id, operation, params=None, actor="agent",
                  verify_operation=None, verify_params=None):
        params = params or {}
        r = self._gw.execute(device_id, operation, params, actor=actor)
        verified = False
        if r.get("ok"):
            if verify_operation:
                vr = self._gw.execute(device_id, verify_operation,
                                      verify_params or {}, actor=actor)
                verified = bool(vr.get("ok"))
            else:
                verified = True
        return {"result": r, "verified": verified,
                "observed_at": _now_str()}


# ---------- Riku Node 适配器（十七） ----------
class RikuNode:
    """Riku Node 节点规范：心跳 / 能力 / 任务 / 结果 / 错误 / 自动重连"""

    def __init__(self, node_id, endpoint="", token="", kind="generic"):
        self.node_id = node_id
        self.endpoint = (endpoint or "").rstrip("/")
        self.token = token
        self.kind = kind
        self.connected = False
        self.last_heartbeat = 0
        self.capabilities = []
        self._fail_streak = 0

    def ping(self):
        if not self.endpoint:
            self.connected = False
            return False
        try:
            resp = requests.get(self.endpoint + "/ping", timeout=5,
                                headers={"Authorization": "Bearer " + self.token}
                                if self.token else {})
            if resp.status_code != 200:
                self._fail_streak += 1
                self.connected = self._fail_streak < 3
                return self.connected
            data = resp.json()
            self.connected = True
            self._fail_streak = 0
            self.last_heartbeat = time.time()
            self.capabilities = data.get("capabilities", []) or []
            return True
        except Exception:
            self._fail_streak += 1
            self.connected = self._fail_streak < 3
            return self.connected

    def send_task(self, operation, params, timeout=None):
        if not self.endpoint:
            raise ConnectionError("node endpoint empty")
        try:
            resp = requests.post(self.endpoint + "/task", timeout=timeout or 30,
                                 json={"operation": operation, "params": params or {}},
                                 headers={"Authorization": "Bearer " + self.token}
                                 if self.token else {})
            if resp.status_code != 200:
                raise ConnectionError("node http %d" % resp.status_code)
            return resp.json()
        except requests.RequestException as e:
            raise ConnectionError("node error: %s" % e)


class AndroidRikuNode(RikuNode):
    def __init__(self, endpoint="", token=""):
        super().__init__("android", endpoint, token, "android")


class WindowsRikuNode(RikuNode):
    def __init__(self, endpoint="", token=""):
        super().__init__("windows", endpoint, token, "windows")


class VisionRikuNode(RikuNode):
    def __init__(self, endpoint="", token=""):
        super().__init__("vision", endpoint, token, "vision")


class HomeAssistantRikuNode(RikuNode):
    def __init__(self, endpoint="", token=""):
        super().__init__("homeassistant", endpoint, token, "homeassistant")


# ---------- 组装 ----------
if CONFIG["modules"].get("context.device_manager", True):
    context.device_manager = RikuDeviceManager()
else:
    context.device_manager = None

if CONFIG["modules"].get("context.tool_gateway", True) and context.device_manager is not None:
    context.tool_gateway = ToolGateway(context.device_manager, CONFIG.get("device_gateway_config", {}))
else:
    context.tool_gateway = None

if CONFIG["modules"].get("environment_awareness", True) and context.device_manager is not None:
    context.environment_hub = EnvironmentHub(context.device_manager)
else:
    context.environment_hub = None

if CONFIG["modules"].get("context.perception_loop", True) and context.tool_gateway is not None:
    context.perception_loop = PerceptionLoop(context.tool_gateway)
else:
    context.perception_loop = None

# Riku Node 注册表（十七）：配置端点即真实接入；未配置仅能力清单
riku_nodes = {
    "windows": WindowsRikuNode,
    "android": AndroidRikuNode,
    "vision": VisionRikuNode,
    "homeassistant": HomeAssistantRikuNode,
}
_riku_cfg = CONFIG.get("riku_node_config", {})
_riku_token = _riku_cfg.get("token", "rik-node-default-token-change-me")
for _ntype, _ncls in riku_nodes.items():
    try:
        _ep = ((_riku_cfg.get(_ntype) or {}).get("endpoint")) or ""
        if _ep:
            riku_nodes[_ntype] = _ncls(_ep, _riku_token)
        else:
            riku_nodes[_ntype] = _ncls("", _riku_token)
    except Exception as e:
        logger.warning("[V13] 节点 %s 初始化失败: %s" % (_ntype, e))
        riku_nodes[_ntype] = _ncls("", _riku_token)

# 本机（Windows 主机）注册为只读 Local Node（十六/十七）
if (CONFIG["modules"].get("context.tool_gateway", True)
        and CONFIG.get("device_gateway_config", {}).get("register_local_node", True)
        and context.device_manager is not None):

    def _local_system_status(params, device=None):
        import platform as _pl
        try:
            import psutil as _ps
            return {"ok": True, "hostname": _pl.node(), "platform": _pl.platform(),
                    "python": _pl.python_version(),
                    "cpu_percent": _ps.cpu_percent(interval=None),
                    "cpu_cores": _ps.cpu_count(logical=True),
                    "memory_percent": _ps.virtual_memory().percent,
                    "memory_used_mb": round(_ps.virtual_memory().used / 1048576, 1),
                    "memory_total_mb": round(_ps.virtual_memory().total / 1048576, 1),
                    "disk_used_percent": _ps.disk_usage("/").percent,
                    "boot_time": time.strftime("%Y-%m-%d %H:%M:%S",
                                               time.localtime(_ps.boot_time())),
                    "uptime_seconds": int(time.time() - _ps.boot_time())}
        except Exception as e:
            return {"ok": False, "reason": "psutil_unavailable", "error": str(e)}

    _local_host_dev = RikuDevice("local-host", DeviceType.WINDOWS,
                                 name="Windows 主机", capabilities=["system_status"],
                                 op_risks={"system_status": RiskLevel.LOW})
    _local_host_dev.local_handlers = {"system_status": _local_system_status}
    context.device_manager.register_device(_local_host_dev)
    if context.environment_hub is not None:
        context.environment_hub.note_event("本机 Local Node 已注册：Windows 主机")

# ---------- 落盘注册（主程序 flush_dirty_modules 调用） ----------
# _DIRTY_MODULES["device_registry"] 之后的代码用 try/except 包裹（依赖未迁移时跳过）
try:
    from qqagent.memory.memory_time import _DIRTY_MODULES

    _DIRTY_MODULES["device_registry"] = context.device_manager
    _DIRTY_MODULES["environment_awareness_data"] = context.environment_hub
    _DIRTY_MODULES["gateway_audit"] = context.tool_gateway

    logger.info("[V13] 现实世界能力层已加载：设备 %d 台，网关就绪，紧急停止=未启用"
                % len(context.device_manager.all_devices()))
except Exception as _mig_e:
    logger.warning(f"[迁移] tools 初始化跳过: {_mig_e}")

