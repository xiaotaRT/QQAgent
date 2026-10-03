#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""V15 智能家居系统（从 src_m_reality.py 迁移）。

HomeAssistantClient + SmartHomeAgent + homeassistant 设备注册。
"""
import os, sys, json, time, logging, threading, re
from qqagent.core import CONFIG, logger, context
from qqagent.tools.reality import RikuDevice, DeviceType, RiskLevel
from qqagent.core.commands import register_command

if CONFIG["modules"].get("smart_home", True) and context.device_manager is not None:
    _HA_CFG = CONFIG.get("homeassistant_config", {})
    _HA_BASE = (_HA_CFG.get("base_url") or "").rstrip("/")
    _HA_TOKEN = _HA_CFG.get("token") or ""

    # ---------- 实体映射（能力/风险） ----------
    _HA_DOMAIN_INFO = {
        "light":     {"risk": RiskLevel.MEDIUM, "control": "控制灯光"},
        "switch":    {"risk": RiskLevel.MEDIUM, "control": "控制开关"},
        "climate":   {"risk": RiskLevel.MEDIUM, "control": "控制空调/温控"},
        "fan":       {"risk": RiskLevel.MEDIUM, "control": "控制风扇"},
        "cover":     {"risk": RiskLevel.MEDIUM, "control": "控制窗帘/门窗"},
        "media_player": {"risk": RiskLevel.MEDIUM, "control": "控制媒体播放"},
        "lock":      {"risk": RiskLevel.CRITICAL, "control": "控制门锁"},
        "scene":     {"risk": RiskLevel.HIGH,    "control": "执行场景"},
        "automation": {"risk": RiskLevel.HIGH,   "control": "运行自动化"},
        "sensor":    {"risk": RiskLevel.LOW,     "control": "读取传感器"},
        "binary_sensor": {"risk": RiskLevel.LOW, "control": "读取状态"},
        "device_tracker": {"risk": RiskLevel.LOW, "control": "读取位置"},
        "camera":    {"risk": RiskLevel.HIGH,    "control": "查看摄像头"},
    }

    class HomeAssistantClient:
        """Home Assistant REST 客户端（真实实现，Bearer Token 鉴权）"""

        def __init__(self, base_url="", token=""):
            self.base_url = (base_url or "").rstrip("/")
            self.token = token or ""
            self.available = bool(self.base_url and self.token)

        def _get(self, path, timeout=15):
            if not self.available:
                return {"ok": False, "reason": "ha_not_configured"}
            try:
                resp = requests.get(self.base_url + path, timeout=timeout,
                                    headers={"Authorization": "Bearer " + self.token})
                if resp.status_code != 200:
                    return {"ok": False, "reason": "ha_http_%d" % resp.status_code}
                return {"ok": True, "data": resp.json()}
            except Exception as e:
                return {"ok": False, "reason": "ha_error", "error": str(e)}

        def _post(self, path, payload=None, timeout=15):
            if not self.available:
                return {"ok": False, "reason": "ha_not_configured"}
            try:
                resp = requests.post(self.base_url + path, json=payload or {},
                                     timeout=timeout,
                                     headers={"Authorization": "Bearer " + self.token})
                if resp.status_code != 200:
                    return {"ok": False, "reason": "ha_http_%d" % resp.status_code,
                            "body": resp.text[:300]}
                return {"ok": True, "data": resp.json()}
            except Exception as e:
                return {"ok": False, "reason": "ha_error", "error": str(e)}

        def get_states(self):
            r = self._get("/api/states")
            return r.get("data", []) if r.get("ok") else r

        def get_state(self, entity_id):
            return self._get("/api/states/%s" % entity_id)

        def get_config(self):
            return self._get("/api/config")

        def call_service(self, domain, service, entity_id=None, data=None):
            payload = dict(data or {})
            if entity_id:
                payload["entity_id"] = entity_id
            return self._post("/api/services/%s/%s" % (domain, service), payload)

    _ha_client = HomeAssistantClient(_HA_BASE, _HA_TOKEN)

    # ---------- 房间 / 名称映射 ----------
    _HA_ROOMS = _HA_CFG.get("rooms") or {}
    _HA_NAMES = _HA_CFG.get("entity_names") or {}

    def _ha_room_of(entity_id, friendly=None):
        name = friendly or entity_id
        for room, pats in _HA_ROOMS.items():
            for p in pats:
                if entity_id.startswith(p) or (p in name):
                    return room
        return "未分组"

    def _ha_display(entity):
        eid = entity.get("entity_id", "?")
        friendly = entity.get("attributes", {}).get("friendly_name") \
            or _HA_NAMES.get(eid, eid)
        return {"entity_id": eid, "name": friendly, "state": entity.get("state"),
                "room": _ha_room_of(eid, friendly)}

    class SmartHomeAgent:
        """智能家居 Agent（查询/控制/场景/自动化/异常通知/状态记忆）"""

        def __init__(self, client, cfg):
            self.client = client
            self.cfg = cfg
            self._state_cache = {}
            self._anomaly_log = []
            self._watcher_alive = False
            self._watcher = None
            self._rules = cfg.get("anomaly_rules") or [
                {"name": "低电量", "entity_prefix": "sensor.*battery*",
                 "attr": "battery_level", "threshold": 20, "below": True,
                 "notify": True},
                {"name": "温度越界", "entity_prefix": "sensor.*temperature*",
                 "attr": None, "min": 5, "max": 32, "notify": True},
                {"name": "湿度偏高", "entity_prefix": "sensor.*humidity*",
                 "attr": None, "min": None, "max": 75, "notify": False},
            ]
            self._load_cache()

        def _load_cache(self):
            try:
                data = dm.load("smart_home_state", {})
                self._state_cache = data.get("cache", {})
                self._anomaly_log = data.get("anomalies", [])
            except Exception:
                pass

        def _persist(self):
            try:
                dm.save("smart_home_state",
                        {"cache": self._state_cache,
                         "anomalies": self._anomaly_log[-50:]})
                dm.flush_all()
            except Exception as e:
                logger.warning("[V15] 智能家居状态记忆落盘失败: %s" % e)

        # ---------- 查询 ----------
        def query_device(self, params, device=None):
            eid = params.get("entity_id") or params.get("device", "")
            if not eid:
                return {"ok": False, "reason": "missing_entity"}
            r = self.client.get_state(eid)
            if not r.get("ok"):
                return r
            ent = r["data"]
            disp = _ha_display(ent)
            self._update_cache(ent)
            return {"ok": True, "device": disp}

        def query_room(self, params, device=None):
            room = params.get("room", "")
            states = self.client.get_states()
            if not isinstance(states, list):
                return states
            rows, count = [], {"on": 0, "total": 0}
            for ent in states:
                disp = _ha_display(ent)
                if room and disp["room"] != room:
                    continue
                rows.append(disp)
                count["total"] += 1
                if disp["state"] in ("on", "open", "home", "unlocked"):
                    count["on"] += 1
            return {"ok": True, "room": room, "summary": count, "devices": rows[:60]}

        def query_env(self, params, device=None):
            states = self.client.get_states()
            if not isinstance(states, list):
                return states
            env = {}
            for ent in states:
                eid = ent.get("entity_id", "")
                state = ent.get("state")
                if eid.startswith("sensor.temperature") or "temperature" in eid:
                    env.setdefault("temperature", []).append((eid, state))
                elif eid.startswith("sensor.humidity") or "humidity" in eid:
                    env.setdefault("humidity", []).append((eid, state))
                elif "illuminance" in eid:
                    env.setdefault("illuminance", []).append((eid, state))
                elif "air" in eid and "quality" in eid or eid.startswith("sensor.pm"):
                    env.setdefault("air_quality", []).append((eid, state))
            return {"ok": True, "environment": env}

        # ---------- 控制 ----------
        def control_device(self, params, device=None):
            eid = params.get("entity_id") or params.get("device", "")
            action = params.get("action") or params.get("operation", "turn_on")
            if not eid:
                return {"ok": False, "reason": "missing_entity"}
            r = self.client.get_state(eid)
            if not r.get("ok"):
                return r
            domain = eid.split(".")[0]
            info = _HA_DOMAIN_INFO.get(domain,
                                       {"risk": RiskLevel.MEDIUM, "control": "控制"})
            action = str(action).lower()
            if action == "turn_on":
                sr = self.client.call_service(domain, "turn_on", eid)
            elif action == "turn_off":
                sr = self.client.call_service(domain, "turn_off", eid)
            elif action == "toggle":
                sr = self.client.call_service(domain, "toggle", eid)
            elif action == "open":
                sr = self.client.call_service(domain, "open_cover", eid)
            elif action == "close":
                sr = self.client.call_service(domain, "close_cover", eid)
            elif action == "lock":
                sr = self.client.call_service(domain, "lock", eid)
            elif action == "unlock":
                sr = self.client.call_service(domain, "unlock", eid)
            elif action == "set_temperature":
                sr = self.client.call_service(
                    domain, "set_temperature", eid,
                    {"temperature": params.get("temperature", 24)})
            elif action == "set_brightness":
                sr = self.client.call_service(
                    domain, "turn_on", eid,
                    {"brightness_pct": params.get("brightness", 50)})
            elif action == "set_volume":
                sr = self.client.call_service(
                    domain, "volume_set", eid,
                    {"volume_level": float(params.get("volume", 0.5))})
            else:
                sr = self.client.call_service(domain, action, eid,
                                              params.get("data") or {})
            if not sr.get("ok"):
                return sr
            # 操作后状态确认（十四）/ 失败后重试
            verified = self._verify_state(eid, params.get("expected_state"))
            return {"ok": True, "entity": eid, "action": action,
                    "risk": info["risk"], "verified": verified}

        def _verify_state(self, eid, expected=None, tries=6):
            for _ in range(tries):
                time.sleep(1.0)
                r = self.client.get_state(eid)
                if r.get("ok"):
                    st = r["data"].get("state")
                    if expected and st == expected:
                        return True
                    if not expected and st not in ("unavailable", "unknown"):
                        return True
            return False

        def execute_scene(self, params, device=None):
            name = params.get("name") or params.get("scene", "")
            if not name:
                return {"ok": False, "reason": "missing_scene"}
            sr = self.client.call_service("scene", "turn_on", data={"scene": name})
            return sr if not sr.get("ok") else {"ok": True, "scene": name}

        def run_automation(self, params, device=None):
            eid = params.get("entity_id") or params.get("automation", "")
            if not eid:
                return {"ok": False, "reason": "missing_automation"}
            sr = self.client.call_service("automation", "turn_on", eid)
            return sr if not sr.get("ok") else {"ok": True, "automation": eid}

        # ---------- 自动化建议（规则引擎） ----------
        def automation_suggestion(self, params, device=None):
            states = self.client.get_states()
            if not isinstance(states, list):
                return states
            hour = datetime.now().hour
            suggestions = []
            for ent in states:
                eid = ent.get("entity_id", "")
                state = ent.get("state")
                if eid.startswith("sensor.temperature") and \
                        state not in ("unavailable", "unknown"):
                    try:
                        t = float(state)
                        if t < 16 and hour in (6, 7, 18, 19, 20, 21, 22, 23):
                            suggestions.append("室温 %.1f°C 较低，建议开启暖气或空调" % t)
                        if t > 30:
                            suggestions.append("室温 %.1f°C 较高，建议开空调/风扇" % t)
                    except Exception:
                        pass
                if eid.startswith("sensor.humidity") and \
                        state not in ("unavailable", "unknown"):
                    try:
                        h = float(state)
                        if h > 75:
                            suggestions.append("湿度 %.0f%% 偏高，建议开启除湿" % h)
                        if h < 25:
                            suggestions.append("湿度 %.0f%% 偏低，建议加湿" % h)
                    except Exception:
                        pass
                if eid.startswith("binary_sensor.door") and state == "on":
                    suggestions.append("门窗传感器触发中，注意检查门窗")
            return {"ok": True, "time": datetime.now().strftime("%H:%M"),
                    "suggestions": suggestions[:10]}

        # ---------- 异常状态通知（后台 watcher） ----------
        def _update_cache(self, ent):
            eid = ent.get("entity_id")
            state = ent.get("state")
            prev = self._state_cache.get(eid, {})
            if prev.get("state") != state:
                self._state_cache[eid] = {"state": state,
                                          "last_change": time.time(),
                                          "ts": _now_str()}
            else:
                self._state_cache.setdefault(eid, {}).setdefault("ts", _now_str())
            self._dirty = True

        def _watch_once(self):
            states = self.client.get_states()
            if not isinstance(states, list):
                return
            for ent in states:
                eid = ent.get("entity_id", "")
                state = ent.get("state")
                attrs = ent.get("attributes", {})
                for rule in self._rules:
                    if not _rule_matches(rule, eid, state, attrs):
                        continue
                    key = "%s|%s" % (eid, rule.get("name"))
                    if key in {a.get("key") for a in self._anomaly_log[-10:]}:
                        continue
                    ev = {"key": key, "ts": _now_str(), "entity": eid,
                          "rule": rule.get("name"), "state": state}
                    self._anomaly_log.append(ev)
                    if rule.get("notify", True):
                        try:
                            context.tool_gateway._notify("[智能家居异常] %s：%s（%s）" %
                                                 (rule.get("name"), eid, state))
                        except Exception:
                            pass
            self._persist()

        def start_watcher(self):
            if self._watcher_alive or not self.client.available:
                return
            self._watcher_alive = True

            def _loop():
                while self._watcher_alive:
                    try:
                        self._watch_once()
                    except Exception as e:
                        logger.warning("[V15] 智能家居异常监控失败: %s" % e)
                    time.sleep(int(self.cfg.get("anomaly_interval", 120)))

            self._watcher = threading.Thread(target=_loop, daemon=True)
            self._watcher.start()

        def stop_watcher(self):
            self._watcher_alive = False

        def flush(self):
            self._persist()

        def get_status(self):
            return {"available": self.client.available,
                    "base_url": self.client.base_url,
                    "rules": len(self._rules),
                    "anomalies": len(self._anomaly_log),
                    "cached_entities": len(self._state_cache)}

    def _rule_matches(rule, eid, state, attrs):
        import fnmatch as _fm
        pref = rule.get("entity_prefix", "")
        if pref and not _fm.fnmatch(eid, pref):
            return False
        attr = rule.get("attr")
        val = None
        if attr:
            val = attrs.get(attr)
            if val is None:
                return False
            try:
                val = float(val)
            except Exception:
                return False
        else:
            try:
                val = float(state)
            except Exception:
                return False
        if rule.get("below") and val <= rule.get("threshold", 0):
            return True
        if rule.get("min") is not None and val < rule["min"]:
            return True
        if rule.get("max") is not None and val > rule["max"]:
            return True
        return False

    # ---------- 接入统一 Device Manager / Tool Gateway ----------
    _smart_home_agent = SmartHomeAgent(_ha_client, _HA_CFG)
    _ha_device = RikuDevice("homeassistant", DeviceType.HOMEASSISTANT,
                            name="Home Assistant", capabilities=[
                                "query_device", "query_room", "query_env",
                                "control_device", "execute_scene",
                                "run_automation", "automation_suggestion"],
                            op_risks={
                                "query_device": RiskLevel.LOW,
                                "query_room": RiskLevel.LOW,
                                "query_env": RiskLevel.LOW,
                                "automation_suggestion": RiskLevel.LOW,
                                "control_device": RiskLevel.MEDIUM,
                                "execute_scene": RiskLevel.HIGH,
                                "run_automation": RiskLevel.HIGH,
                            },
                            forbidden_ops=["execute_scene", "run_automation"])
    _ha_device.local_handlers = {
        "query_device": _smart_home_agent.query_device,
        "query_room": _smart_home_agent.query_room,
        "query_env": _smart_home_agent.query_env,
        "control_device": _smart_home_agent.control_device,
        "execute_scene": _smart_home_agent.execute_scene,
        "run_automation": _smart_home_agent.run_automation,
        "automation_suggestion": _smart_home_agent.automation_suggestion,
    }
    _ha_device.agent_deny_all = True          # 家居默认禁止 Agent 自主（安全）
    context.device_manager.register_device(_ha_device)   # 始终注册：未配置时能力清单仍可见


@register_command("private", ["智能家居状态", "家居状态"], perm_required=2,
                  owner_only=True)
def _cmd_ha_status(msg):
    st = context.get("_smart_home_agent").get_status()
    lines = [
        "【智能家居层】",
        "HA 连接: %s" % ("已接入 " + st["base_url"] if st["available"] else "未配置"),
        "异常规则: %d 条 · 异常记录: %d 条 · 状态缓存: %d 个实体"
        % (st["rules"], st["anomalies"], st["cached_entities"]),
        "实体域: %s" % "、".join(sorted(_HA_DOMAIN_INFO)),
        "",
        "Zigbee/Matter/ESPHome 适配：",
    ]
    for k, v in _HA_ECO_ADAPTERS.items():
        lines.append("· %s：%s" % (k, v["接入"]))
    return "\n".join(lines)

# ============================================================
# 四十二·C V16、视觉系统
# ============================================================
# 对应【里克现实世界能力建设 TODO】第五~九章：
#   摄像头接入（RTSP/HTTP/WebRTC/USB/网络/本机）/ 在线检测 / 视频流 /
#   抽帧 / 按需截图 / 关键帧 / 画面变化检测 / 访问权限 / 启用禁用 / 日志 / 隐私
