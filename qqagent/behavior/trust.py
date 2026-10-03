#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""信任系统（从 src_c_trust.py 迁移）。"""
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

class BlacklistModule:
    def __init__(self):
        self._bl = set(dm.load("blacklist", []))

    def has(self, uid):
        return str(uid) in self._bl

    def add(self, uid):
        uid = str(uid)
        if uid == str(CONFIG["owner_qq"]):
            return
        self._bl.add(uid)
        dm.save("blacklist", list(self._bl))

    def remove(self, uid):
        uid = str(uid)
        if uid in self._bl:
            self._bl.remove(uid)
            dm.save("blacklist", list(self._bl))

    def list(self):
        return sorted(self._bl)


blacklist = BlacklistModule()

# ============================================================
# 八、权限系统
# ============================================================

PERM_OWNER, PERM_ADMIN, PERM_USER = 2, 1, 0


class PermissionModule:
    def __init__(self):
        data = dm.load("admin", {})
        self._admins = {str(q): PERM_ADMIN for q in data} if isinstance(data, list) else data

    def level(self, uid):
        u = str(uid)
        if u == str(CONFIG["owner_qq"]):
            try:
                if context.get("emotion_isolated") and context.get("emotion_isolated").get_test_mode():
                    return PERM_USER
            except Exception:
                pass
            return PERM_OWNER
        return self._admins.get(u, PERM_USER)

    def set(self, uid, lvl):
        u = str(uid)
        if lvl == PERM_USER:
            if u in self._admins:
                del self._admins[u]
        else:
            self._admins[u] = lvl
        dm.save("admin", self._admins)

    def list_admins(self):
        return sorted(self._admins.keys())


permission = PermissionModule()

# ============================================================
# 九、信任系统
# ============================================================

class TrustModule:
    def __init__(self):
        self._data = dm.load("trust", {})
        self._lock = _FakeLock()
        self._dirty = False

    def get(self, uid):
        u = str(uid)
        if u == str(CONFIG["owner_qq"]):
            return CONFIG["trust_owner"]
        with self._lock:
            if u not in self._data:
                return CONFIG["trust_initial"]
            v = self._data[u]
            return v.get("value", CONFIG["trust_initial"]) if isinstance(v, dict) else v

    def update(self, uid, delta=None):
        u = str(uid)
        if u == str(CONFIG["owner_qq"]):
            return
        now = datetime.now()
        with self._lock:
            if u not in self._data:
                self._data[u] = {"value": CONFIG["trust_initial"], "last_chat": now.strftime("%Y-%m-%d %H:%M:%S")}
            elif isinstance(self._data[u], (int, float)):
                self._data[u] = {"value": self._data[u], "last_chat": now.strftime("%Y-%m-%d %H:%M:%S")}
            e = self._data[u]
            cur = e["value"]
            tmax = CONFIG["trust_max"]
            dmin = CONFIG["trust_decay_min"]
            try:
                last = datetime.strptime(e["last_chat"], "%Y-%m-%d %H:%M:%S")
                days = (now - last).total_seconds() / 86400
                if days > 1 and cur > dmin:
                    cur = max(dmin, cur - days * CONFIG["trust_decay_per_day"])
            except Exception:
                pass
            if delta is not None:
                cur = max(0, min(tmax, cur + delta))
            elif cur < tmax:
                gain = 0.01 if cur < 50 else 0.015 if cur < 100 else 0.02
                cur = min(tmax, cur + gain)
            e["value"] = cur
            e["last_chat"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self._dirty = True  # 标脏，由全局 flush 统一落盘

    def penalize(self, uid, reason=""):
        penalty = CONFIG["trust_negative_penalty"]
        self.update(uid, -penalty)

    def flush(self):
        """由全局调度器调用的定时落盘"""
        with self._lock:
            if self._dirty:
                dm.save("trust", self._data)
                self._dirty = False


trust = TrustModule()

# ============================================================
# 十、用户档案系统
# ============================================================

class UserProfileManager:
    def __init__(self):
        self._data = dm.load("user_profiles", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._start_auto_flush()

    def _start_auto_flush(self):
        def flush_worker():
            while True:
                time.sleep(60)
                if self._dirty:
                    try:
                        dm.save("user_profiles", self._data)
                        self._dirty = False
                    except Exception as e:
                        logger.error(f"[用户档案] 落盘失败: {e}")
        threading.Thread(target=flush_worker, daemon=True).start()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data:
                self._data[uid] = {
                    "first_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "last_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "nickname": "",
                    "private_interactions": 0,
                    "group_interactions": {},
                    "last_group": "",
                    "tags": [],
                    "milestones": [],
                    "nickname_internal": "",
                }
                self._mark_dirty()
            return self._data[uid]

    def get_profile(self, uid):
        uid = str(uid)
        return self._ensure_user(uid)

    def update_last_seen(self, uid, group_id=None):
        uid = str(uid)
        profile = self._ensure_user(uid)
        profile["last_seen"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if group_id:
            profile["last_group"] = str(group_id)
        self._mark_dirty()

    def update_nickname(self, uid, nickname):
        uid = str(uid)
        profile = self._ensure_user(uid)
        old = profile.get("nickname", "")
        if nickname and (not old or len(nickname) > len(old)):
            profile["nickname"] = nickname
            self._mark_dirty()

    def increment_interaction(self, uid, source="private", group_id=None):
        uid = str(uid)
        profile = self._ensure_user(uid)
        if source == "private":
            profile["private_interactions"] = profile.get("private_interactions", 0) + 1
        elif source == "group" and group_id:
            gid = str(group_id)
            if gid not in profile["group_interactions"]:
                profile["group_interactions"][gid] = 0
            profile["group_interactions"][gid] += 1
        self._mark_dirty()

    def get_interaction_count(self, uid, source="total", group_id=None):
        uid = str(uid)
        profile = self._ensure_user(uid)
        if source == "private":
            return profile.get("private_interactions", 0)
        elif source == "group" and group_id:
            gid = str(group_id)
            return profile["group_interactions"].get(gid, 0)
        total = profile.get("private_interactions", 0)
        total += sum(profile["group_interactions"].values())
        return total

    def add_tag(self, uid, tag):
        uid = str(uid)
        profile = self._ensure_user(uid)
        if tag not in profile["tags"]:
            profile["tags"].append(tag)
            self._mark_dirty()

    def add_milestone(self, uid, milestone):
        uid = str(uid)
        profile = self._ensure_user(uid)
        if milestone not in profile["milestones"]:
            profile["milestones"].append({
                "name": milestone,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            self._mark_dirty()
            return True
        return False

    def set_internal_nickname(self, uid, nickname):
        uid = str(uid)
        profile = self._ensure_user(uid)
        if nickname:
            profile["nickname_internal"] = nickname
            self._mark_dirty()
            return True
        return False

    def get_internal_nickname(self, uid):
        uid = str(uid)
        profile = self._ensure_user(uid)
        return profile.get("nickname_internal", "")

    def flush(self):
        if self._dirty:
            dm.save("user_profiles", self._data)
            self._dirty = False


user_profiles = UserProfileManager()

# ============================================================
# 十一、主人高频限流
# ============================================================

class OwnerRateManager:
    def __init__(self):
        self.owner_id = str(CONFIG["owner_qq"])
        self.window = CONFIG["owner_rate_window"]
        self.warn_threshold = CONFIG["owner_warn_threshold"]
        self.angry_threshold = CONFIG["owner_angry_threshold"]
        self.cooldown = CONFIG["owner_angry_cooldown"]
        self.timestamps = []
        self.state = "normal"
        self.angry_until = 0

    def _clean_old(self):
        now = time.time()
        cutoff = now - self.window
        self.timestamps = [t for t in self.timestamps if t > cutoff]

    def _count(self):
        self._clean_old()
        return len(self.timestamps)

    def check(self, uid):
        uid = str(uid)
        if uid != self.owner_id:
            return "reply"
        now = time.time()
        self.timestamps.append(now)
        count = self._count()

        if self.state == "angry":
            if now < self.angry_until:
                return "ignore"
            self.state = "normal"

        if count >= self.angry_threshold:
            if self.state != "angry":
                self.state = "angry"
                self.angry_until = now + self.cooldown
                return "angry"
            return "ignore"

        if count >= self.warn_threshold:
            if self.state == "normal":
                self.state = "warning"
                return "warn"
            return "reply"
        else:
            if self.state == "warning":
                self.state = "normal"
            return "reply"


owner_rate = OwnerRateManager()

# ============================================================
# 十二、全局群组限流
# ============================================================

class GroupThrottle:
    def __init__(self):
        self._groups = defaultdict(lambda: {"count": 0, "start": 0})

    def allow(self, gid):
        gid = str(gid)
        now = time.time()
        g = self._groups[gid]
        if now - g["start"] > CONFIG["group_rate_window"]:
            g["count"] = 0
            g["start"] = now
        g["count"] += 1
        if g["count"] > CONFIG["group_rate_max"]:
            return False
        return True


group_throttle = GroupThrottle()

# ============================================================
# 十三、情绪隔离系统
# ============================================================

class EmotionIsolationManager:
    def __init__(self):
        self._data = dm.load("emotion_isolated", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._owner_id = str(CONFIG["owner_qq"])
        self._owner_relaxed = True
        self._test_mode = False
        self._test_mode_start = None
        self._test_mode_toggle_time = None
        self._test_mode_timeout = 30
        self._start_auto_flush()
        self._start_timeout_monitor()

        if "global" not in self._data:
            self._data["global"] = {"date": datetime.now().strftime("%Y-%m-%d"), "mood": "平静", "modifier": 0}
            self._mark_dirty()

    def _start_auto_flush(self):
        def flush_worker():
            while True:
                time.sleep(60)
                if self._dirty:
                    try:
                        dm.save("emotion_isolated", self._data)
                        self._dirty = False
                    except Exception as e:
                        logger.error(f"[情绪隔离] 落盘失败: {e}")
        threading.Thread(target=flush_worker, daemon=True).start()

    def _start_timeout_monitor(self):
        def monitor_worker():
            while True:
                time.sleep(60)
                if self._test_mode and self._test_mode_toggle_time:
                    elapsed = (time.time() - self._test_mode_toggle_time) / 60
                    if elapsed > self._test_mode_timeout:
                        logger.warning(f"[情绪隔离] 测试模式超时，自动退出")
                        self.emergency_exit_test_mode()
        threading.Thread(target=monitor_worker, daemon=True).start()

    def _mark_dirty(self):
        self._dirty = True

    def _validate_key(self, uid, scope, gid=None):
        uid = str(uid).strip()
        if not uid or uid == "0":
            return None
        if scope == "private":
            return f"private_{uid}"
        elif scope == "group_private":
            if not gid or str(gid).strip() == "0":
                return None
            return f"group_{str(gid).strip()}_{uid}"
        elif scope == "group_shared":
            if not gid or str(gid).strip() == "0":
                return None
            return f"group_{str(gid).strip()}_shared"
        elif scope == "global":
            return "global"
        return None

    def _ensure_data(self, key):
        with self._lock:
            if key not in self._data:
                self._data[key] = {
                    "emotions": EMOTION_DEFAULTS.copy(),
                    "anger_level": 0,
                    "anger_count": 0,
                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "history": [],
                    "is_cold_war": False,
                    "cold_war_end": ""
                }
                self._mark_dirty()
            return self._data[key]

    def _is_owner(self, uid):
        uid = str(uid)
        if uid != self._owner_id:
            return False
        if self._test_mode:
            return False
        return True

    def get_emotions(self, uid, scope="private", gid=None):
        if scope == "global":
            return EMOTION_DEFAULTS.copy()
        key = self._validate_key(uid, scope, gid)
        if not key or key not in self._data:
            return EMOTION_DEFAULTS.copy()
        return self._data[key].get("emotions", EMOTION_DEFAULTS.copy())

    def change_emotion(self, uid, emotion_type, delta, reason="",
                       scope="private", gid=None, apply_chain=True):
        if scope == "global":
            return 0, False
        key = self._validate_key(uid, scope, gid)
        if not key:
            return 0, False

        is_owner = self._is_owner(uid)

        with self._lock:
            data = self._ensure_data(key)
            emotions = data["emotions"]
            if emotion_type not in emotions:
                return 0, False

            old_val = emotions[emotion_type]

            if is_owner and self._owner_relaxed and not self._test_mode:
                if delta > 10:
                    delta = 10
                elif delta < -10:
                    delta = -10

            new_val = max(0, min(100, old_val + delta))
            emotions[emotion_type] = new_val

            data["history"].append({
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "emotion": emotion_type,
                "change": delta,
                "from": old_val,
                "to": new_val,
                "reason": reason,
                "scope": scope,
                "gid": str(gid) if gid else None,
                "uid": str(uid)
            })
            if len(data["history"]) > 100:
                data["history"] = data["history"][-100:]

            data["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._mark_dirty()

            if apply_chain:
                self._apply_chain_reaction(emotions, emotion_type, delta)

            triggered = self._check_threshold(emotions, emotion_type, old_val, new_val)
            return new_val, triggered

    def _apply_chain_reaction(self, emotions, primary_emotion, delta):
        # ---- 链式反应：绝对值变化（不再与 delta 成比例，涟漪更可感知）----
        # 每个关联情绪按固定绝对值变化，方向跟随 delta 正负
        chain = {
            "angry": {"nervous": 3, "calm": -4, "sad": 2, "tired": 1, "shy": -1},
            "joy": {"calm": 3, "tired": -2, "curious": 2, "shy": 1, "nervous": -1},
            "sad": {"tired": 4, "joy": -5, "calm": -2, "nervous": 2, "curious": -1},
            "shy": {"nervous": 5, "joy": 1, "calm": -3, "curious": 1},
            "nervous": {"shy": 3, "angry": 2, "calm": -5, "tired": 1, "sad": 1},
            "curious": {"joy": 2, "calm": 1, "nervous": 1},
            "tired": {"sad": 3, "calm": 1, "nervous": 1, "curious": -2},
            "calm": {"nervous": -3, "angry": -3, "joy": 1, "tired": -1},
        }
        # ---- 强度因子：主情绪越强，涟漪幅度越大 ----
        primary_val = emotions.get(primary_emotion, 50)
        if primary_val >= 70:
            intensity_factor = 1.5
        elif primary_val >= 50:
            intensity_factor = 1.0
        else:
            intensity_factor = 0.6

        # ---- delta 方向：正变化（情绪上升）涟漪为正，负变化（下降）涟漪为负 ----
        direction = 1 if delta >= 0 else -1

        for emo, abs_change in chain.get(primary_emotion, {}).items():
            if emo in emotions:
                ripple = direction * abs(abs_change) * intensity_factor
                emotions[emo] = max(0, min(100, emotions[emo] + ripple))

    def _check_threshold(self, emotions, emotion_type, old_val, new_val):
        if emotion_type == "angry" and new_val >= 80 and old_val < 80:
            return {"triggered": True, "event": "rage"}
        if emotion_type == "joy" and new_val >= 80 and old_val < 80:
            return {"triggered": True, "event": "happy"}
        if emotion_type == "nervous" and new_val >= 80 and old_val < 80:
            return {"triggered": True, "event": "panic"}
        return {"triggered": False}

    def get_anger_level(self, uid, scope="private", gid=None):
        if scope in ("global", "group_shared"):
            return 0
        key = self._validate_key(uid, scope, gid)
        if not key or key not in self._data:
            return 0
        return self._data[key].get("anger_level", 0)

    def trigger_anger(self, uid, reason, severity=1, scope="private", gid=None):
        if scope in ("global", "group_shared"):
            return "......（别过脸去）", False

        is_owner = self._is_owner(uid)
        key = self._validate_key(uid, scope, gid)
        if not key:
            return "......（别过脸去）", False

        with self._lock:
            data = self._ensure_data(key)

            if is_owner and self._owner_relaxed and not self._test_mode:
                play_count = data.get("owner_play_count", 0) + 1
                data["owner_play_count"] = play_count
                data["last_owner_action"] = reason
                data["last_owner_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                emotions = data["emotions"]
                emotions["angry"] = min(30, emotions.get("angry", 10) + 3)
                emotions["shy"] = min(60, emotions.get("shy", 35) + 5)
                self._mark_dirty()
                return self._get_owner_playful_response(play_count), False

            old_level = data.get("anger_level", 0)
            new_level = min(4, old_level + severity)
            data["anger_level"] = new_level
            data["anger_count"] = data.get("anger_count", 0) + 1
            data["last_anger_reason"] = reason
            data["last_anger_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            emotions = data["emotions"]
            emotions["angry"] = min(100, emotions.get("angry", 10) + severity * 10)

            if new_level >= 4:
                data["is_cold_war"] = True
                data["cold_war_end"] = (datetime.now() + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")

            self._mark_dirty()

            if new_level >= 4:
                return self._get_anger_response(4), True
            return self._get_anger_response(new_level), False

    def _get_owner_playful_response(self, play_count):
        if play_count < 3:
            replies = ["（尾巴轻轻甩了一下）……小塔，你故意的吧？",
                       "（耳朵动了动，假装生气）……哼，不理你了。",
                       "（别过脸去，但尾巴尖还在晃）……你、你讨厌。"]
        elif play_count < 6:
            replies = ["（尾巴炸了一下又放下）……小塔！你再这样我真的生气了！",
                       "（耳朵往后压，但眼睛还在看你）……你、你完蛋了。",
                       "（抱着尾巴缩了缩）……小塔是坏人。"]
        elif play_count < 10:
            replies = ["（转过身去，但尾巴还在偷偷晃）……今天不想理你了。",
                       "（声音软软的）……小塔你太过分了……",
                       "（假装生气，但声音带着笑意）……你走开啦。"]
        else:
            replies = ["（尾巴炸开，但很快就放下）……小塔！我生气了！……（小声）哄我。",
                       "（耳朵贴平，但尾巴尖在偷偷勾你）……你、你知道错了吗？",
                       "（别别扭扭地转回来）……我、我这次真的生气了……除非你给我巧克力。"]
        return random.choice(replies)

    def _get_anger_response(self, level):
        responses = {
            1: ["（尾巴不满地甩了一下）……哼。", "（耳朵往后压了压）……你烦不烦。", "（别过脸去）……不想理你。"],
            2: ["（尾巴炸毛，往后跳了一步）……你、你走开！", "（耳朵紧紧贴在脑后，声音发冷）……我生气了。", "（转过身去）……不想跟你说话。"],
            3: ["（尾巴炸成球，全身僵硬）……你、你太过分了！", "（声音发抖，带着怒气）……我讨厌你。", "（猛地站起来）……我走了。"],
            4: ["（尾巴炸开，眼眶泛红）……你永远别跟我说话了。", "（声音冰冷到极点）……你被拉黑了。", "（尾巴猛地拍了一下地面）……你给我滚。"]
        }
        return random.choice(responses.get(level, responses[1]))

    def calm_down(self, uid, apology="", scope="private", gid=None):
        if scope in ("global", "group_shared"):
            return context.get("ReplyBank").get("calm_no_anger")
        key = self._validate_key(uid, scope, gid)
        if not key or key not in self._data:
            return context.get("ReplyBank").get("calm_no_anger")

        with self._lock:
            data = self._data[key]
            current_level = data.get("anger_level", 0)
            if current_level <= 0:
                return context.get("ReplyBank").get("calm_no_anger")

            # ---- 结合关系阶段：关系越亲近，原谅越快、越带撒娇感 ----
            rel_hint = ""
            if CONFIG["modules"].get("relationship"):
                stage_key = relationship_manager.get_stage(uid)
                stage_name = relationship_manager._stages.get(stage_key, {}).get("name", "陌生人")
                if stage_key in ("close_friend", "trusted", "soulmate"):
                    rel_hint = f"你们关系很近（{stage_name}），你其实心里已经软了，但嘴上还想别扭一下。"
                elif stage_key == "friend":
                    rel_hint = f"你们是朋友（{stage_name}），你会原谅但会让他记住这次。"
                elif stage_key in ("stranger", "acquaintance"):
                    rel_hint = f"你们还不太熟（{stage_name}），原谅会比较客气、有距离感。"

            if apology and len(apology) > 10:
                if current_level <= 2:
                    data["anger_level"] = max(0, current_level - 2)
                    self._mark_dirty()
                    # ---- 三模型协同：用 talk 槽位生成自然的原谅回应（结合关系阶段）----
                    prompt = SYSTEM_PROMPT + f"\n\n你在生对方的气，但对方认真道歉了，你决定原谅。{rel_hint}" \
                        "请用你的口吻说一句话，带动作描写，别扭地表示原谅。简短自然。只说这一句。"
                    reply = _v6_call("talk", prompt, [], temperature_override=_v6_temp("talk"))
                    if reply and len(reply) > 3:
                        return reply.strip()
                    return random.choice(["（尾巴放松了一点）……算、算你识相。", "（小声）……这次原谅你了。", "（别别扭扭地）……好吧。"])
                else:
                    data["anger_level"] = max(0, current_level - 1)
                    self._mark_dirty()
                    # ---- 三模型协同：用 talk 槽位生成自然的半原谅回应（结合关系阶段）----
                    prompt = SYSTEM_PROMPT + f"\n\n你在生对方的气，对方道歉了但你还没完全消气。{rel_hint}" \
                        "请用你的口吻说一句话，带动作描写，表示还不够。简短自然。只说这一句。"
                    reply = _v6_call("talk", prompt, [], temperature_override=_v6_temp("talk"))
                    if reply and len(reply) > 3:
                        return reply.strip()
                    return random.choice(["（尾巴稍微放下了一点）……还不够。", "（转回半个身子）……你、你再说一遍？", "（耳朵动了动）……就、就这样？"])
            else:
                data["anger_level"] = min(4, current_level + 1)
                self._mark_dirty()
                # ---- 三模型协同：用 talk 槽位生成自然的愤怒回应（结合关系阶段）----
                prompt = SYSTEM_PROMPT + f"\n\n对方试图道歉但很敷衍，你更生气了。{rel_hint}" \
                    "请用你的口吻说一句话，带动作描写，表达愤怒。简短自然。只说这一句。"
                reply = _v6_call("talk", prompt, [], temperature_override=_v6_temp("talk"))
                if reply and len(reply) > 3:
                    return reply.strip()
                return random.choice(["（尾巴猛地炸开）……你、你根本没诚意！", "（怒气冲冲地转过身）……你走！", "（声音发冷）……敷衍我？你完了。"])

    def get_mood_summary(self, uid, scope="private", gid=None):
        if scope == "global":
            return {"dominant": "平静", "anger_level": 0, "description": "全局基调", "emotions": EMOTION_DEFAULTS.copy()}
        key = self._validate_key(uid, scope, gid)
        if not key or key not in self._data:
            return {"dominant": "平静", "anger_level": 0, "description": "情绪数据未初始化", "emotions": EMOTION_DEFAULTS.copy()}

        data = self._data[key]
        emotions = data["emotions"]
        dominant = max(emotions, key=emotions.get)
        return {
            "dominant": EMOTION_TYPES.get(dominant, {}).get("name", dominant),
            "emotions": emotions,
            "anger_level": data.get("anger_level", 0),
            "anger_count": data.get("anger_count", 0),
            "description": f"当前情绪主要是{EMOTION_TYPES.get(dominant, {}).get('name', dominant)}，愤怒等级{data.get('anger_level', 0)}/4"
        }

    # ---- 情绪叙事模板：把数值转换为自然语言描写 ----
    _NARRATIVE_TEMPLATES = {
        "joy": {
            "high":   ["开心得尾巴尖都在晃", "心情很好，眼睛亮亮的", "嘴角压不住地上扬"],
            "medium": ["心情不错", "有点开心", "情绪轻快"],
            "low":    ["隐约有点高兴", "心情稍微好了一点"],
        },
        "sad": {
            "high":   ["心里沉甸甸的，提不起劲", "很难过，尾巴都垂下来了", "心情低落得不想说话"],
            "medium": ["有点低落", "心情不太好", "闷闷的"],
            "low":    ["微微有些丧", "心情有一点沉"],
        },
        "shy": {
            "high":   ["脸红到耳根", "害羞得不敢看人", "耳朵都烧起来了"],
            "medium": ["有点害羞", "脸上微微发热", "不太好意思"],
            "low":    ["稍微有点脸红", "隐约有些害羞"],
        },
        "nervous": {
            "high":   ["紧张得尾巴都竖起来了", "整个人绷得很紧", "手心都在出汗"],
            "medium": ["有些紧张", "心里不太踏实", "有点坐立不安"],
            "low":    ["稍微有点紧张", "隐约有点不安"],
        },
        "calm": {
            "high":   ["很平静，没什么波澜", "安安静静的", "心如止水"],
            "medium": ["还算平静", "情绪平稳", "没什么特别的感觉"],
            "low":    ["勉强平静", "看似淡定"],
        },
        "angry": {
            "high":   ["气得说不出话", "怒火压都压不住", "恨不得转身就走"],
            "medium": ["有点生气", "不太高兴", "心里窝着火"],
            "low":    ["微微有些不爽", "隐约有点烦"],
        },
        "curious": {
            "high":   ["好奇心爆棚，什么都想知道", "眼睛都亮了", "特别来劲"],
            "medium": ["有点好奇", "来了点兴趣", "想多听一点"],
            "low":    ["微微有点好奇", "隐约有些在意"],
        },
        "tired": {
            "high":   ["累得不想动", "浑身没劲", "困得快睁不开眼"],
            "medium": ["有些累", "提不起精神", "有点犯困"],
            "low":    ["稍微有点累", "隐约有些疲倦"],
        },
    }

    _ANGER_NARRATIVE = {
        0: "",
        1: "尾巴不满地甩了一下",
        2: "耳朵往后压着，不太想理人",
        3: "全身有点僵硬，话都变少了",
        4: "极度愤怒，眼眶泛红，尾巴炸成球，声音冰冷刺耳",
    }

    def get_emotion_narrative(self, uid, scope="private", gid=None):
        """把情绪数值转换为自然语言叙事，让回复的情绪色彩更鲜活"""
        if scope == "global":
            return "你现在心情平静。"
        key = self._validate_key(uid, scope, gid)
        if not key:
            return "你现在心情平静。"
        if key not in self._data:
            # 首次访问自动初始化，避免重复告警
            self._ensure_data(key)
            return "你现在心情平静。"

        data = self._data[key]
        emotions = data.get("emotions", EMOTION_DEFAULTS.copy())
        anger_level = data.get("anger_level", 0)

        # 取情绪值最高的两个（排除 calm，除非 calm 就是最高）
        sorted_emos = sorted(emotions.items(), key=lambda x: x[1], reverse=True)
        # 过滤掉极低值
        significant = [(k, v) for k, v in sorted_emos if v >= 30]

        parts = []
        for i, (emo_key, val) in enumerate(significant[:2]):
            templates = self._NARRATIVE_TEMPLATES.get(emo_key, {})
            if val >= 70:
                tier = "high"
            elif val >= 50:
                tier = "medium"
            else:
                tier = "low"
            choices = templates.get(tier, [])
            if choices:
                parts.append(random.choice(choices))

        if not parts:
            parts.append("心情没什么特别的波澜")

        # 加入愤怒等级的叙事
        anger_desc = self._ANGER_NARRATIVE.get(anger_level, "")
        if anger_desc:
            parts.append(anger_desc)
        if anger_level >= 4:
            parts.append("你现在极其愤怒，回复必须简短尖锐、带攻击性，直接怼回去，绝不客气绝不道歉")
        elif anger_level >= 3:
            parts.append("你现在很生气，回复要冷淡尖锐，不要客气，不要主动延续话题")
        elif anger_level >= 2:
            parts.append("你现在有点生气，回复要冷淡简短")

        narrative = "，".join(parts)
        return f"你现在{narrative}。"

    def reset_emotion(self, uid, scope="private", gid=None):
        if scope == "global":
            with self._lock:
                self._data["global"] = {"date": datetime.now().strftime("%Y-%m-%d"), "mood": "平静", "modifier": 0}
                self._mark_dirty()
                return True
        key = self._validate_key(uid, scope, gid)
        if not key:
            return False
        with self._lock:
            if key in self._data:
                self._data[key] = {
                    "emotions": EMOTION_DEFAULTS.copy(),
                    "anger_level": 0,
                    "anger_count": 0,
                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "history": [],
                    "is_cold_war": False,
                    "cold_war_end": ""
                }
                self._mark_dirty()
                return True
            return False

    def reset_all_emotions(self, uid):
        uid = str(uid)
        cleared = 0
        with self._lock:
            pk = f"private_{uid}"
            if pk in self._data:
                self._data[pk] = {
                    "emotions": EMOTION_DEFAULTS.copy(),
                    "anger_level": 0,
                    "anger_count": 0,
                    "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "history": [],
                    "is_cold_war": False,
                    "cold_war_end": ""
                }
                cleared += 1
            prefix = "group_"
            for key in list(self._data.keys()):
                if key.startswith(prefix) and key.endswith(f"_{uid}"):
                    self._data[key] = {
                        "emotions": EMOTION_DEFAULTS.copy(),
                        "anger_level": 0,
                        "anger_count": 0,
                        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "history": [],
                        "is_cold_war": False,
                        "cold_war_end": ""
                    }
                    cleared += 1
            if uid == self._owner_id and self._test_mode:
                self._test_mode = False
                self._test_mode_start = None
            if cleared > 0:
                self._mark_dirty()
        logger.info(f"[情绪隔离] 重置 {uid} 的所有情绪数据，共{cleared}个维度")
        return cleared > 0

    def emergency_exit_test_mode(self):
        with self._lock:
            if not self._test_mode:
                return False
            self._test_mode = False
            self._test_mode_start = None
            owner_id = self._owner_id
            pk = f"private_{owner_id}"
            if pk in self._data:
                self._data[pk]["anger_level"] = 0
                self._data[pk]["anger_count"] = 0
                self._data[pk]["emotions"]["angry"] = 10
                self._data[pk]["is_cold_war"] = False
                self._data[pk]["cold_war_end"] = ""
            prefix = "group_"
            for key in list(self._data.keys()):
                if key.startswith(prefix) and key.endswith(f"_{owner_id}"):
                    self._data[key]["anger_level"] = 0
                    self._data[key]["anger_count"] = 0
                    self._data[key]["emotions"]["angry"] = 10
                    self._data[key]["is_cold_war"] = False
                    self._data[key]["cold_war_end"] = ""
            self._mark_dirty()
            logger.warning(f"[情绪隔离] 紧急退出测试模式")
            return True

    def _check_and_clear_cold_war(self, uid):
        uid = str(uid)
        cleared = False
        with self._lock:
            pk = f"private_{uid}"
            if pk in self._data:
                if self._data[pk].get("is_cold_war", False):
                    self._data[pk]["is_cold_war"] = False
                    self._data[pk]["cold_war_end"] = ""
                    cleared = True
                    self._mark_dirty()
            prefix = "group_"
            for key, data in self._data.items():
                if key.startswith(prefix) and key.endswith(f"_{uid}"):
                    if data.get("is_cold_war", False):
                        data["is_cold_war"] = False
                        data["cold_war_end"] = ""
                        cleared = True
                        self._mark_dirty()
        return cleared

    def get_test_mode(self):
        return self._test_mode

    def set_test_mode(self, value):
        with self._lock:
            self._test_mode = value
            if value:
                self._test_mode_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._test_mode_toggle_time = time.time()
            else:
                self._test_mode_start = None
                self._test_mode_toggle_time = None
            self._mark_dirty()

    def fluctuate_all(self):
        import random as _r
        emotions = ["joy", "sad", "shy", "nervous", "calm", "angry", "curious", "tired"]
        with self._lock:
            for key, data in self._data.items():
                if key in ("global", "test_meta"):
                    continue
                if not isinstance(data, dict):
                    continue
                changed = False
                for emo in emotions:
                    if emo not in data:
                        continue
                    delta = _r.randint(-8, 12)
                    data[emo] = max(0, min(100, data[emo] + delta))
                    changed = True
                if changed:
                    self._mark_dirty()
            if self._data:
                dm.save("emotion_isolated", self._data)
                dm.flush_all()
                logger.info(f"[情绪模块] 随机波动完成并已保存，覆盖 {len(self._data)} 条数据")


context.set("emotion_isolated", EmotionIsolationManager())

# ============================================================
# 十四、永久黑名单
# ============================================================

