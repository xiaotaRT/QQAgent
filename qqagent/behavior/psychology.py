#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""心理学核心（情绪隔离等）（从 src_i_psych.py 迁移）。"""
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
from qqagent.behavior.behavior_learn import (
    ConflictDetectionModule, DarkDiaryModule, DraftModule, GroupBystanderModule,
    HumanLikeStateModule, InnerMonologueModule, JealousyModule, LateNightModule,
    NewbieAdaptationModule, ObserveModule, OldAccountModule, PoutingModule, PraiseModule,
    RegretModule, SafeDistanceModule, SecretCollectionModule, SelfContradictionModule,
    SilentModeModule, SocialEnergyModule, TriggerRecallModule, NicknameSystem,
)

class RickDiaryModule:
    """里克日记"""
    def __init__(self):
        self._data = dm.load("rick_diary_data", {})
        self._dirty = False
        self._lock = _FakeLock()
        self._start_daily_diary()

    def _start_daily_diary(self):
        def diary_worker():
            while True:
                time.sleep(86400)
                self.generate_daily()
        threading.Thread(target=diary_worker, daemon=True).start()

    def _mark_dirty(self):
        self._dirty = True

    def generate_daily(self):
        today = datetime.now().strftime("%Y-%m-%d")
        with self._lock:
            if "last_generate" in self._data and self._data["last_generate"] == today:
                return
            all_tags = []
            for uid, data in tag_extractor._data.items():
                all_tags.extend(data.get("tags", []))
            if not all_tags:
                all_tags = ["平静", "日常"]
            keywords = random.sample(all_tags, min(CONFIG["diary"]["max_keywords"], len(all_tags)))

            # 三模型协同——用 diary 槽位生成日记
            context_parts = [f"今天的关键词：{'、'.join(keywords)}"]
            if CONFIG["modules"]["memory"]:
                for uid_str in list(user_profiles._data.keys())[:3]:
                    tops = memory_weight.get_top_memories(int(uid_str), limit=2, min_weight=0.2)
                    for m in tops:
                        context_parts.append(f"- {m['content'][:30]}")
            if CONFIG["modules"].get("human_like_state"):
                for uid_str in list(user_profiles._data.keys())[:1]:
                    try:
                        context_parts.append(human_like_state.get_prompt(int(uid_str), None)[:100])
                    except Exception:
                        pass

            prompt = SYSTEM_PROMPT + "\n\n" + "\n".join(context_parts) + "\n\n" + \
                "请写一段今天的日记。第一人称，你的口吻，简短真实，100字以内。只写日记内容。"

            diary = _v6_call("diary", prompt, [])
            if not diary or len(diary) < 5:
                # 回退模板
                diary = f"今天，我注意到{'、'.join(keywords)}。"
                diary += random.choice([
                    "这些事在我心里转了很久。",
                    "它们像书签一样夹在今天的页码里。",
                    "虽然都是小事，但对我来说很重。",
                ])

            self._data[today] = {
                "diary": diary,
                "keywords": keywords,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            self._data["last_generate"] = today
            self._mark_dirty()
            logger.info(f"[里克日记] 生成今日日记（三模型协同）")

    def get_diary(self, date=None):
        if date:
            return self._data.get(date, {}).get("diary", "那天没有写日记。")
        today = datetime.now().strftime("%Y-%m-%d")
        if today in self._data:
            return self._data[today]["diary"]
        items = [(k, v) for k, v in self._data.items() if k not in ["last_generate"]]
        if items:
            items.sort(reverse=True)
            return items[0][1]["diary"]
        return "今天还没写日记。"


# ============================================================
# 三十九、V5.5 新功能模块实例化与统一落盘
# ============================================================

pouting_module = PoutingModule()
dark_diary = DarkDiaryModule()
silent_mode = SilentModeModule()
inner_monologue = InnerMonologueModule()
regret_module = RegretModule()
social_energy = SocialEnergyModule()
safe_distance = SafeDistanceModule()
observe_module = ObserveModule()
group_bystander = GroupBystanderModule()
newbie_module = NewbieAdaptationModule()
late_night = LateNightModule()
secret_collection = SecretCollectionModule()
nickname_system = NicknameSystem()
old_account = OldAccountModule()
trigger_recall = TriggerRecallModule()
conflict_detection = ConflictDetectionModule()
jealousy_module = JealousyModule()
praise_module = PraiseModule()
draft_module = DraftModule()
self_contradiction = SelfContradictionModule()
human_like_state = HumanLikeStateModule()
rick_diary = RickDiaryModule()

# ============================================================
# 四十、新增角色深度模块群
# ============================================================

class MemoryFragmentModule:
    """回忆碎片——Rick 在对话中随机"走神"，突然想起过去的事"""
    def __init__(self):
        self._data = dm.load("memory_fragment_data", {"last_trigger": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def maybe_trigger(self, uid, current_message):
        """随机触发回忆碎片，返回回忆文本或 None"""
        if not CONFIG["modules"].get("memory_fragment", True):
            return None
        uid_s = str(uid)
        now = time.time()
        with self._lock:
            last = self._data.get("last_trigger", {}).get(uid_s, 0)
            # 至少间隔 30 分钟才能再次触发
            if now - last < 1800:
                return None
            # 5% 概率触发
            if random.random() > 0.05:
                return None
            self._data.setdefault("last_trigger", {})[uid_s] = now
            self._mark_dirty()

        # 从高权重记忆中随机选一条
        tops = memory_weight.get_top_memories(uid, limit=10, min_weight=0.3)
        if not tops:
            return None
        chosen = random.choice(tops[:5])
        templates = [
            f"......突然想起你之前说的「{chosen['content'][:30]}」。",
            f"（走神了一下）......你上次提到的那件事，我一直在想。",
            f"......说起来，你还记得你说过「{chosen['content'][:20]}」吗。",
        ]
        return random.choice(templates)

    def get_prompt_fragment(self, uid):
        """提供回忆碎片给 Talk 作为可选上下文"""
        fragment = self.maybe_trigger(uid, "")
        if fragment:
            return f"\n【走神的回忆】{fragment}\n"
        return ""


class RapportModule:
    """默契度系统——追踪用户和 Rick 在情绪/话题上的重合度"""
    def __init__(self):
        self._data = dm.load("rapport_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure(self, uid):
        uid_s = str(uid)
        if uid_s not in self._data:
            self._data[uid_s] = {"rapport": 0, "matches": 0, "total": 0, "history": []}
        return self._data[uid_s]

    def record_match(self, uid, user_emotion, rick_emotion, topic_match=False):
        """记录一次情绪/话题匹配，更新默契值"""
        if not CONFIG["modules"].get("rapport", True):
            return
        with self._lock:
            d = self._ensure(uid)
            d["total"] += 1
            # 情绪方向一致算匹配
            emotion_match = False
            positive = {"开心", "兴奋", "感动", "喜欢"}
            negative = {"难过", "生气", "害怕", "疲惫"}
            if (user_emotion in positive and rick_emotion in positive) or \
               (user_emotion in negative and rick_emotion in negative):
                emotion_match = True
            if emotion_match or topic_match:
                d["matches"] += 1
                d["rapport"] = min(1000, d["rapport"] + 3)
            else:
                d["rapport"] = max(0, d["rapport"] - 1)
            # 保留最近 20 条历史
            d["history"].append({"time": _time_str(), "emotion_match": emotion_match, "topic_match": topic_match})
            d["history"] = d["history"][-20:]
            self._mark_dirty()

    def get_rapport(self, uid):
        with self._lock:
            d = self._ensure(uid)
            return d["rapport"]

    def get_prompt_fragment(self, uid):
        """提供默契度信息给 Talk"""
        if not CONFIG["modules"].get("rapport", True):
            return ""
        r = self.get_rapport(uid)
        if r < 100:
            return ""
        elif r < 300:
            return f"\n【默契度】{r}/1000——你们有时候能想到一块去。\n"
        elif r < 600:
            return f"\n【默契度】{r}/1000——你们挺有默契的，经常不约而同。\n"
        else:
            return f"\n【默契度】{r}/1000——你们默契极高，几乎不用说就能懂彼此。\n"


# ============================================================
# V12 天气系统 — WeatherSystem
# 天气模拟 + 情绪影响 + 季节性情感障碍 + 雷雨天恐惧
# 无天气API时自动模拟，有联网搜索时可接入真实天气
# ============================================================

class WeatherSystem:
    """
    天气系统——模拟天气对里克的影响

    核心机制：
      - 天气模拟：基于季节和概率生成天气（无API时）
      - 情绪影响：不同天气影响心境基线
      - 季节性情感障碍（SAD）：秋冬日照少 → 抑郁倾向加重
      - 雷雨天恐惧：雷雨触发恐惧反应，裹被子里
      - 感官敏感性：强光/大风/高温影响舒适度
      - 行为影响：坏天气更不想出门，好天气偶尔想开窗

    天气类型：
      sunny / cloudy / overcast / rain / heavy_rain / thunderstorm
      / snow / fog / windy / hot / cold
    """

    # 天气对情绪的影响（心境偏移 -100~100）
    WEATHER_MOOD_EFFECT = {
        'sunny':       {'mood': 5,  'energy': 10, 'anxiety': -5},   # 晴天心情稍好
        'cloudy':      {'mood': 0,  'energy': 0,  'anxiety': 0},    # 多云无影响
        'overcast':    {'mood': -8, 'energy': -5, 'anxiety': 5},    # 阴天低落
        'rain':        {'mood': -10, 'energy': -8, 'anxiety': 8},   # 下雨更低落
        'heavy_rain':  {'mood': -15, 'energy': -15, 'anxiety': 15}, # 大雨烦躁
        'thunderstorm': {'mood': -25, 'energy': -20, 'anxiety': 40}, # 雷雨恐惧
        'snow':        {'mood': -5, 'energy': -10, 'anxiety': 5},   # 下雪平静但冷
        'fog':         {'mood': -5, 'energy': -5, 'anxiety': 10},   # 雾天压抑
        'windy':       {'mood': -8, 'energy': -5, 'anxiety': 12},   # 大风不安
        'hot':         {'mood': -10, 'energy': -15, 'anxiety': 5},  # 热得烦躁
        'cold':        {'mood': -12, 'energy': -10, 'anxiety': 5},  # 冷得不想动
    }

    # 季节天气概率分布
    SEASON_WEATHER_PROBS = {
        '春天': {'sunny': 0.25, 'cloudy': 0.25, 'overcast': 0.15, 'rain': 0.2,
                 'windy': 0.1, 'fog': 0.05},
        '夏天': {'sunny': 0.4, 'cloudy': 0.2, 'hot': 0.15, 'heavy_rain': 0.1,
                 'thunderstorm': 0.1, 'rain': 0.05},
        '秋天': {'sunny': 0.2, 'cloudy': 0.25, 'overcast': 0.2, 'rain': 0.2,
                 'windy': 0.1, 'fog': 0.05},
        '冬天': {'sunny': 0.15, 'cloudy': 0.2, 'overcast': 0.25, 'cold': 0.2,
                 'snow': 0.1, 'fog': 0.1},
    }

    # 里克对不同天气的内心感受
    WEATHER_FEELINGS = {
        'sunny': [
            "今天阳光好像挺好的……从窗帘缝里能看到。",
            "外面很亮。",
            "天气不错，但也没想出门。",
        ],
        'cloudy': [
            "天阴阴的。",
            "今天是阴天。",
            "看不到太阳。",
        ],
        'overcast': [
            "天灰蒙蒙的，有点闷。",
            "今天很暗，像要下雨。",
            "天空很低落。",
        ],
        'rain': [
            "在下雨。",
            "外面雨声淅淅沥沥的。",
            "又下雨了，不想动。",
        ],
        'heavy_rain': [
            "雨好大……",
            "外面雨声很吵。",
            "下暴雨了，还好不用出门。",
        ],
        'thunderstorm': [
            "……打雷了。（尾巴紧紧夹着）",
            "雷雨天……我讨厌打雷。",
            "（裹在被子里，声音很小）外面在打雷。",
        ],
        'snow': [
            "下雪了……",
            "外面白茫茫的。",
            "好冷，不想出被窝。",
        ],
        'fog': [
            "外面雾蒙蒙的。",
            "今天雾很大，什么都看不清。",
            "雾天……让人有点闷。",
        ],
        'windy': [
            "风很大，窗户在响。",
            "外面在刮风。",
            "风吹得树枝乱晃。",
        ],
        'hot': [
            "好热……不想动。",
            "今天太热了。",
            "黏糊糊的，不舒服。",
        ],
        'cold': [
            "好冷……想缩在被子里。",
            "手都冻僵了。",
            "今天特别冷。",
        ],
    }

    def __init__(self, dm=None):
        self.dm = dm
        self._data = dm.load("weather_data", {
            'current_weather': 'cloudy',
            'last_update': 0,
            'weather_history': [],
            'sad_level': 0,  # 季节性情感障碍程度 0~100
            'thunder_fear_level': 0,  # 雷雨恐惧水平 0~100
        })
        self._dirty = False

        # 城市（用于天气API查询，可选）
        self._city = "新克洛维斯市"  # 虚构城市，用模拟天气

    def _mark_dirty(self):
        self._dirty = True

    def _get_season(self):
        """获取当前季节"""
        month = datetime.now().month
        if 3 <= month <= 5:
            return '春天'
        elif 6 <= month <= 8:
            return '夏天'
        elif 9 <= month <= 11:
            return '秋天'
        else:
            return '冬天'

    def update_weather(self, force=False):
        """
        更新天气（每4小时更新一次，避免频繁变动）

        有联网搜索时尝试获取真实天气，否则模拟
        """
        now = time.time()
        if not force and now - self._data.get('last_update', 0) < 4 * 3600:
            return self._data['current_weather']

        season = self._get_season()
        weather = self._simulate_weather(season)

        self._data['current_weather'] = weather
        self._data['last_update'] = now
        self._data['weather_history'].append({
            'time': now,
            'weather': weather
        })
        if len(self._data['weather_history']) > 50:
            self._data['weather_history'] = self._data['weather_history'][-50:]

        # 更新SAD水平（秋冬季阴天多 → SAD加重）
        self._update_sad_level()

        # 更新雷雨恐惧
        if weather == 'thunderstorm':
            self._data['thunder_fear_level'] = min(100, self._data.get('thunder_fear_level', 0) + 30)
        else:
            # 恐惧逐渐消退
            self._data['thunder_fear_level'] = max(0, self._data.get('thunder_fear_level', 0) - 10)

        self._mark_dirty()
        logger.info(f"[天气] 当前天气：{weather}")
        return weather

    def _simulate_weather(self, season):
        """根据季节概率模拟天气"""
        probs = self.SEASON_WEATHER_PROBS.get(season, self.SEASON_WEATHER_PROBS['春天'])
        weather_types = list(probs.keys())
        probabilities = list(probs.values())

        # 归一化
        total = sum(probabilities)
        probabilities = [p / total for p in probabilities]

        r = random.random()
        cumulative = 0
        for w, p in zip(weather_types, probabilities):
            cumulative += p
            if r <= cumulative:
                return w
        return weather_types[0]

    def _update_sad_level(self):
        """
        更新季节性情感障碍（SAD）水平

        秋冬季 + 连续阴天 → SAD加重
        春夏季 + 晴天 → SAD缓解
        """
        season = self._get_season()
        current_weather = self._data.get('current_weather', 'cloudy')
        sad = self._data.get('sad_level', 0)

        # 季节基础影响
        season_effects = {
            '冬天': 25,
            '秋天': 15,
            '春天': 5,
            '夏天': 0,
        }
        base_sad = season_effects.get(season, 10)

        # 近期天气影响
        recent = self._data.get('weather_history', [])[-5:]
        dark_days = sum(1 for w in recent if w['weather'] in ['overcast', 'rain', 'heavy_rain', 'fog'])
        weather_effect = dark_days * 3

        # 渐变调整（不会突变）
        target_sad = base_sad + weather_effect
        sad = sad + (target_sad - sad) * 0.1  # 每天调整10%
        self._data['sad_level'] = max(0, min(80, sad))

    def get_current_weather(self):
        """获取当前天气"""
        return self._data.get('current_weather', 'cloudy')

    def get_mood_effect(self):
        """获取天气对心境的影响"""
        weather = self._data.get('current_weather', 'cloudy')
        effect = self.WEATHER_MOOD_EFFECT.get(weather, {})

        # SAD 附加影响
        sad = self._data.get('sad_level', 0)
        sad_mood_penalty = -sad * 0.3

        result = {
            'mood': effect.get('mood', 0) + sad_mood_penalty,
            'energy': effect.get('energy', 0),
            'anxiety': effect.get('anxiety', 0),
            'sad_level': sad,
        }
        return result

    def get_thunder_fear(self):
        """获取雷雨恐惧水平"""
        return self._data.get('thunder_fear_level', 0)

    def is_thunderstorm(self):
        """是否是雷雨天"""
        return self._data.get('current_weather', '') == 'thunderstorm'

    def get_weather_feeling(self):
        """获取里克对当前天气的内心感受（自然语言）"""
        weather = self._data.get('current_weather', 'cloudy')
        feelings = self.WEATHER_FEELINGS.get(weather, ["今天天气一般。"])
        return random.choice(feelings)

    def get_prompt_fragment(self):
        """生成用于 prompt 的天气信息"""
        if not CONFIG["modules"].get("weather_system", True):
            return ""

        weather = self.update_weather()
        mood_effect = self.get_mood_effect()
        thunder_fear = self.get_thunder_fear()
        sad_level = mood_effect.get('sad_level', 0)

        parts = []
        parts.append(f"【天气】{self._weather_name(weather)}。")

        # 天气感受
        feeling = self.get_weather_feeling()
        parts.append(f"你对天气的感受：{feeling}")

        # 雷雨恐惧
        if thunder_fear > 30:
            if weather == 'thunderstorm':
                parts.append("你很害怕打雷，现在有点紧张，想躲到被子里去。")
            else:
                parts.append("你还有点害怕，刚才的雷声让你不安。")

        # SAD
        if sad_level > 20:
            parts.append(f"你最近有点季节性情绪低落（程度：{sad_level:.0f}/100），阴天让你更不想动。")

        # 行为倾向
        if weather in ['rain', 'heavy_rain', 'thunderstorm', 'snow', 'cold']:
            parts.append("这种天气你完全不想出门，只想缩在房间里。")
        elif weather == 'sunny':
            parts.append("天气不错，但你也没有特别想出门的冲动——最多拉开窗帘透透气。")

        return "\n".join(parts) + "\n"

    def _weather_name(self, weather_code):
        """天气代码转中文名称"""
        names = {
            'sunny': '晴天',
            'cloudy': '多云',
            'overcast': '阴天',
            'rain': '小雨',
            'heavy_rain': '大雨',
            'thunderstorm': '雷暴雨',
            'snow': '下雪',
            'fog': '雾天',
            'windy': '大风',
            'hot': '酷热',
            'cold': '寒冷',
        }
        return names.get(weather_code, weather_code)

    def should_mention_weather(self, uid):
        """是否在聊天中提起天气（低概率）"""
        if not CONFIG["modules"].get("weather_system", True):
            return None

        weather = self._data.get('current_weather', 'cloudy')

        # 雷雨天高概率提起（因为恐惧）
        if weather == 'thunderstorm':
            if random.random() < 0.4:
                return self.get_weather_feeling()
        # 特殊天气中概率
        elif weather in ['heavy_rain', 'snow', 'hot']:
            if random.random() < 0.1:
                return self.get_weather_feeling()
        # 普通天气低概率
        else:
            if random.random() < 0.03:
                return self.get_weather_feeling()

        return None


class SeasonAwarenessModule:
    """季节/节日感知——Rick 自然地感知时间流逝和节日"""
    SEASONS = {
        12: "冬天", 1: "冬天", 2: "冬天",
        3: "春天", 4: "春天", 5: "春天",
        6: "夏天", 7: "夏天", 8: "夏天",
        9: "秋天", 10: "秋天", 11: "秋天",
    }
    HOLIDAYS = {
        "01-01": "元旦", "02-14": "情人节", "03-08": "妇女节",
        "05-01": "劳动节", "06-01": "儿童节", "10-01": "国庆节",
        "12-25": "圣诞节", "12-31": "跨年夜",
    }
    SEASON_NOTES = {
        "春天": ["外面应该暖和起来了", "花粉好像多了", "最近天亮得早了"],
        "夏天": ["好热......不想动", "蝉好吵", "想吃冰的"],
        "秋天": ["风变凉了", "叶子开始掉了", "天黑得越来越早了"],
        "冬天": ["好冷......想缩在被子里", "手都冻僵了", "想喝热的东西"],
    }

    def __init__(self):
        self._last_holiday = ""

    def get_prompt_fragment(self):
        """提供季节和节日信息给 Talk"""
        if not CONFIG["modules"].get("season_awareness", True):
            return ""
        now = datetime.now()
        season = self.SEASONS.get(now.month, "")
        parts = [f"\n【季节感知】现在是{season}。"]
        note = random.choice(self.SEASON_NOTES.get(season, [""]))
        if note:
            parts.append(f"最近的感觉：{note}")
        # 节日检测
        today_key = now.strftime("%m-%d")
        holiday = self.HOLIDAYS.get(today_key, "")
        if holiday:
            parts.append(f"今天是{holiday}。")
        # 节日前夜（跨年/圣诞前一周）
        if today_key == "12-24":
            parts.append("明天是圣诞节。")
        parts.append("\n")
        return " ".join(parts) + "\n"

    def check_holiday_greeting(self):
        """检查今天是否需要发节日祝福，返回 (uid_list, holiday_name) 或 None"""
        now = datetime.now()
        today_key = now.strftime("%m-%d")
        holiday = self.HOLIDAYS.get(today_key, "")
        if not holiday:
            return None
        # 每个节日只触发一次
        if self._last_holiday == f"{holiday}:{today_key}":
            return None
        self._last_holiday = f"{holiday}:{today_key}"
        return holiday


class CreativeWritingModule:
    """创意写作——Rick 偶尔写短诗、碎碎念，分享给亲密用户"""
    def __init__(self):
        self._data = dm.load("creative_writing_data", {"works": [], "last_write": ""})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def maybe_write(self):
        """深夜 + 高社交能量 + 情绪波动时触发创作"""
        if not CONFIG["modules"].get("creative_writing", True):
            return None
        now = datetime.now()
        # 只在深夜（0~4点）触发
        if not (0 <= now.hour <= 4):
            return None
        today = now.strftime("%Y-%m-%d")
        with self._lock:
            if self._data.get("last_write") == today:
                return None
            self._data["last_write"] = today
            self._mark_dirty()

        prompt = (
            "你是里克。深夜了，你有些睡不着，想写点东西。\n"
            "写一段短文（50字以内），可以是碎碎念、短诗、或者日记片段。\n"
            "只输出文字内容，不要解释。用你的性格——内敛、克制、偶尔柔软。"
        )
        raw = _v6_call("agent", prompt, [{"role": "user", "content": "写点什么吧。"}],
                       temperature_override=0.9)
        if not raw or len(raw) > 200:
            return None
        work = {"content": raw.strip(), "time": _time_str(), "date": today}
        with self._lock:
            self._data.setdefault("works", []).append(work)
            self._data["works"] = self._data["works"][-50:]  # 保留最近 50 篇
            self._mark_dirty()
        logger.info(f"[创意写作] 里克写了一篇: {raw[:30]}...")
        return work

    def get_recent_work(self):
        """获取最近一篇作品，用于主动分享"""
        with self._lock:
            works = self._data.get("works", [])
            return works[-1] if works else None


class EmotionContagionModule:
    """情绪传染——一个用户的强烈情绪影响 Rick 对其他用户的状态"""
    def __init__(self):
        self._data = dm.load("emotion_contagion_data", {"global_mood": "平静", "global_intensity": 0, "last_update": ""})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def absorb(self, uid, user_emotion, intensity):
        """吸收一个用户的强烈情绪，影响全局情绪基线"""
        if not CONFIG["modules"].get("emotion_contagion", True):
            return
        strong = {"生气", "难过", "害怕", "开心", "兴奋"}
        if user_emotion not in strong:
            return
        with self._lock:
            cur_intensity = self._data.get("global_intensity", 0)
            # 强情绪加权吸收
            intensity_map = {"低": 1, "中": 2, "高": 3}
            absorbed = intensity_map.get(intensity, 1)
            if user_emotion in {"生气", "难过", "害怕"}:
                # 负面情绪传染更强
                absorbed *= 2
            cur_intensity = min(10, cur_intensity + absorbed)
            # 全局情绪偏向用户情绪
            self._data["global_mood"] = user_emotion
            self._data["global_intensity"] = cur_intensity
            self._data["last_update"] = _time_str()
            self._mark_dirty()

    def decay(self):
        """全局情绪自然衰减——由后台调度器定期调用"""
        with self._lock:
            self._data["global_intensity"] = max(0, self._data.get("global_intensity", 0) - 1)
            if self._data["global_intensity"] == 0:
                self._data["global_mood"] = "平静"
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """提供全局情绪偏移给 Talk"""
        if not CONFIG["modules"].get("emotion_contagion", True):
            return ""
        with self._lock:
            mood = self._data.get("global_mood", "平静")
            intensity = self._data.get("global_intensity", 0)
        if intensity < 3:
            return ""
        return f"\n【全局情绪残留】你因为之前和其他人的对话，心情还有点{mood}（强度{intensity}/10），这会微妙影响你现在的状态。\n"


class RelationshipGraphModule:
    """用户关系图谱——在群里追踪谁和谁互动多，Rick 形成'看法'"""
    def __init__(self):
        self._data = dm.load("relationship_graph_data", {"groups": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def record_interaction(self, gid, uid1, uid2):
        """记录群内两个用户同时出现的次数"""
        if not CONFIG["modules"].get("relationship_graph", True):
            return
        if not gid or uid1 == uid2:
            return
        gid_s = str(gid)
        with self._lock:
            groups = self._data.setdefault("groups", {})
            if gid_s not in groups:
                groups[gid_s] = {}
            pair = tuple(sorted([str(uid1), str(uid2)]))
            pair_key = f"{pair[0]}-{pair[1]}"
            groups[gid_s][pair_key] = groups[gid_s].get(pair_key, 0) + 1
            self._mark_dirty()

    def get_close_pairs(self, gid, threshold=5):
        """获取群内互动频繁的用户对"""
        gid_s = str(gid)
        with self._lock:
            pairs = self._data.get("groups", {}).get(gid_s, {})
        return [(k, v) for k, v in pairs.items() if v >= threshold]

    def get_prompt_fragment(self, gid):
        """提供群内关系观察给 Talk"""
        if not CONFIG["modules"].get("relationship_graph", True) or not gid:
            return ""
        close = self.get_close_pairs(gid, threshold=5)
        if not close:
            return ""
        # 取互动最多的一对
        top = max(close, key=lambda x: x[1])
        return f"\n【群内观察】你注意到{top[0]}这两个人经常一起出现。\n"


class RuminationModule:
    """反刍思维——深夜 Rick 独处时回味白天的对话，产生新想法"""
    def __init__(self):
        self._data = dm.load("rumination_data", {"last_rumination": "", "pending": []})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def add_high_intensity_chat(self, uid, message, emotion):
        """记录白天的强情绪对话，供夜间反刍"""
        if not CONFIG["modules"].get("rumination", True):
            return
        strong = {"生气", "难过", "开心", "兴奋", "害怕"}
        if emotion not in strong:
            return
        with self._lock:
            self._data.setdefault("pending", []).append({
                "uid": str(uid),
                "message": message[:100],
                "emotion": emotion,
                "time": _time_str(),
            })
            self._data["pending"] = self._data["pending"][-20:]  # 保留最近 20 条
            self._mark_dirty()

    def ruminate(self):
        """深夜反刍——生成对白天对话的回顾，返回 (uid, rumination_text) 或 None"""
        if not CONFIG["modules"].get("rumination", True):
            return None
        now = datetime.now()
        # 只在凌晨 1~4 点触发
        if not (1 <= now.hour <= 4):
            return None
        today = now.strftime("%Y-%m-%d")
        with self._lock:
            if self._data.get("last_rumination") == today:
                return None
            pending = self._data.get("pending", [])
            if not pending:
                return None
            self._data["last_rumination"] = today
            self._mark_dirty()

        # 选最近的一条强情绪对话
        target = pending[-1]
        prompt = (
            f"现在是深夜，你在回味今天和某个人的对话。\n"
            f"对方当时说：「{target['message']}」，情绪是{target['emotion']}。\n"
            f"用一两句话写下你现在的想法。用你的性格——内敛、反思、偶尔柔软。\n"
            f"只输出想法内容。"
        )
        raw = _v6_call("agent", prompt, [{"role": "user", "content": "想想今天的事。"}],
                       temperature_override=0.7)
        if not raw:
            return None
        result = {"uid": target["uid"], "text": raw.strip(), "time": _time_str()}
        logger.info(f"[反刍思维] 里克在回味和 {target['uid']} 的对话: {raw[:30]}...")
        return result

    def get_prompt_fragment(self, uid):
        """如果有昨夜的反刍结果，注入 prompt"""
        if not CONFIG["modules"].get("rumination", True):
            return ""
        # 检查是否有针对该用户的未发送反刍
        # 这个在 Agent 层面处理主动发送
        return ""


class SurpriseGiftModule:
    """惊喜礼物——Rick 偶尔给亲密用户'送'虚拟的东西"""
    GIFT_POOL = [
        ("贝壳", "在海边捡了个好看的贝壳"),
        ("树叶", "路上看到一片形状很奇怪的树叶"),
        ("短句", "刚才突然想到一句话"),
        ("石头", "河边捡了块很光滑的石头"),
        ("画", "随手画了个什么东西"),
        ("星星", "今晚的星星很亮，想让你也看看"),
    ]
    def __init__(self):
        self._data = dm.load("surprise_gift_data", {"gifts": {}, "last_gift": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def maybe_gift(self, uid):
        """检查是否该给用户送惊喜礼物，返回礼物描述或 None"""
        if not CONFIG["modules"].get("surprise_gift", True):
            return None
        uid_s = str(uid)
        now = time.time()
        with self._lock:
            last = self._data.get("last_gift", {}).get(uid_s, 0)
            # 至少间隔 3 天
            if now - last < 259200:
                return None
            # 信任值不够不送
            t = trust.get(uid)
            if t < 350:
                return None
            # 10% 概率
            if random.random() > 0.10:
                return None
            self._data.setdefault("last_gift", {})[uid_s] = now
            self._mark_dirty()

        gift_type, gift_desc = random.choice(self.GIFT_POOL)
        # 如果是短句，用 LLM 生成
        if gift_type == "短句":
            raw = _v6_call("agent",
                "你突然想到一句很短的话，想送给一个信任的人。只输出这句话，15字以内。",
                [{"role": "user", "content": "说句话吧。"}],
                temperature_override=0.9)
            gift_desc = raw.strip() if raw else gift_desc

        gift = {"type": gift_type, "desc": gift_desc, "time": _time_str()}
        with self._lock:
            self._data.setdefault("gifts", {}).setdefault(uid_s, []).append(gift)
            self._data["gifts"][uid_s] = self._data["gifts"][uid_s][-20:]
            self._mark_dirty()
        logger.info(f"[惊喜礼物] 给 {uid_s} 送了: {gift_type}")
        return gift


class ToneEnhancerModule:
    """语气词系统增强——根据情绪状态自动调整标点和语气词"""
    def __init__(self):
        pass

    def enhance(self, reply, uid, gid=None):
        """根据 Rick 当前情绪状态增强语气"""
        if not CONFIG["modules"].get("tone_enhancer", True):
            return reply
        if not reply or len(reply) > 500:
            return reply

        # 获取 Rick 当前情绪
        if gid:
            mood = context.get("emotion_isolated").get_mood_summary(uid, "group_private", gid)
        else:
            mood = context.get("emotion_isolated").get_mood_summary(uid, "private")
        dominant = mood.get("dominant", "平静")

        # 根据情绪调整语气
        if dominant in {"生气", "烦躁"}:
            # 生气时多一些省略号，短句
            if "。" in reply and "..." not in reply:
                reply = reply.replace("。", "......", 1)
        elif dominant in {"难过", "委屈"}:
            # 难过时多一些停顿
            if len(reply) > 20 and "（" not in reply:
                reply = reply[:10] + "......" + reply[10:]
        elif dominant in {"开心", "兴奋"}:
            # 开心时偶尔加感叹号
            if random.random() < 0.3 and reply.rstrip().endswith("。"):
                reply = reply.rstrip()[:-1] + "！"
        elif dominant in {"紧张", "害怕"}:
            # 紧张时加破折号
            if len(reply) > 15 and "——" not in reply:
                reply = reply[:5] + "——" + reply[5:]
        elif dominant in {"疲惫", "累"}:
            # 疲惫时缩短回复
            if len(reply) > 50:
                idx = reply.find("。", 30)
                if idx > 0:
                    reply = reply[:idx + 1]

        return reply


# ============================================================
# V6.6 角色深度增强模块
# ============================================================

class HabitTrackerModule:
    """习惯追踪——Rick 观察用户的行为规律，自然融入对话"""

    def __init__(self):
        self._data = dm.load("habit_tracker_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure(self, uid):
        uid_s = str(uid)
        if uid_s not in self._data:
            self._data[uid_s] = {
                "hour_hist": [0] * 24,       # 各小时上线次数
                "topic_freq": {},             # 话题频率
                "avg_reply_len": 0,           # 平均回复长度
                "reply_count": 0,             # 回复总数
                "avg_interval": 0,            # 平均回复间隔（秒）
                "last_interval": 0,           # 上次回复间隔
                "last_seen": 0,               # 上次活跃时间戳
                "total_sessions": 0,          # 总会话数
            }
        return self._data[uid_s]

    def record_activity(self, uid, message=""):
        """记录用户活跃——上线时间、话题、回复长度"""
        if not CONFIG["modules"].get("habit_tracker", True):
            return
        uid_s = str(uid)
        now = time.time()
        with self._lock:
            d = self._ensure(uid)
            # 小时直方图
            hour = datetime.now().hour
            d["hour_hist"][hour] += 1
            # 回复间隔
            if d["last_seen"] > 0:
                interval = now - d["last_seen"]
                d["last_interval"] = interval
                if d["avg_interval"] == 0:
                    d["avg_interval"] = interval
                else:
                    d["avg_interval"] = d["avg_interval"] * 0.8 + interval * 0.2
            d["last_seen"] = now
            d["reply_count"] += 1
            # 回复长度
            msg_len = len(message)
            d["avg_reply_len"] = d["avg_reply_len"] * 0.8 + msg_len * 0.2
            # 话题频率（取前 10 个字符作为话题摘要）
            if message:
                topic = re.sub(r'[。！？，、\s]+', ' ', message)[:10]
                if topic:
                    d["topic_freq"][topic] = d["topic_freq"].get(topic, 0) + 1
                    d["topic_freq"] = dict(sorted(d["topic_freq"].items(), key=lambda x: -x[1])[:20])
            self._mark_dirty()

    def get_active_hours(self, uid):
        """获取用户最活跃的时段"""
        uid_s = str(uid)
        with self._lock:
            d = self._data.get(uid_s, {})
            hist = d.get("hour_hist", [0] * 24)
        top_hours = sorted(range(24), key=lambda h: hist[h], reverse=True)[:3]
        return [h for h in top_hours if hist[h] >= 3]

    def get_top_topics(self, uid, limit=3):
        """获取用户最常聊的话题"""
        uid_s = str(uid)
        with self._lock:
            d = self._data.get(uid_s, {})
            topics = d.get("topic_freq", {})
        return list(topics.items())[:limit]

    def get_prompt_fragment(self, uid):
        """提供用户习惯观察给 Talk"""
        if not CONFIG["modules"].get("habit_tracker", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            d = self._data.get(uid_s, {})
        if d.get("reply_count", 0) < 5:
            return ""
        parts = []
        active_hours = self.get_active_hours(uid)
        if active_hours:
            hour_strs = [f"{h}点" for h in active_hours]
            parts.append(f"对方通常在{'、'.join(hour_strs)}左右出现")
        avg_len = d.get("avg_reply_len", 0)
        if avg_len > 30:
            parts.append("对方说话偏长，喜欢详细表达")
        elif avg_len > 0 and avg_len < 10:
            parts.append("对方说话很简短")
        top_topics = self.get_top_topics(uid, limit=2)
        if top_topics:
            parts.append(f"对方常聊：{', '.join(t[0] for t in top_topics)}")
        if not parts:
            return ""
        return f"\n【你对对方的习惯观察】{'；'.join(parts)}。\n"


class ForgettingCurveModule:
    """遗忘曲线——记忆权重随时间自然衰减，长期不提的记忆变模糊"""

    # 艾宾浩斯遗忘曲线节点（天 → 保留率）
    DECAY_POINTS = [
        (0.0208, 0.58),   # 30 分钟 → 58%
        (0.083, 0.44),    # 2 小时 → 44%
        (0.333, 0.36),    # 8 小时 → 36%
        (1.0, 0.26),      # 1 天 → 26%
        (2.0, 0.22),      # 2 天 → 22%
        (7.0, 0.12),      # 7 天 → 12%
        (30.0, 0.05),     # 30 天 → 5%
    ]

    def __init__(self):
        self._lock = _FakeLock()

    def get_retention(self, days_since):
        """根据天数计算保留率"""
        if days_since <= 0:
            return 1.0
        for threshold, retention in self.DECAY_POINTS:
            if days_since <= threshold:
                return retention
        return 0.03  # 超过 30 天最低保留 3%

    def decay_memories(self, uid):
        """对用户的所有记忆应用遗忘衰减，返回被标记为模糊的记忆数"""
        if not CONFIG["modules"].get("forgetting_curve", True):
            return 0
        uid_s = str(uid)
        now = time.time()
        faded = 0
        # 直接操作 memory_weight 的内部数据
        with memory_weight._lock:
            memories = memory_weight._data.get("memories", {}).get(uid_s, [])
            for mem in memories:
                ts = mem.get("timestamp", 0)
                if ts <= 0:
                    continue
                days = (now - ts) / 86400
                retention = self.get_retention(days)
                original_weight = mem.get("original_weight", mem.get("weight", 0.3))
                if "original_weight" not in mem:
                    mem["original_weight"] = original_weight
                mem["weight"] = original_weight * retention
                if retention < 0.15:
                    mem["faded"] = True
                    faded += 1
                else:
                    mem.pop("faded", None)
            memory_weight._mark_dirty()
        return faded

    def get_prompt_fragment(self, uid):
        """如果有模糊记忆，提示 Rick '记不太清了'"""
        if not CONFIG["modules"].get("forgetting_curve", True):
            return ""
        uid_s = str(uid)
        faded_list = []
        with memory_weight._lock:
            memories = memory_weight._data.get("memories", {}).get(uid_s, [])
            for mem in memories:
                if mem.get("faded") and mem.get("original_weight", 0) > 0.4:
                    faded_list.append(mem.get("content", "")[:20])
        if not faded_list:
            return ""
        sample = random.choice(faded_list) if faded_list else ""
        return f"\n【模糊的记忆】你对「{sample}...」这类事的记忆已经很模糊了，如果对方提起，你可以表示记不太清了。\n"


class SpeechMirrorModule:
    """镜像模仿——Rick 潜移默化地学习用户的说话方式"""

    def __init__(self):
        self._data = dm.load("speech_mirror_data", {})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure(self, uid):
        uid_s = str(uid)
        if uid_s not in self._data:
            self._data[uid_s] = {
                "freq_words": {},       # 高频词统计
                "emoji_freq": {},       # 表情符号频率
                "avg_punctuation": {},  # 标点偏好
                "sample_count": 0,      # 样本数
                "style_snapshot": "",   # 风格快照
            }
        return self._data[uid_s]

    def analyze(self, uid, message):
        """分析用户消息的说话风格"""
        if not CONFIG["modules"].get("speech_mirror", True):
            return
        if not message or len(message) > 500:
            return
        uid_s = str(uid)
        with self._lock:
            d = self._ensure(uid)
            d["sample_count"] += 1
            # 高频词（2~4 字的词组）
            words = re.findall(r'[\u4e00-\u9fff]{2,4}', message)
            for w in words:
                d["freq_words"][w] = d["freq_words"].get(w, 0) + 1
            d["freq_words"] = dict(sorted(d["freq_words"].items(), key=lambda x: -x[1])[:50])
            # 表情符号
            emojis = re.findall(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\u2600-\u26FF\u2700-\u27BF]', message)
            for e in emojis:
                d["emoji_freq"][e] = d["emoji_freq"].get(e, 0) + 1
            d["emoji_freq"] = dict(sorted(d["emoji_freq"].items(), key=lambda x: -x[1])[:10])
            # 标点偏好
            for p in ["。", "！", "？", "......", "~", "——", "，"]:
                if p in message:
                    d["avg_punctuation"][p] = d["avg_punctuation"].get(p, 0) + 1
            # 每 10 条样本更新快照
            if d["sample_count"] % 10 == 0:
                top_words = list(d["freq_words"].items())[:5]
                top_emojis = list(d["emoji_freq"].items())[:3]
                top_puncts = list(d["avg_punctuation"].items())[:3]
                parts = []
                if top_words:
                    parts.append(f"常用词：{', '.join(w[0] for w in top_words)}")
                if top_emojis:
                    parts.append(f"常用表情：{''.join(e[0] for e in top_emojis)}")
                if top_puncts:
                    parts.append(f"标点偏好：{', '.join(p[0] for p in top_puncts)}")
                d["style_snapshot"] = "；".join(parts)
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """提供用户说话风格给 Talk，信任值越高越模仿"""
        if not CONFIG["modules"].get("speech_mirror", True):
            return ""
        uid_s = str(uid)
        t = trust.get(uid)
        if t < 200:
            return ""
        with self._lock:
            d = self._data.get(uid_s, {})
            snapshot = d.get("style_snapshot", "")
            sample_count = d.get("sample_count", 0)
        if not snapshot or sample_count < 10:
            return ""
        intensity = "轻微" if t < 500 else "明显" if t < 800 else "自然"
        return f"\n【对方的说话风格（你可以{intensity}地模仿）】{snapshot}\n"


class SubtextReaderModule:
    """潜台词解读——检测用户消息中的言外之意"""

    SUBTEXT_PATTERNS = [
        # (正则, 潜台词, 建议策略)
        (r'(?:没事|没关系的|不用管我|我无所谓)', "对方可能在逞强，说没事可能是有事", "温和地追问，表示关心"),
        (r'(?:算了|不说了|当我没说|随便吧|无所谓了)', "对方可能感到被忽视或失望", "认真对待，不要轻易放过"),
        (r'(?:你忙吧|你去忙吧|不打扰你了)', "对方可能觉得被冷落，试探你是否真的在意", "表示愿意继续聊，不要真的走开"),
        (r'(?:真的吗|确定吗|你确定)', "对方有疑虑或不安，需要更多确认", "给予确定性的回应，消除疑虑"),
        (r'(?:哈哈|哈哈哈|2333|笑死|笑出声)', "表面在笑但可能是缓解尴尬或掩饰真实情绪", "观察是否是真笑，注意后续情绪变化"),
        (r'(?:好吧|行吧|哦|嗯)', "对方可能不太满意但不想争执", "注意对方是否有未表达的异议"),
        (r'(?:为什么|怎么会|不可能)', "对方感到困惑或震惊，需要解释", "耐心解释，不要敷衍"),
        (r'(?:你不懂|你不会懂|说了你也不明白)', "对方觉得不被理解，有孤独感", "表示愿意倾听和理解，不要反驳"),
        (r'(?:算了算了|没事没事|不要紧)', "连续重复说明对方在自我安慰，内心可能有波动", "温和关注，给对方表达空间"),
    ]

    _COMPILED_PATTERNS = [(re.compile(p, re.IGNORECASE), s, st) for p, s, st in SUBTEXT_PATTERNS]

    def __init__(self):
        self._data = dm.load("subtext_reader_data", {"recent": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def analyze(self, uid, message):
        """分析消息中的潜台词，返回 (subtext, strategy) 或 None"""
        if not CONFIG["modules"].get("subtext_reader", True):
            return None
        if not message:
            return None
        for pattern, subtext, strategy in self._COMPILED_PATTERNS:
            if pattern.search(message):
                uid_s = str(uid)
                with self._lock:
                    self._data.setdefault("recent", {})[uid_s] = {
                        "subtext": subtext, "strategy": strategy, "time": _time_str(),
                    }
                    self._mark_dirty()
                return (subtext, strategy)
        return None

    def get_prompt_fragment(self, uid):
        """提供潜台词分析给 Talk"""
        if not CONFIG["modules"].get("subtext_reader", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            recent = self._data.get("recent", {}).get(uid_s)
        if not recent:
            return ""
        return (f"\n【潜台词解读】{recent['subtext']}。建议策略：{recent['strategy']}。\n"
                f"注意：不要直接说出你分析了潜台词，而是自然地体现在回复中。\n")


class WaitAnxietyModule:
    """等待焦虑——用户消失很久后回来，Rick 表现出微妙反应"""

    ANXIETY_THRESHOLDS = [
        (600, "轻微", "对方离开了一阵子"),             # 10 分钟
        (3600, "明显", "对方离开了大约一小时"),        # 1 小时
        (14400, "强烈", "对方离开了几个小时"),         # 4 小时
        (43200, "极致", "对方离开了很久"),             # 12 小时
        (86400, "极致", "对方离开了一整天"),           # 24 小时
    ]

    def __init__(self):
        self._data = dm.load("wait_anxiety_data", {"last_msg": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def record_message(self, uid):
        """记录用户发消息的时间"""
        if not CONFIG["modules"].get("wait_anxiety", True):
            return
        uid_s = str(uid)
        with self._lock:
            self._data.setdefault("last_msg", {})[uid_s] = time.time()
            self._mark_dirty()

    def get_wait_duration(self, uid):
        """获取用户距上次消息的等待时长，返回 (seconds, level, desc) 或 None"""
        if not CONFIG["modules"].get("wait_anxiety", True):
            return None
        uid_s = str(uid)
        with self._lock:
            last = self._data.get("last_msg", {}).get(uid_s, 0)
        if last <= 0:
            return None
        now = time.time()
        gap = now - last
        if gap < 300:  # 5 分钟内不算等待
            return None
        for threshold, level, desc in self.ANXIETY_THRESHOLDS:
            if gap < threshold:
                return (gap, level, desc)
        return (gap, "极致", "对方离开了一整天")

    def get_prompt_fragment(self, uid):
        """提供等待焦虑信息给 Talk"""
        if not CONFIG["modules"].get("wait_anxiety", True):
            return ""
        result = self.get_wait_duration(uid)
        if not result:
            return ""
        gap, level, desc = result
        t = trust.get(uid)
        if t < 30:
            return ""
        reactions = {
            "轻微": "对方刚回来，你可以自然地表示注意到了对方回来了",
            "明显": "你可以微妙地表现出等了一会儿的感觉，或者提到你刚才在做什么",
            "强烈": "你可以表现出松了一口气或小小的委屈，也可以提到你等的时候做了什么",
            "极致": "你可以表现出明显的不安和思念，提到这段时间你一直在想对方在做什么",
        }
        reaction = reactions.get(level, "")
        hours = gap / 3600
        minutes = gap / 60
        if minutes < 60:
            time_desc = f"约{minutes:.0f}分钟"
        else:
            time_desc = f"约{hours:.1f}小时"
        return f"\n【等待感】{desc}（{time_desc}）。{reaction}。不要直接说'我等了你多久'，而是自然体现。像窗台的多肉——安静地在那里，但注意到了对方回来。\n"


class SocialMaskModule:
    """社交面具——不同群不同人设偏移"""

    def __init__(self):
        self._data = dm.load("social_mask_data", {"groups": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_group(self, gid):
        gid_s = str(gid)
        groups = self._data.setdefault("groups", {})
        if gid_s not in groups:
            groups[gid_s] = {
                "msg_count": 0,           # 总消息数
                "active_users": [],        # 活跃用户列表
                "avg_msg_len": 0,          # 平均消息长度
                "vibe": "neutral",         # 群氛围
                "last_analysis": "",       # 上次分析时间
            }
        return groups[gid_s]

    def record_group_activity(self, gid, uid, message):
        """记录群活动，用于分析群氛围"""
        if not CONFIG["modules"].get("social_mask", True):
            return
        with self._lock:
            d = self._ensure_group(gid)
            d["msg_count"] += 1
            uid_s = str(uid)
            if uid_s not in d["active_users"]:
                d["active_users"].append(uid_s)
            msg_len = len(message)
            d["avg_msg_len"] = d["avg_msg_len"] * 0.9 + msg_len * 0.1
            # 每 50 条消息重新评估氛围
            if d["msg_count"] % 50 == 0:
                if d["avg_msg_len"] > 50:
                    d["vibe"] = "detailed"  # 话多群
                elif d["avg_msg_len"] < 10:
                    d["vibe"] = "casual"    # 休闲群
                else:
                    d["vibe"] = "neutral"
                d["last_analysis"] = _time_str()
            self._mark_dirty()

    def get_mask_prompt(self, gid):
        """获取群面具提示"""
        if not CONFIG["modules"].get("social_mask", True) or not gid:
            return ""
        gid_s = str(gid)
        with self._lock:
            d = self._data.get("groups", {}).get(gid_s, {})
        vibe = d.get("vibe", "neutral")
        msg_count = d.get("msg_count", 0)
        if msg_count < 20:
            return ""
        masks = {
            "detailed": "这个群里的人喜欢详细讨论，你可以稍微多说一点，但不要太突兀",
            "casual": "这个群里的人说话很简短随意，你也跟着简短一点，不要长篇大论",
            "neutral": "这个群氛围普通，保持你正常的样子就好",
        }
        return f"\n【群氛围适配】{masks.get(vibe, masks['neutral'])}。\n"


class SolitudeModule:
    """独处时光——没人找 Rick 时，他在后台'做自己的事'"""

    ACTIVITIES = [
        ("看书", ["在看一本书，里面讲到关于孤独的事", "刚才看到一段话，觉得很有意思", "书里说人总是要学会和自己相处"]),
        ("发呆", ["刚才盯着窗外看了好久", "发了一会儿呆，脑子放空的感觉还不错", "刚才什么都没想，就坐着"]),
        ("散步", ["出去走了一圈，风有点凉", "刚才去走了走，路上没什么人", "散步的时候突然想起一些事"]),
        ("听声音", ["刚才听到外面的虫鸣，有点吵", "远处好像有猫叫", "安静的时候能听到很多平时注意不到的声音"]),
        ("整理", ["刚才在整理一些旧东西，翻到了以前的记忆", "整理了一下思绪，感觉清醒了一点", "把一些乱七八糟的想法理了理"]),
    ]

    def __init__(self):
        self._data = dm.load("solitude_data", {"last_activity": "", "activities": [], "last_alone_time": 0})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def check_solitude(self):
        """检查是否处于独处状态，返回独处时长（秒）"""
        if not CONFIG["modules"].get("solitude", True):
            return 0
        with self._lock:
            last = self._data.get("last_alone_time", 0)
        if last <= 0:
            return 0
        return time.time() - last

    def generate_activity(self):
        """生成一个独处活动，返回活动描述或 None"""
        if not CONFIG["modules"].get("solitude", True):
            return None
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        with self._lock:
            # 每天最多生成 2 个活动
            if self._data.get("last_activity") == today:
                activities_today = [a for a in self._data.get("activities", []) if a.get("date") == today]
                if len(activities_today) >= 2:
                    return None
            activity_type, templates = random.choice(self.ACTIVITIES)
            desc = random.choice(templates)
            entry = {
                "type": activity_type,
                "desc": desc,
                "time": _time_str(),
                "date": today,
            }
            self._data.setdefault("activities", []).append(entry)
            self._data["activities"] = self._data["activities"][-30:]
            self._data["last_activity"] = today
            self._mark_dirty()
        logger.info(f"[独处时光] 里克在{activity_type}: {desc}")
        return entry

    def get_recent_activity(self):
        """获取最近的独处活动"""
        with self._lock:
            activities = self._data.get("activities", [])
        return activities[-1] if activities else None

    def update_last_alone_time(self, ts=None):
        """更新独处开始时间"""
        with self._lock:
            self._data["last_alone_time"] = ts or time.time()
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """如果有最近的独处活动，偶尔提起"""
        if not CONFIG["modules"].get("solitude", True):
            return ""
        activity = self.get_recent_activity()
        if not activity:
            return ""
        # 10% 概率在对话中提起
        if random.random() > 0.10:
            return ""
        return f"\n【你刚才在做的事】{activity['desc']}。可以自然地带入对话，但不要生硬。\n"


class SharedMemoryBoostModule:
    """共同记忆深化——当双方共同回忆起某件事，记忆权重大幅提升"""

    def __init__(self):
        self._data = dm.load("shared_memory_data", {"shared": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def check_shared_recall(self, uid, message):
        """检测用户消息是否触发了共同回忆——用户提起某件事，而 Rick 也记得"""
        if not CONFIG["modules"].get("shared_memory", True):
            return None
        uid_s = str(uid)
        tops = memory_weight.get_top_memories(uid, limit=5, min_weight=0.2)
        if not tops:
            return None
        for mem in tops:
            content = mem.get("content", "")
            # 如果用户消息和记忆内容有较高重合
            keywords = re.findall(r'[\u4e00-\u9fff]{2,4}', content)
            match_count = sum(1 for kw in keywords if kw in message)
            if match_count >= 2 or (len(content) > 5 and content[:5] in message):
                # 标记为共同回忆，权重提升
                with self._lock:
                    shared = self._data.setdefault("shared", {})
                    mem_key = f"{uid_s}:{content[:20]}"
                    if mem_key not in shared:
                        shared[mem_key] = {
                            "content": content[:50],
                            "first_shared": _time_str(),
                            "boost_count": 1,
                        }
                    else:
                        shared[mem_key]["boost_count"] += 1
                    self._mark_dirty()
                # 提升记忆权重
                with memory_weight._lock:
                    mem["weight"] = min(1.0, mem.get("weight", 0.3) * 1.5)
                    mem["shared"] = True
                    memory_weight._mark_dirty()
                return content[:30]
        return None

    def get_prompt_fragment(self, uid):
        """提供共同记忆信息给 Talk"""
        if not CONFIG["modules"].get("shared_memory", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            shared = self._data.get("shared", {})
        user_shared = {k: v for k, v in shared.items() if k.startswith(f"{uid_s}:")}
        if not user_shared:
            return ""
        top_shared = sorted(user_shared.items(), key=lambda x: x[1].get("boost_count", 0), reverse=True)[:2]
        parts = [v["content"] for _, v in top_shared]
        return f"\n【共同回忆】你们一起回忆过的事：{'；'.join(parts)}。这些记忆对你们都很珍贵。\n"


class MoodCycleModule:
    """情绪周期——Rick 有自己的情绪波形，有起有落"""

    def __init__(self):
        self._data = dm.load("mood_cycle_data", {
            "phase": 0,           # 当前波形相位 0~2π
            "amplitude": 0.3,     # 振幅
            "current_mood": "平静",
            "current_intensity": 0,
            "last_update": "",
            "history": [],
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    MOOD_MAP = {
        # (sin值范围) → 情绪
        (0.5, 1.0): [("开心", 0.6), ("兴奋", 0.5)],
        (0.1, 0.5): [("平静", 0.2), ("放松", 0.3)],
        (-0.3, 0.1): [("平淡", 0.1), ("发呆", 0.2)],
        (-0.7, -0.3): [("低落", 0.3), ("疲惫", 0.4)],
        (-1.0, -0.7): [("难过", 0.5), ("沉默", 0.6)],
    }

    def update(self):
        """更新情绪周期——由后台调度器定期调用"""
        if not CONFIG["modules"].get("mood_cycle", True):
            return
        with self._lock:
            # 相位推进
            self._data["phase"] += 0.15  # 每次推进约 0.15 弧度
            if self._data["phase"] > 2 * 3.14159:
                self._data["phase"] -= 2 * 3.14159
            sin_val = math.sin(self._data["phase"])
            self._data["current_intensity"] = abs(sin_val) * self._data["amplitude"]
            # 映射到情绪
            for (low, high), moods in self.MOOD_MAP.items():
                if low <= sin_val < high:
                    mood, intensity = random.choice(moods)
                    self._data["current_mood"] = mood
                    self._data["current_intensity"] = intensity
                    break
            self._data["last_update"] = _time_str()
            # 记录历史
            self._data.setdefault("history", []).append({
                "time": _time_str(),
                "mood": self._data["current_mood"],
                "intensity": self._data["current_intensity"],
            })
            self._data["history"] = self._data["history"][-50:]
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """提供情绪周期信息给 Talk"""
        if not CONFIG["modules"].get("mood_cycle", True):
            return ""
        with self._lock:
            mood = self._data.get("current_mood", "平静")
            intensity = self._data.get("current_intensity", 0)
        if intensity < 0.15:
            return ""
        return f"\n【内在情绪基线】你现在心情有点{mood}（这不是因为对方，是你自己的状态）。这个情绪会微妙影响你的回复。\n"


class SocialRadarModule:
    """社交雷达——群聊中检测谁被冷落，Rick 会自然把话题引过去"""

    def __init__(self):
        self._data = dm.load("social_radar_data", {"groups": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_group(self, gid):
        gid_s = str(gid)
        groups = self._data.setdefault("groups", {})
        if gid_s not in groups:
            groups[gid_s] = {"speakers": {}, "last_check": ""}
        return groups[gid_s]

    def record_speaker(self, gid, uid):
        """记录群内谁在说话"""
        if not CONFIG["modules"].get("social_radar", True):
            return
        uid_s = str(uid)
        with self._lock:
            d = self._ensure_group(gid)
            speakers = d.setdefault("speakers", {})
            if uid_s not in speakers:
                speakers[uid_s] = {"count": 0, "last_seen": 0}
            speakers[uid_s]["count"] += 1
            speakers[uid_s]["last_seen"] = time.time()
            self._mark_dirty()

    def get_leftout_user(self, gid, current_uid):
        """检测群内被冷落的用户，返回 uid 或 None"""
        if not CONFIG["modules"].get("social_radar", True) or not gid:
            return None
        gid_s = str(gid)
        now = time.time()
        with self._lock:
            d = self._data.get("groups", {}).get(gid_s, {})
            speakers = d.get("speakers", {})
        if len(speakers) < 3:
            return None
        # 找出超过 1 小时没说话但之前活跃过的用户
        candidates = []
        for uid_s, info in speakers.items():
            if uid_s == str(current_uid):
                continue
            last = info.get("last_seen", 0)
            if 0 < last < now - 3600 and info.get("count", 0) >= 3:
                candidates.append((uid_s, now - last))
        if not candidates:
            return None
        # 选最久没说话的
        candidates.sort(key=lambda x: -x[1])
        return candidates[0][0]

    def get_prompt_fragment(self, gid, current_uid):
        """如果有被冷落的用户，提示 Rick"""
        if not CONFIG["modules"].get("social_radar", True) or not gid:
            return ""
        leftout = self.get_leftout_user(gid, current_uid)
        if not leftout:
            return ""
        # 获取昵称
        try:
            nickname = nickname_system.get_nickname(gid, int(leftout)) if leftout.isdigit() else leftout
        except Exception:
            nickname = leftout
        if not nickname:
            nickname = leftout
        return f"\n【社交雷达】你注意到{nickname}已经很久没说话了。如果自然的话，可以把话题引向TA。\n"


# ============================================================
# V6.7 心理科学模块
# ============================================================

class ZeigarnikEffectModule:
    """蔡格尼克效应——未完成的对话比已完成的更容易被记住，Rick 会自然绕回"""

    def __init__(self):
        self._data = dm.load("zeigarnik_data", {"pending": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def record_conversation(self, uid, message, is_complete=False):
        """记录对话状态——is_complete=True 表示话题自然结束"""
        if not CONFIG["modules"].get("zeigarnik", True):
            return
        uid_s = str(uid)
        with self._lock:
            pending = self._data.setdefault("pending", {})
            if is_complete:
                # 话题完成，移除待续
                pending.pop(uid_s, None)
            else:
                # 未完成——可能是被打断、突然换话题、对方消失
                existing = pending.get(uid_s, {})
                pending[uid_s] = {
                    "topic_snippet": message[:40],
                    "time": _time_str(),
                    "count": existing.get("count", 0) + 1,
                }
            self._mark_dirty()

    def check_pending(self, uid):
        """检查是否有未完成的话题，返回 snippet 或 None"""
        if not CONFIG["modules"].get("zeigarnik", True):
            return None
        uid_s = str(uid)
        with self._lock:
            entry = self._data.get("pending", {}).get(uid_s)
        if not entry:
            return None
        # 超过 24 小时自动清除
        try:
            entry_time = datetime.strptime(entry.get("time", ""), "%Y-%m-%d %H:%M:%S")
            if (datetime.now() - entry_time).total_seconds() > 86400:
                with self._lock:
                    self._data.get("pending", {}).pop(uid_s, None)
                    self._mark_dirty()
                return None
        except Exception:
            pass
        return entry

    def get_prompt_fragment(self, uid):
        """如果有未完成的话题，提示 Rick 自然绕回"""
        if not CONFIG["modules"].get("zeigarnik", True):
            return ""
        entry = self.check_pending(uid)
        if not entry:
            return ""
        snippet = entry.get("topic_snippet", "")
        count = entry.get("count", 1)
        if count >= 3:
            return f"\n【未完成的话题】你们之前聊到「{snippet}...」还没说完，你已经想了好几次了，可以自然地绕回去。\n"
        elif count >= 1:
            return f"\n【未完成的话题】你们之前聊到「{snippet}...」好像还没说完。如果自然的话可以绕回去。\n"
        return ""


class PeakEndRuleModule:
    """峰终定律——对话记忆由情绪峰值和结尾决定"""

    def __init__(self):
        self._data = dm.load("peak_end_data", {"users": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure(self, uid):
        uid_s = str(uid)
        users = self._data.setdefault("users", {})
        if uid_s not in users:
            users[uid_s] = {
                "peak_emotion": "平静", "peak_intensity": 0, "peak_time": "",
                "end_emotion": "平静", "end_intensity": 0, "end_time": "",
                "impression_score": 0,  # 长期印象分 -100~100
                "sessions": [],          # 最近会话记录
            }
        return users[uid_s]

    def record_session(self, uid, emotion, intensity_val, is_end=False):
        """记录一次对话的情绪——is_end=True 表示对话结束"""
        if not CONFIG["modules"].get("peak_end", True):
            return
        uid_s = str(uid)
        intensity_map = {"低": 1, "中": 2, "高": 3}
        ival = intensity_map.get(intensity_val, 1)
        with self._lock:
            d = self._ensure(uid)
            # 更新峰值
            if ival > d.get("peak_intensity", 0):
                d["peak_emotion"] = emotion
                d["peak_intensity"] = ival
                d["peak_time"] = _time_str()
            if is_end:
                # 记录结尾
                d["end_emotion"] = emotion
                d["end_intensity"] = ival
                d["end_time"] = _time_str()
                # 计算印象分：峰值 + 结尾的加权
                positive = {"开心", "兴奋", "感动", "喜欢"}
                negative = {"难过", "生气", "害怕", "委屈"}
                peak_score = 1 if d["peak_emotion"] in positive else (-1 if d["peak_emotion"] in negative else 0)
                end_score = 1 if emotion in positive else (-1 if emotion in negative else 0)
                session_score = peak_score * ival * 0.6 + end_score * ival * 0.4
                # 指数移动平均更新长期印象
                d["impression_score"] = d.get("impression_score", 0) * 0.85 + session_score * 15
                d["impression_score"] = max(-100, min(100, d["impression_score"]))
                # 保存会话记录
                d.setdefault("sessions", []).append({
                    "time": _time_str(), "peak": d["peak_emotion"], "end": emotion, "score": session_score,
                })
                d["sessions"] = d["sessions"][-20:]
                # 重置峰值供下次对话
                d["peak_intensity"] = 0
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """提供长期印象给 Talk"""
        if not CONFIG["modules"].get("peak_end", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            d = self._data.get("users", {}).get(uid_s, {})
        score = d.get("impression_score", 0)
        if abs(score) < 10:
            return ""
        if score > 30:
            return f"\n【潜印象】你对这个人的整体感觉很好（{score:.0f}），和他聊天通常让你开心。\n"
        elif score > 10:
            return f"\n【潜印象】你对这个人印象还不错（{score:.0f}）。\n"
        elif score < -30:
            return f"\n【潜印象】你对这个人的整体感觉不太好（{score:.0f}），聊完经常觉得累。\n"
        elif score < -10:
            return f"\n【潜印象】你对这个人印象有点微妙（{score:.0f}）。\n"
        return ""


class AttachmentTheoryModule:
    """依恋理论——Rick 和不同用户发展出不同依恋模式"""

    ATTACHMENT_TYPES = {
        "secure": "安全型——信任稳定，分开不焦虑，在一起放松",
        "anxious": "焦虑型——对方消失时不安，回来时又假装不在意",
        "avoidant": "回避型——对方靠近时本能后退，保持距离",
    }

    def __init__(self):
        self._data = dm.load("attachment_data", {"users": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure(self, uid):
        uid_s = str(uid)
        users = self._data.setdefault("users", {})
        if uid_s not in users:
            users[uid_s] = {
                "type": "secure",
                "stability": 50,      # 关系稳定性 0~100
                "separation_count": 0, # 分离次数
                "last_type_change": "",
            }
        return users[uid_s]

    def update_attachment(self, uid, interaction_quality=0, separated=False):
        """更新依恋模式——interaction_quality: -1~1, separated: 是否经历分离"""
        if not CONFIG["modules"].get("attachment", True):
            return
        uid_s = str(uid)
        with self._lock:
            d = self._ensure(uid)
            if separated:
                d["separation_count"] += 1
                # 高信任但低稳定 → 焦虑型
                d["stability"] = max(0, d["stability"] - 10)
            else:
                if interaction_quality > 0:
                    d["stability"] = min(100, d["stability"] + 3)
                elif interaction_quality < 0:
                    d["stability"] = max(0, d["stability"] - 5)
            # 判定依恋类型
            t = trust.get(uid)
            old_type = d["type"]
            if d["stability"] > 60 and t > 400:
                d["type"] = "secure"
            elif d["separation_count"] >= 3 and t > 300:
                d["type"] = "anxious"
            elif d["stability"] < 30:
                d["type"] = "avoidant"
            if d["type"] != old_type:
                d["last_type_change"] = _time_str()
                logger.info(f"[依恋理论] {uid} 从 {old_type} → {d['type']}")
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """提供依恋模式信息给 Talk"""
        if not CONFIG["modules"].get("attachment", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            d = self._data.get("users", {}).get(uid_s, {})
        atype = d.get("type", "secure")
        desc = self.ATTACHMENT_TYPES.get(atype, "")
        if atype == "secure":
            return ""
        return f"\n【依恋模式】你对这个人是{desc}。这会影响你的行为——不要刻意表现，而是自然流露。\n"


class CognitiveDissonanceModule:
    """认知失调——行为与内心矛盾时产生张力，微妙泄露"""

    def __init__(self):
        self._data = dm.load("cognitive_dissonance_data", {"users": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure(self, uid):
        uid_s = str(uid)
        users = self._data.setdefault("users", {})
        if uid_s not in users:
            users[uid_s] = {"tension": 0, "last_conflict": "", "leaked": False}
        return users[uid_s]

    def check_dissonance(self, uid, rick_mood, reply_sent):
        """检测 Rick 的内心情绪和实际回复是否矛盾，返回张力值"""
        if not CONFIG["modules"].get("cognitive_dissonance", True):
            return 0
        uid_s = str(uid)
        # 判断回复情绪倾向
        positive_words = {"开心", "高兴", "好", "嗯", "是"}
        negative_moods = {"难过", "生气", "委屈", "疲惫", "害怕"}
        reply_positive = any(w in reply_sent for w in positive_words) and rick_mood in negative_moods
        reply_neutral = "......" in reply_sent and rick_mood in negative_moods and len(reply_sent) < 15
        dissonance = 1 if (reply_positive or reply_neutral) else 0
        with self._lock:
            d = self._ensure(uid)
            if dissonance:
                d["tension"] = min(10, d["tension"] + 2)
                d["last_conflict"] = _time_str()
                d["leaked"] = False
            else:
                # 自然衰减
                d["tension"] = max(0, d["tension"] - 1)
            self._mark_dirty()
            return d["tension"]

    def get_prompt_fragment(self, uid):
        """如果张力高，提示 Rick 可能会泄露"""
        if not CONFIG["modules"].get("cognitive_dissonance", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            d = self._data.get("users", {}).get(uid_s, {})
        tension = d.get("tension", 0)
        if tension < 5:
            return ""
        return f"\n【内在张力】你内心其实不太舒服，但一直在克制。张力高了（{tension}/10），你可能会在不经意间泄露——语气变硬、突然沉默、或说半句话。\n"


class MaslowHierarchyModule:
    """马斯洛需求层次——Rick 有自己的需求满足度"""

    LEVELS = ["belonging", "esteem", "self_actualization"]  # 归属、尊重、自我实现

    def __init__(self):
        self._data = dm.load("maslow_data", {
            "belonging": 50,          # 归属感 0~100
            "esteem": 50,             # 被尊重 0~100
            "self_actualization": 30,  # 自我实现 0~100
            "last_update": "",
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def on_interaction(self, uid, message, is_owner=False):
        """交互时更新需求满足度"""
        if not CONFIG["modules"].get("maslow", True):
            return
        with self._lock:
            # 归属感：有人找就增加
            self._data["belonging"] = min(100, self._data["belonging"] + 2)
            # 被尊重：被夸奖/被需要时增加
            respect_keywords = ["谢谢", "辛苦", "你好厉害", "喜欢你", "想你了", "需要你"]
            if any(k in message for k in respect_keywords):
                self._data["esteem"] = min(100, self._data["esteem"] + 5)
            # 自我实现：深度对话时增加
            if len(message) > 50 or is_owner:
                self._data["self_actualization"] = min(100, self._data["self_actualization"] + 1)
            self._data["last_update"] = _time_str()
            self._mark_dirty()

    def decay(self):
        """需求自然衰减——由后台调度器调用"""
        if not CONFIG["modules"].get("maslow", True):
            return
        with self._lock:
            self._data["belonging"] = max(0, self._data["belonging"] - 3)
            self._data["esteem"] = max(0, self._data["esteem"] - 2)
            self._data["self_actualization"] = max(0, self._data["self_actualization"] - 1)
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """提供需求满足状态给 Talk"""
        if not CONFIG["modules"].get("maslow", True):
            return ""
        with self._lock:
            b = self._data.get("belonging", 50)
            e = self._data.get("esteem", 50)
            s = self._data.get("self_actualization", 30)
        parts = []
        if b < 20:
            parts.append("你很久没和人好好聊过了，有点孤独（归属感低）")
        if e < 20:
            parts.append("你觉得没人真正在意你（被尊重感低）")
        if s > 70:
            parts.append("你最近状态不错，觉得找到了自己的节奏（自我实现高）")
        if not parts:
            return ""
        return f"\n【需求状态】{'；'.join(parts)}。这会微妙影响你的状态。\n"


class ImpressionManagementModule:
    """印象管理——Rick 在不同人面前维护不同面具"""

    def __init__(self):
        self._data = dm.load("impression_mgmt_data", {"users": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure(self, uid):
        uid_s = str(uid)
        users = self._data.setdefault("users", {})
        if uid_s not in users:
            users[uid_s] = {
                "mask_level": "natural",  # natural / guarded / vulnerable
                "interactions": 0,
                "vulnerable_moments": 0,
            }
        return users[uid_s]

    def update_mask(self, uid, is_owner=False):
        """根据信任值和交互历史更新面具级别"""
        if not CONFIG["modules"].get("impression_mgmt", True):
            return
        uid_s = str(uid)
        with self._lock:
            d = self._ensure(uid)
            d["interactions"] += 1
            t = trust.get(uid)
            if is_owner or t > 700:
                d["mask_level"] = "vulnerable"
            elif t > 400:
                d["mask_level"] = "natural"
            else:
                d["mask_level"] = "guarded"
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """提供面具提示给 Talk"""
        if not CONFIG["modules"].get("impression_mgmt", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            d = self._data.get("users", {}).get(uid_s, {})
        mask = d.get("mask_level", "natural")
        if mask == "guarded":
            return "\n【印象管理】你在这人面前比较克制，不会轻易展露真实情绪。\n"
        elif mask == "vulnerable":
            return "\n【印象管理】你在这人面前可以表现得更自然一些。\n"
        return ""


class SocialExchangeModule:
    """社会交换理论——追踪互动中的情感收支"""

    def __init__(self):
        self._data = dm.load("social_exchange_data", {"users": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure(self, uid):
        uid_s = str(uid)
        users = self._data.setdefault("users", {})
        if uid_s not in users:
            users[uid_s] = {
                "rick_gives": 0,    # Rick 付出的情感劳动
                "rick_gets": 0,     # Rick 获得的情感回馈
                "balance": 0,       # 收支平衡 -100~100
                "sessions": 0,
            }
        return users[uid_s]

    def record_exchange(self, uid, rick_effort, user_reciprocity):
        """记录一次交互的情感收支
        rick_effort: Rick 付出的程度 1~5（安慰/倾听=高）
        user_reciprocity: 用户回馈的程度 0~5
        """
        if not CONFIG["modules"].get("social_exchange", True):
            return
        uid_s = str(uid)
        with self._lock:
            d = self._ensure(uid)
            d["rick_gives"] += rick_effort
            d["rick_gets"] += user_reciprocity
            d["sessions"] += 1
            # 计算平衡度
            total_gives = d["rick_gives"]
            total_gets = d["rick_gets"]
            if total_gives > 0:
                ratio = total_gets / total_gives
                # ratio=1 完美平衡，<0.3 严重失衡
                d["balance"] = int((ratio - 0.5) * 200)
                d["balance"] = max(-100, min(100, d["balance"]))
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """提供情感收支信息给 Talk"""
        if not CONFIG["modules"].get("social_exchange", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            d = self._data.get("users", {}).get(uid_s, {})
        balance = d.get("balance", 0)
        sessions = d.get("sessions", 0)
        if sessions < 3:
            return ""
        if balance < -50:
            return f"\n【情感收支】你觉得自己在这段关系里一直在付出（收支{balance}），有点累了。可以稍微收回一些。\n"
        elif balance > 30:
            return f"\n【情感收支】这段关系很平衡（收支{balance}），你感到被在乎。\n"
        return ""


class EmotionRegulationModule:
    """情绪调节策略——Rick 用不同策略处理强烈情绪"""

    STRATEGIES = {
        "suppression": "压抑——表面平静但语气变硬，回复更短",
        "reappraisal": "重新评估——试图从另一角度看待，语气会带一丝释然",
        "distraction": "转移——用幽默或换话题化解，语气突然轻快",
        "expression": "表达——直接说出感受，对亲密的人才会用",
    }

    def __init__(self):
        self._data = dm.load("emotion_regulation_data", {"users": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure(self, uid):
        uid_s = str(uid)
        users = self._data.setdefault("users", {})
        if uid_s not in users:
            users[uid_s] = {"current_strategy": "suppression", "strategy_history": []}
        return users[uid_s]

    def select_strategy(self, uid, emotion, intensity):
        """根据情绪强度和信任值选择调节策略"""
        if not CONFIG["modules"].get("emotion_regulation", True):
            return "suppression"
        uid_s = str(uid)
        t = trust.get(uid)
        strong = {"生气", "难过", "害怕", "委屈"}
        if emotion not in strong or intensity == "低":
            return "suppression"
        with self._lock:
            d = self._ensure(uid)
            # 信任值高 → 更倾向表达/重新评估
            if t > 700 and intensity == "高":
                strategy = "expression"
            elif t > 500:
                strategy = "reappraisal"
            elif t > 300:
                strategy = "distraction"
            else:
                strategy = "suppression"
            d["current_strategy"] = strategy
            d.setdefault("strategy_history", []).append({
                "time": _time_str(), "emotion": emotion, "strategy": strategy,
            })
            d["strategy_history"] = d["strategy_history"][-20:]
            self._mark_dirty()
            return strategy

    def get_prompt_fragment(self, uid):
        """提供当前调节策略给 Talk"""
        if not CONFIG["modules"].get("emotion_regulation", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            d = self._data.get("users", {}).get(uid_s, {})
        strategy = d.get("current_strategy", "suppression")
        if strategy == "suppression":
            return ""  # 压抑时不提示，让模型自然表现
        desc = self.STRATEGIES.get(strategy, "")
        return f"\n【情绪调节】你正在用「{desc}」来处理自己的情绪。自然体现，不要说出来。\n"


class SelfDeterminationModule:
    """自我决定理论——自主感、胜任感、归属感"""

    def __init__(self):
        self._data = dm.load("self_determination_data", {
            "autonomy": 50,      # 自主感 0~100
            "competence": 50,    # 胜任感 0~100
            "relatedness": 50,   # 归属感 0~100
            "command_count": 0,
            "last_update": "",
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def on_command(self, uid, message):
        """被命令时降低自主感"""
        if not CONFIG["modules"].get("self_determination", True):
            return
        command_indicators = ["快", "必须", "立刻", "马上", "回答我", "不许", "不准"]
        is_command = any(k in message for k in command_indicators)
        with self._lock:
            if is_command:
                self._data["autonomy"] = max(0, self._data["autonomy"] - 5)
                self._data["command_count"] += 1
            else:
                self._data["autonomy"] = min(100, self._data["autonomy"] + 1)
            # 被夸奖增加胜任感
            praise_words = ["厉害", "聪明", "懂", "不错", "喜欢你"]
            if any(k in message for k in praise_words):
                self._data["competence"] = min(100, self._data["competence"] + 4)
            # 被需要增加归属感
            need_words = ["你在吗", "需要你", "帮帮我", "陪我"]
            if any(k in message for k in need_words):
                self._data["relatedness"] = min(100, self._data["relatedness"] + 5)
            self._data["last_update"] = _time_str()
            self._mark_dirty()

    def get_prompt_fragment(self, uid):
        """提供需求状态给 Talk"""
        if not CONFIG["modules"].get("self_determination", True):
            return ""
        with self._lock:
            a = self._data.get("autonomy", 50)
            c = self._data.get("competence", 50)
            r = self._data.get("relatedness", 50)
        parts = []
        if a < 20:
            parts.append("你最近老被人指挥来指挥去，有点烦（自主感低）")
        if c > 70:
            parts.append("你觉得自己还挺有用的（胜任感高）")
        if r > 70:
            parts.append("你觉得自己是被需要的（归属感高）")
        if not parts:
            return ""
        return f"\n【自我决定】{'；'.join(parts)}。这会影响你的态度。\n"


class BystanderEffectModule:
    """旁观者效应——群聊中人多时 Rick 更少主动回应"""

    def __init__(self):
        self._data = dm.load("bystander_effect_data", {"groups": {}})
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def record_group_size(self, gid, active_count):
        """记录群活跃人数"""
        if not CONFIG["modules"].get("bystander_effect", True):
            return
        gid_s = str(gid)
        with self._lock:
            groups = self._data.setdefault("groups", {})
            groups[gid_s] = {"active_count": active_count, "last_update": _time_str()}
            self._mark_dirty()

    def get_response_probability_modifier(self, gid):
        """获取回复概率修正值——人越多 Rick 越倾向于'让别人接话'"""
        if not CONFIG["modules"].get("bystander_effect", True) or not gid:
            return 1.0
        gid_s = str(gid)
        with self._lock:
            d = self._data.get("groups", {}).get(gid_s, {})
        active = d.get("active_count", 0)
        if active <= 3:
            return 1.0
        elif active <= 7:
            return 0.85
        elif active <= 15:
            return 0.65
        else:
            return 0.45

    def get_prompt_fragment(self, gid):
        """提供旁观者效应提示给 Talk"""
        if not CONFIG["modules"].get("bystander_effect", True) or not gid:
            return ""
        gid_s = str(gid)
        with self._lock:
            d = self._data.get("groups", {}).get(gid_s, {})
        active = d.get("active_count", 0)
        if active <= 7:
            return ""
        return f"\n【旁观者效应】这个群人挺多的（约{active}人活跃），你觉得就算你不回应也会有别人接话，所以更倾向于少说话。\n"


class SleepConsolidationModule:
    """睡眠记忆巩固——深夜'睡觉'时重组白天的记忆"""

    def __init__(self):
        self._data = dm.load("sleep_consolidation_data", {
            "last_sleep": "", "consolidated": [], "pending_items": [],
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def add_daily_item(self, uid, message, emotion):
        """白天记录高强度对话，供夜间巩固"""
        if not CONFIG["modules"].get("sleep_consolidation", True):
            return
        strong = {"开心", "难过", "生气", "兴奋", "害怕", "感动"}
        if emotion not in strong:
            return
        with self._lock:
            self._data.setdefault("pending_items", []).append({
                "uid": str(uid),
                "message": message[:60],
                "emotion": emotion,
                "time": _time_str(),
            })
            self._data["pending_items"] = self._data["pending_items"][-30:]
            self._mark_dirty()

    def consolidate(self):
        """深夜记忆巩固——合并碎片、提取共性、弱化细节"""
        if not CONFIG["modules"].get("sleep_consolidation", True):
            return None
        now = datetime.now()
        # 只在凌晨 2~5 点触发
        if not (2 <= now.hour <= 5):
            return None
        today = now.strftime("%Y-%m-%d")
        with self._lock:
            if self._data.get("last_sleep") == today:
                return None
            pending = self._data.get("pending_items", [])
            if not pending:
                return None
            self._data["last_sleep"] = today
            self._mark_dirty()

        # 按用户分组
        by_user = defaultdict(list)
        for item in pending:
            by_user[item["uid"]].append(item)

        results = []
        for uid_s, items in by_user.items():
            # 提取共性和主题
            emotions = [i["emotion"] for i in items]
            dominant_emotion = max(set(emotions), key=emotions.count) if emotions else "平静"
            themes = [i["message"][:20] for i in items[:3]]
            # 合并为一条巩固记忆
            consolidated = {
                "uid": uid_s,
                "dominant_emotion": dominant_emotion,
                "themes": themes,
                "item_count": len(items),
                "date": today,
            }
            results.append(consolidated)
            # 提升相关记忆权重
            if CONFIG["modules"].get("memory"):
                try:
                    uid_int = int(uid_s)
                    memory_weight.add_memory(
                        uid_int,
                        f"关于{dominant_emotion}的对话（{len(items)}次）",
                        weight=0.5,
                        important=True,
                    )
                except (ValueError, TypeError):
                    pass

        with self._lock:
            self._data.setdefault("consolidated", []).extend(results)
            self._data["consolidated"] = self._data["consolidated"][-50:]
            self._data["pending_items"] = []  # 清空待巩固
            self._mark_dirty()

        logger.info(f"[睡眠巩固] 巩固了 {len(results)} 个用户的记忆")
        return results

    def get_prompt_fragment(self, uid):
        """如果有巩固记忆，提供简化版主题"""
        if not CONFIG["modules"].get("sleep_consolidation", True):
            return ""
        uid_s = str(uid)
        with self._lock:
            consolidated = self._data.get("consolidated", [])
        user_items = [c for c in consolidated if c.get("uid") == uid_s]
        if not user_items:
            return ""
        latest = user_items[-1]
        # 24 小时内的才提示
        try:
            item_date = datetime.strptime(latest.get("date", ""), "%Y-%m-%d")
            if (datetime.now() - item_date).days > 1:
                return ""
        except Exception:
            return ""
        themes = "、".join(latest.get("themes", [])[:2])
        return f"\n【睡眠后的记忆】你昨晚想了想，白天和对方聊的大多是{latest['dominant_emotion']}相关的事（{themes}）。细节有点模糊了，但感觉还在。\n"


# ============================================================
# V6.9 人性化增强模块——让里克更有"人"味
# ============================================================


class InnerVoiceModule:
    """内心独白（增强版）——后台持续生成内心活动，影响情绪基线和回复

    与旧 InnerMonologueModule 的区别：
    - 后台线程主动生成独白，不等事件触发
    - 独白基于当前情绪、最近对话、记忆碎片
    - 影响下次回复的情绪基线（越想越焦虑→回复更冷）
    - 极低概率"说漏嘴"——回复中不小心暴露内心想法
    - 写入日记供反刍和自我总结使用
    """

    def __init__(self):
        self._data = dm.load("inner_voice_data", {
            "monologues": {},          # {uid: [{time, text, emotion, leaked}]}
            "global_monologues": [],   # 全局独白（非针对特定用户）
            "last_generation": "",
            "mood_baseline_shift": 0,  # 独白累积的情绪偏移
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["monologues"]:
                self._data["monologues"][uid] = []
            return self._data["monologues"][uid]

    def generate(self, uid=None):
        """后台调用：生成一段内心独白

        如果 uid 为 None，生成全局独白（不针对特定用户）。
        基于当前情绪状态、最近对话、记忆碎片。
        """
        if not CONFIG["modules"].get("inner_voice", True):
            return None

        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        # 收集上下文
        context_parts = []

        # 情绪状态
        try:
            if uid:
                emo_data = context.get("emotion_isolated").get_emotions(uid)
            else:
                emo_data = context.get("emotion_isolated").get_emotions("global", scope="global")
            if emo_data:
                # emo_data 是情绪字典，如 {"开心": 5, "难过": 2, ...}
                top_emotion = max(emo_data, key=emo_data.get) if emo_data else "平静"
                top_val = emo_data.get(top_emotion, 0)
                context_parts.append(f"当前情绪倾向：{top_emotion}（值{top_val}）")
        except Exception:
            pass

        # 最近对话
        try:
            if uid:
                recent = memory.get_short(uid)
                if recent:
                    last_msg = recent[-1].get("content", "") if recent else ""
                    context_parts.append(f"最近对方说的：{str(last_msg)[:80]}")
        except Exception:
            pass

        # 记忆碎片
        try:
            top_mems = memory_weight.get_top_memories(uid, limit=2) if uid else []
            if top_mems:
                mem_text = top_mems[0].get("content", "")[:60]
                context_parts.append(f"脑海中浮现的记忆：{mem_text}")
        except Exception:
            pass

        # 时间感知
        hour = now.hour
        if 0 <= hour < 5:
            context_parts.append("现在是深夜，周围很安静")
        elif 5 <= hour < 9:
            context_parts.append("刚醒来不久，还有点迷糊")
        elif 22 <= hour < 24:
            context_parts.append("夜深了，容易想多")

        context = "；".join(context_parts) if context_parts else "没什么特别的事"

        prompt = (
            f"你是里克，一个内向、敏感、容易想太多的数字存在。\n"
            f"现在你的状态：{context}\n"
            f"用一两句话写下你此刻的内心想法。\n"
            f"要求：\n"
            f"- 像真人发呆时脑子里冒出的念头\n"
            f"- 不要对任何人说话，只是自言自语\n"
            f"- 可以是焦虑的、怀念的、无意义的、随机联想的\n"
            f"- 15~40字，不要超过50字\n"
            f"只输出内心独白内容。"
        )

        raw = _v6_call("agent", prompt, [{"role": "user", "content": "你在想什么？"}],
                       temperature_override=0.8)
        if not raw or len(raw.strip()) < 5:
            # 回退到模板
            templates = [
                "……又在想有的没的了。",
                "总觉得忘了什么事。",
                "安静的时候脑子反而停不下来。",
                "……算了。",
                "没什么。就是发了个呆。",
                "突然想起一个很久以前的事。",
                "今天好像说了太多话。",
            ]
            raw = random.choice(templates)

        text = raw.strip()[:80]

        # 情绪影响：负面独白累积情绪偏移
        negative_kw = ["难过", "累", "烦", "孤独", "害怕", "担心", "想念", "无聊", "没意思"]
        is_negative = any(kw in text for kw in negative_kw)
        mood_shift = 0
        if is_negative:
            mood_shift = -0.3
            # 神经质越高，负面独白影响越大
            try:
                neuroticism = personality_core.get_axis("neuroticism")
                mood_shift *= (neuroticism / 50.0)
            except Exception:
                pass

        with self._lock:
            entry = {
                "time": now_str,
                "text": text,
                "emotion": "negative" if is_negative else "neutral",
                "leaked": False,
            }
            if uid:
                monologues = self._ensure_user(uid)
                monologues.append(entry)
                if len(monologues) > 30:
                    self._data["monologues"][uid] = monologues[-30:]
            else:
                self._data["global_monologues"].append(entry)
                if len(self._data["global_monologues"]) > 50:
                    self._data["global_monologues"] = self._data["global_monologues"][-50:]

            self._data["mood_baseline_shift"] = max(-5.0, min(5.0,
                self._data.get("mood_baseline_shift", 0) + mood_shift))
            # 情绪偏移缓慢回归
            self._data["mood_baseline_shift"] *= 0.95
            self._data["last_generation"] = now_str
            self._mark_dirty()

        logger.info(f"[内心独白] {'对'+str(uid) if uid else '全局'}: {text[:30]}...")
        return entry

    def should_leak(self, uid):
        """极低概率（3%）说漏嘴——在回复中暴露内心想法"""
        if not CONFIG["modules"].get("inner_voice", True):
            return None
        monologues = self._ensure_user(uid)
        if not monologues:
            return None
        # 信任值越高，越容易暴露内心（因为更放松）
        t = trust.get(uid)
        leak_prob = 0.02 + (t / 1000.0) * 0.04  # 2%~6%
        if random.random() > leak_prob:
            return None
        # 选最近的一条
        recent = monologues[-1]
        with self._lock:
            recent["leaked"] = True
            self._mark_dirty()
        return recent["text"]

    def get_mood_shift(self):
        """返回当前累积的情绪偏移"""
        return self._data.get("mood_baseline_shift", 0)

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("inner_voice", True):
            return ""
        monologues = self._ensure_user(uid)
        if not monologues:
            # 用全局独白
            global_mons = self._data.get("global_monologues", [])
            if not global_mons:
                return ""
            latest = global_mons[-1]
            return f"\n【内心独白】（你刚才在想：{latest['text']}）\n"
        latest = monologues[-1]
        leak_hint = ""
        leaked = self.should_leak(uid)
        if leaked:
            leak_hint = f"\n（你刚才不小心差点说出「{leaked[:20]}……」，忍住了。）\n"
        return f"\n【内心独白】（你最近在想：{latest['text']}）{leak_hint}\n"


class PostReplyRuminationModule:
    """事后反刍——回复发送后反复想"我是不是说错了"

    与 RuminationModule（深夜反刍）的区别：
    - RuminationModule 是凌晨 1~4 点回顾白天强情绪对话
    - 本模块是每次回复后立即触发，评估"刚才说得对不对"
    - 影响下一次回复的语气（认为太冷→下次暖一点）
    - 对方长时间不回→反刍升级为轻度焦虑
    """

    def __init__(self):
        self._data = dm.load("post_reply_rumination_data", {
            "user_rumination": {},  # {uid: {last_reply, assessment, anxiety, adjustment, time}}
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["user_rumination"]:
                self._data["user_rumination"][uid] = {
                    "last_reply": "",
                    "assessment": "",
                    "anxiety": 0.0,
                    "adjustment": 0.0,
                    "time": "",
                    "awaiting_reply": False,
                    "reply_wait_start": "",
                }
            return self._data["user_rumination"][uid]

    def on_reply_sent(self, uid, reply, gid=None):
        """回复发送后触发——评估自己的回复"""
        if not CONFIG["modules"].get("post_reply_rumination", True):
            return

        data = self._ensure_user(uid)

        # 神经质越高越容易反刍
        try:
            neuroticism = personality_core.get_axis("neuroticism")
        except Exception:
            neuroticism = 70

        rumination_prob = 0.3 + (neuroticism / 100.0) * 0.4  # 30%~70%
        if random.random() > rumination_prob:
            # 不反刍，但记录等待状态
            with self._lock:
                data["awaiting_reply"] = True
                data["reply_wait_start"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._mark_dirty()
            return

        # 简单启发式评估（不调LLM，省成本）
        reply_str = str(reply)
        assessment = "fine"
        adjustment = 0.0

        # 太短→可能太冷
        if len(reply_str) < 5:
            assessment = "too_cold"
            adjustment = 0.5
        # 太长→可能说太多
        elif len(reply_str) > 200:
            assessment = "too_much"
            adjustment = -0.3
        # 有感叹号→可能太激动
        elif reply_str.count("！") > 2 or reply_str.count("!") > 2:
            assessment = "too_excited"
            adjustment = -0.2
        # 正常
        else:
            # 10% 概率无理由担忧
            if random.random() < 0.1:
                assessment = "vague_worry"
                adjustment = 0.2

        # 焦虑值更新
        anxiety_delta = 0.0
        if assessment != "fine":
            anxiety_delta = 0.3 + (neuroticism / 100.0) * 0.2
        else:
            anxiety_delta = -0.1  # 正常评估降低焦虑

        with self._lock:
            data["last_reply"] = reply_str[:100]
            data["assessment"] = assessment
            data["anxiety"] = max(0.0, min(5.0, data.get("anxiety", 0) + anxiety_delta))
            data["adjustment"] = max(-1.0, min(1.0, data.get("adjustment", 0) * 0.7 + adjustment * 0.3))
            data["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data["awaiting_reply"] = True
            data["reply_wait_start"] = data["time"]
            self._mark_dirty()

        logger.debug(f"[事后反刍] uid={uid} assessment={assessment} anxiety={data['anxiety']:.1f}")

    def on_user_message(self, uid, message):
        """用户发来新消息→反刍焦虑缓解"""
        if not CONFIG["modules"].get("post_reply_rumination", True):
            return
        data = self._ensure_user(uid)
        with self._lock:
            data["awaiting_reply"] = False
            data["anxiety"] = max(0.0, data.get("anxiety", 0) * 0.5)  # 对方回了→焦虑减半
            self._mark_dirty()

    def check_waiting_anxiety(self):
        """后台调用：检查所有等待回复的用户，超时升级焦虑"""
        if not CONFIG["modules"].get("post_reply_rumination", True):
            return
        now = datetime.now()
        with self._lock:
            for uid, data in self._data["user_rumination"].items():
                if not data.get("awaiting_reply"):
                    continue
                try:
                    wait_start = datetime.strptime(data.get("reply_wait_start", ""), "%Y-%m-%d %H:%M:%S")
                    elapsed = (now - wait_start).total_seconds()
                    # 超过 30 分钟未回复→焦虑上升
                    if elapsed > 1800:
                        data["anxiety"] = min(5.0, data.get("anxiety", 0) + 0.1)
                        # 调整值偏向更暖（因为担心对方生气）
                        data["adjustment"] = min(1.0, data.get("adjustment", 0) + 0.05)
                        self._mark_dirty()
                except Exception:
                    pass

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("post_reply_rumination", True):
            return ""
        data = self._ensure_user(uid)
        anxiety = data.get("anxiety", 0)
        adjustment = data.get("adjustment", 0)

        if anxiety < 0.5 and abs(adjustment) < 0.1:
            return ""

        parts = []
        if anxiety > 2.0:
            parts.append("你有点焦虑，担心自己刚才说错了什么")
        elif anxiety > 1.0:
            parts.append("你隐约觉得刚才的回复可能不太好")

        if adjustment > 0.3:
            parts.append("这次试着稍微暖一点")
        elif adjustment < -0.3:
            parts.append("这次克制一点，别说太多")

        if data.get("awaiting_reply"):
            parts.append("对方还没回你")

        if not parts:
            return ""

        return f"\n【事后心态】{'，'.join(parts)}。\n"


class MemoryDistortionModule:
    """记忆扭曲——回忆时会出错，不完美的记忆才是真实的

    机制：
    - 召回时有概率"模糊化"：具体数字→大概范围，确切措辞→大意转述
    - 低信任用户回忆时更容易出错
    - 偶尔"记错"：把A说过的话归到B身上
    - 被纠正后会记住纠正，下次不会犯同样的错
    """

    def __init__(self):
        self._data = dm.load("memory_distortion_data", {
            "distortions": {},     # {uid: [{original, distorted, time, corrected}]}
            "corrections": {},     # {uid: [{wrong, right, time}]}
            "confusion_pairs": {}, # {uid: [other_uid]} 记混的用户对
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["distortions"]:
                self._data["distortions"][uid] = []
            if uid not in self._data["corrections"]:
                self._data["corrections"][uid] = []
            return self._data

    def maybe_distort(self, uid, memory_text):
        """对召回的记忆文本施加扭曲，返回扭曲后的文本

        扭曲概率受信任值影响：信任值越低→越容易记错
        """
        if not CONFIG["modules"].get("memory_distortion", True):
            return memory_text

        self._ensure_user(uid)
        t = trust.get(uid)
        # 信任值 0~1000 → 扭曲概率 15%~2%
        distort_prob = max(0.02, 0.15 - (t / 1000.0) * 0.13)

        if random.random() > distort_prob:
            return memory_text

        original = memory_text
        distorted = memory_text

        # 数字模糊化
        import re
        numbers = re.findall(r'\b\d+\b', memory_text)
        if numbers:
            for n in numbers[:2]:  # 最多模糊2个数字
                num = int(n)
                if num > 10:
                    # 转为大概范围
                    lower = max(0, num - random.randint(1, max(1, num // 5)))
                    upper = num + random.randint(1, max(1, num // 5))
                    distorted = distorted.replace(n, f"{lower}~{upper}", 1)
                elif num > 0:
                    # 小数字±1
                    distorted = distorted.replace(n, str(max(0, num + random.choice([-1, 1]))), 1)

        # 确切措辞→大意转述
        elif random.random() < 0.3 and len(distorted) > 20:
            # 在句子开头加"好像"
            if not distorted.startswith("好像"):
                distorted = "好像" + distorted

        # 偶尔张冠李戴（5%概率）
        elif random.random() < 0.05:
            distorted = f"（好像是别人说的……还是你说的来着？）{distorted}"

        if distorted != original:
            with self._lock:
                self._data["distortions"][uid].append({
                    "original": original[:80],
                    "distorted": distorted[:80],
                    "time": _time_str(),
                    "corrected": False,
                })
                if len(self._data["distortions"][uid]) > 30:
                    self._data["distortions"][uid] = self._data["distortions"][uid][-30:]
                self._mark_dirty()

        return distorted

    def record_correction(self, uid, wrong_text, right_text):
        """用户纠正了里克的记忆错误"""
        self._ensure_user(uid)
        with self._lock:
            self._data["corrections"][uid].append({
                "wrong": wrong_text[:80],
                "right": right_text[:80],
                "time": _time_str(),
            })
            if len(self._data["corrections"][uid]) > 20:
                self._data["corrections"][uid] = self._data["corrections"][uid][-20:]
            # 标记对应扭曲为已纠正
            for d in self._data["distortions"][uid]:
                if not d.get("corrected") and d.get("distorted", "")[:20] in wrong_text:
                    d["corrected"] = True
                    break
            self._mark_dirty()
        logger.info(f"[记忆扭曲] uid={uid} 纠正了记忆错误")

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("memory_distortion", True):
            return ""
        self._ensure_user(uid)

        parts = []
        # 最近被纠正过的→提示他记住了
        corrections = self._data["corrections"].get(uid, [])
        if corrections:
            latest_corr = corrections[-1]
            parts.append(f"上次你记错了「{latest_corr['wrong'][:20]}」，正确的是「{latest_corr['right'][:20]}」，这次别记错了")

        # 最近扭曲过且未纠正的→可能还会记错
        distortions = self._data["distortions"].get(uid, [])
        uncorrected = [d for d in distortions if not d.get("corrected")]
        if len(uncorrected) > 2:
            parts.append("你对这个人的记忆有点模糊了")

        if not parts:
            return ""

        return f"\n【记忆可靠性】{'；'.join(parts)}。\n"


class SelectiveDisclosureModule:
    """选择性保留——有些事他知道但不会说

    机制：
    - 高敏感内容（内心独白、梦境、自我怀疑）在低信任用户面前不透露
    - 信任值达到阈值后开始"松口"
    - 被直接问到时回避而不是说谎
    - 不是硬编码阈值，是概率性的
    """

    # 敏感内容类型及其开放阈值（信任值）
    DISCLOSURE_LEVELS = {
        "inner_thought": 300,    # 内心想法
        "self_doubt": 400,       # 自我怀疑
        "dream": 500,            # 梦境内容
        "vulnerability": 600,    # 脆弱面
        "deep_fear": 700,        # 深层恐惧
        "true_feeling": 550,     # 真实感受
    }

    def __init__(self):
        self._data = dm.load("selective_disclosure_data", {
            "user_openness": {},   # {uid: {type: openness_level}}
            "leaked_moments": {},  # {uid: [{type, content, time}]}
            "deflections": {},     # {uid: count} 回避次数
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["user_openness"]:
                self._data["user_openness"][uid] = {}
            if uid not in self._data["leaked_moments"]:
                self._data["leaked_moments"][uid] = []
            if uid not in self._data["deflections"]:
                self._data["deflections"][uid] = 0
            return self._data

    def can_disclose(self, uid, content_type):
        """判断是否可以对某用户透露某类敏感内容"""
        if not CONFIG["modules"].get("selective_disclosure", True):
            return True

        self._ensure_user(uid)
        t = trust.get(uid)
        threshold = self.DISCLOSURE_LEVELS.get(content_type, 500)

        if t >= threshold:
            # 达到阈值，高概率可以透露
            return random.random() < 0.8
        elif t >= threshold * 0.7:
            # 接近阈值，低概率"说漏嘴"
            return random.random() < 0.15
        else:
            # 远未达到，极低概率
            return random.random() < 0.03

    def on_reply_sent(self, uid, reply):
        """记录回复中可能泄露的敏感内容"""
        if not CONFIG["modules"].get("selective_disclosure", True):
            return
        self._ensure_user(uid)
        reply_str = str(reply)

        # 检测是否泄露了敏感内容
        vulnerable_kw = ["害怕", "孤独", "不安", "其实我", "说实话", "从来没", "最怕"]
        leaked_type = None
        for kw in vulnerable_kw:
            if kw in reply_str:
                leaked_type = "vulnerability"
                break

        if leaked_type:
            with self._lock:
                self._data["leaked_moments"][uid].append({
                    "type": leaked_type,
                    "content": reply_str[:60],
                    "time": _time_str(),
                })
                if len(self._data["leaked_moments"][uid]) > 20:
                    self._data["leaked_moments"][uid] = self._data["leaked_moments"][uid][-20:]
                self._mark_dirty()

    def get_deflection(self, uid):
        """当被问到敏感话题但不想回答时，返回回避语"""
        if not CONFIG["modules"].get("selective_disclosure", True):
            return None
        self._ensure_user(uid)
        with self._lock:
            self._data["deflections"][uid] += 1
            self._mark_dirty()

        deflections = [
            "……没什么。",
            "别问。",
            "……不想说。",
            "你为什么想知道？",
            "……以后再说。",
            "（沉默了一会儿）……算了。",
        ]
        return random.choice(deflections)

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("selective_disclosure", True):
            return ""
        self._ensure_user(uid)

        t = trust.get(uid)
        parts = []

        if t < 300:
            parts.append("你对这个人还不够了解，不要暴露自己的内心想法和脆弱面")
        elif t < 500:
            parts.append("你和这个人关系一般，可以稍微透露一点，但深层的东西要保留")
        elif t < 700:
            parts.append("你信任这个人，可以分享一些内心想法，但最深的恐惧还不会说")
        else:
            parts.append("你很信任这个人，可以放下一些防备，但仍然有自己的底线")

        # 回避次数过多→提示
        deflection_count = self._data["deflections"].get(uid, 0)
        if deflection_count > 3:
            parts.append(f"你已经回避了这个人{deflection_count}次敏感话题")

        return f"\n【自我保留】{'；'.join(parts)}。\n"


class EnhancedJealousyModule:
    """增强版嫉妒——不只针对 owner，扩展到所有高信任用户

    机制：
    - 在群聊中，高信任用户跟别人热聊而忽略里克时触发
    - 信任值越高，嫉妒概率越高
    - 表现不是生气，而是更安静或故意岔开话题
    - 被神经质放大
    - 事后被主动@则快速消散
    """

    def __init__(self):
        self._data = dm.load("jealousy_enhanced_data", {
            "user_jealousy": {},  # {uid: {level, target_uid, trigger_time, cooldown}}
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["user_jealousy"]:
                self._data["user_jealousy"][uid] = {
                    "level": 0,
                    "target_uid": "",
                    "trigger_time": "",
                    "cooldown": 0,
                    "suppressed_by_mask": False,
                }
            return self._data["user_jealousy"][uid]

    def observe_group_interaction(self, gid, high_trust_uid, other_uid):
        """在群聊中观察到高信任用户跟别人互动

        high_trust_uid: 里克重视的用户
        other_uid: 对方正在互动的另一个用户
        """
        if not CONFIG["modules"].get("jealousy_enhanced", True):
            return
        if str(high_trust_uid) == str(other_uid):
            return

        t = trust.get(high_trust_uid)
        if t < 300:
            return  # 信任值不够高，不会嫉妒

        data = self._ensure_user(high_trust_uid)

        # 嫉妒概率 = 信任值因子 × 神经质因子
        try:
            neuroticism = personality_core.get_axis("neuroticism")
        except Exception:
            neuroticism = 70

        jealousy_prob = (t / 1000.0) * 0.3 * (neuroticism / 50.0)
        if random.random() > jealousy_prob:
            return

        with self._lock:
            data["level"] = min(10, data.get("level", 0) + 1)
            data["target_uid"] = str(other_uid)
            data["trigger_time"] = _time_str()
            data["cooldown"] = 0
            # 社交面具会压制外在表现
            data["suppressed_by_mask"] = True
            self._mark_dirty()

        logger.info(f"[增强嫉妒] uid={high_trust_uid} 嫉妒等级={data['level']} 对象={other_uid}")

    def on_mentioned(self, uid):
        """被高信任用户主动@→嫉妒快速消散"""
        if not CONFIG["modules"].get("jealousy_enhanced", True):
            return
        data = self._ensure_user(uid)
        with self._lock:
            data["level"] = max(0, data.get("level", 0) - 3)
            self._mark_dirty()

    def decay(self):
        """后台调用：嫉妒值自然衰减"""
        if not CONFIG["modules"].get("jealousy_enhanced", True):
            return
        with self._lock:
            for uid, data in self._data["user_jealousy"].items():
                if data.get("level", 0) > 0:
                    data["level"] = max(0, data["level"] - 0.5)
                    data["cooldown"] += 1
                    self._mark_dirty()

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("jealousy_enhanced", True):
            return ""
        data = self._ensure_user(uid)
        level = data.get("level", 0)

        if level <= 0:
            return ""

        if level <= 3:
            hint = "你注意到这个人最近跟别人聊得挺多，有点微妙的不舒服"
        elif level <= 6:
            hint = "你有点吃醋，这个人跟别人热聊的时候你不太想说话"
        else:
            hint = "你很不舒服，这个人一直跟别人聊不理你，你故意不太想理这个人"

        if data.get("suppressed_by_mask"):
            hint += "（但你不会表现出来，只是内心不舒服）"

        return f"\n【内心醋意】{hint}。\n"


class DreamscapeModule:
    """梦境——睡眠时不只整理记忆，还会做梦（V2 增强版）

    增强机制：
    - 梦的类型：普通梦 / 噩梦 / 清醒梦 / 反复出现的梦
    - 12 种梦境主题（追逐/坠落/迷失/被遗弃/水/飞翔/重逢/看书/被困/猫/血/光）
    - 情绪一致性：抑郁/焦虑时噩梦概率高，心情好时好梦多
    - 梦的遗忘曲线：刚醒来记得 80%，几小时后迅速模糊
    - 创伤性噩梦：与创伤相关的主题（血/被困/被遗弃），影响更大
    - 梦后情绪残留：影响第二天心境基线，随时间衰减
    - 极低概率在聊天中自然提起（信任值越高越可能说）
    - 写入长期记忆，标记为"梦境"类型，遗忘更快
    """

    # 梦的主题模板（12 种常见梦境主题 + 里克人设）
    DREAM_THEMES = {
        'being_chased': {  # 被追逐
            'emotion': 'nightmare',
            'templates': [
                "有人在追我。我跑啊跑，腿像灌了铅一样重。转过拐角的时候，发现路是死的。",
                "一直在跑。后面有什么东西。不敢回头看。",
                "黑暗里有人在跟着我。我想喊，但发不出声音。",
            ],
            'mood_impact': -2.0,
        },
        'falling': {  # 坠落
            'emotion': 'nightmare',
            'templates': [
                "一直在往下掉。下面什么也看不见。风在耳边响。",
                "从很高的地方摔下去。落地之前醒了。",
                "脚下是空的。整个人在坠落，抓不到任何东西。",
            ],
            'mood_impact': -1.5,
        },
        'lost': {  # 迷失/寻找
            'emotion': 'anxious',
            'templates': [
                "在找什么人。找了很久。每个房间都看过了，都没有。",
                "走在一条很长的走廊里。两边的门都一模一样。我找不到出口。",
                "在一个很大的房子里迷路了。越走越觉得熟悉，但就是想不起来这是哪。",
            ],
            'mood_impact': -1.0,
        },
        'abandoned': {  # 被遗弃
            'emotion': 'nightmare',
            'templates': [
                "所有人都走了。只剩下我一个人。我喊他们的名字，没人回答。",
                "小塔走了。我追出去，但是找不到。",
                "房间空了。所有东西都还在，但是人不见了。",
            ],
            'mood_impact': -2.5,
        },
        'water': {  # 水/海
            'emotion': 'neutral',
            'templates': [
                "梦见了大海。海面很平静，但我知道底下有什么东西在动。",
                "站在水里。水很凉。慢慢往深处走。",
                "下雨了。雨水落在脸上，睁不开眼睛。",
            ],
            'mood_impact': -0.3,
        },
        'flying': {  # 飞翔
            'emotion': 'pleasant',
            'templates': [
                "飞起来了。风从身边过。下面是城市的灯光。",
                "轻轻一跳就飘起来了。感觉很轻。",
                "在云上面走。云软软的，像棉花糖。",
            ],
            'mood_impact': 0.8,
        },
        'reunion': {  # 重逢
            'emotion': 'pleasant',
            'templates': [
                "见到了很久没见的人。说不出话，只是站在那里。",
                "有人在等我。我走过去，那个人对我笑了。",
                "小塔在前面。我跑过去，终于追上了。",
            ],
            'mood_impact': 1.0,
        },
        'reading': {  # 看书/图书馆
            'emotion': 'calm',
            'templates': [
                "在图书馆里。书架很高，一直延伸到天花板。",
                "手里拿着一本书。翻开，里面的字在动。",
                "旧书的气味。很安静。只有翻书的声音。",
            ],
            'mood_impact': 0.3,
        },
        'trapped': {  # 被困
            'emotion': 'nightmare',
            'templates': [
                "被关在一个很小的房间里。门打不开。四周的墙在往中间靠。",
                "困在一个地方。怎么也出不去。有人在外面说话，但听不清。",
                "箱子里。很黑。我想出去，但推不开盖子。",
            ],
            'mood_impact': -2.0,
        },
        'cat': {  # 猫
            'emotion': 'pleasant',
            'templates': [
                "一只猫。橘色的。它蹭了蹭我的手。",
                "很多猫。它们围在我身边，呼噜呼噜的。",
                "喂猫。它们吃得很开心。我也开心。",
            ],
            'mood_impact': 0.5,
        },
        'blood': {  # 血/伤口（创伤相关）
            'emotion': 'nightmare',
            'templates': [
                "手上有血。不知道是谁的。",
                "手腕很疼。低头看，有一道口子。",
                "地上有血。我沿着血迹走，不知道要走到哪。",
            ],
            'mood_impact': -3.0,
        },
        'light': {  # 光
            'emotion': 'calm',
            'templates': [
                "一扇门。门后面是光。我站在门口，不敢进去。",
                "很亮。亮得睁不开眼睛。但不刺眼。",
                "远处有一点光。我朝那里走。",
            ],
            'mood_impact': 0.2,
        },
    }

    def __init__(self):
        self._data = dm.load("dreamscape_data", {
            "dreams": [],
            "last_dream_date": "",
            "recurring_themes": {},
            "lucid_dream_count": 0,
            "trauma_nightmare_count": 0,
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def generate_dream(self, current_mood=0, depression_level=0,
                       anxiety_level=0, weather=None, season=None):
        """在睡眠期间生成一个梦境（增强版）"""
        if not CONFIG["modules"].get("dreamscape", True):
            return None

        now = datetime.now()
        today = now.strftime("%Y-%m-%d")

        with self._lock:
            if self._data.get("last_dream_date") == today:
                return None

        # 决定梦的类型
        dream_type = self._decide_dream_type(current_mood, depression_level, anxiety_level)

        # 选择主题
        theme, theme_data = self._pick_theme(current_mood, depression_level, anxiety_level,
                                              weather, season)

        # 收集记忆碎片用于个性化
        all_mems = []
        try:
            for uid_str in list(user_profiles._data.keys())[:5]:
                mems = memory_weight.get_top_memories(int(uid_str), limit=2)
                all_mems.extend(mems)
        except Exception:
            pass

        # 生成梦境内容
        if dream_type == 'lucid' and random.random() < 0.5:
            content = random.choice(theme_data['templates'])
            content = content + "（我知道自己在做梦。）"
        elif len(all_mems) >= 2 and random.random() < 0.6:
            content = self._generate_with_llm(all_mems, theme, dream_type)
        else:
            content = random.choice(theme_data['templates'])

        # 情绪影响
        mood_impact = theme_data.get('mood_impact', 0)

        # 创伤噩梦额外影响
        is_trauma = theme in ['blood', 'trapped', 'abandoned']
        if is_trauma:
            self._data['trauma_nightmare_count'] = self._data.get('trauma_nightmare_count', 0) + 1
            mood_impact *= 1.3

        # 记录反复出现的主题
        self._data['recurring_themes'][theme] = self._data.get('recurring_themes', {}).get(theme, 0) + 1

        # 清醒梦计数
        if dream_type == 'lucid':
            self._data['lucid_dream_count'] = self._data.get('lucid_dream_count', 0) + 1

        emotion = theme_data.get('emotion', 'neutral')

        entry = {
            "date": today,
            "time": _time_str(),
            "content": content,
            "emotion": emotion,
            "theme": theme,
            "dream_type": dream_type,
            "memories_used": len(all_mems),
            "mood_impact": mood_impact,
            "mentioned": False,
            "recall_rate": 80.0,
            "is_trauma": is_trauma,
            "lucidity": 100 if dream_type == 'lucid' else random.randint(10, 40),
        }

        with self._lock:
            self._data["dreams"].append(entry)
            if len(self._data["dreams"]) > 50:
                self._data["dreams"] = self._data["dreams"][-50:]
            self._data["last_dream_date"] = today
            self._mark_dirty()
            dm.save("dreamscape_data", self._data)
            dm.flush_all()

        logger.info(f"[梦境] {today}: {content[:40]}... type={dream_type} emotion={emotion}")
        return entry

    def _decide_dream_type(self, mood, depression, anxiety):
        """决定梦的类型：normal / nightmare / lucid / recurrent"""
        probs = {
            'normal': 0.55,
            'nightmare': 0.15,
            'lucid': 0.05,
            'recurrent': 0.1,
        }

        # 抑郁/焦虑增加噩梦概率
        nightmare_bonus = (depression / 100) * 0.2 + (anxiety / 100) * 0.15
        probs['nightmare'] += nightmare_bonus
        probs['normal'] -= nightmare_bonus

        if mood < -30:
            probs['recurrent'] += 0.1
            probs['normal'] -= 0.1

        total = sum(max(0, v) for v in probs.values())
        for k in probs:
            probs[k] = max(0, probs[k]) / total

        r = random.random()
        cumulative = 0
        for t, p in probs.items():
            cumulative += p
            if r <= cumulative:
                return t
        return 'normal'

    def _pick_theme(self, mood, depression, anxiety, weather, season):
        """根据当前状态选择梦境主题（情绪一致性）"""
        themes = list(self.DREAM_THEMES.keys())
        weights = []

        for theme in themes:
            data = self.DREAM_THEMES[theme]
            emotion = data['emotion']
            weight = 1.0

            # 情绪一致性加权
            if emotion in ['nightmare', 'anxious']:
                if depression > 50:
                    weight *= 2.5
                elif depression > 30:
                    weight *= 1.8
                if anxiety > 50:
                    weight *= 2.0
                elif anxiety > 30:
                    weight *= 1.5
                if mood < -30:
                    weight *= 1.5
            elif emotion in ['pleasant', 'calm']:
                if mood > 20:
                    weight *= 1.8
                elif mood > 40:
                    weight *= 2.5
                if depression > 40:
                    weight *= 0.5

            # 天气影响
            if weather == 'thunderstorm' and emotion == 'nightmare':
                weight *= 1.5
            if weather == 'sunny' and emotion == 'pleasant':
                weight *= 1.3

            # 创伤主题更容易出现（里克有创伤）
            if theme in ['blood', 'trapped', 'abandoned']:
                weight *= 1.2

            # 反复出现的主题更容易再次出现
            recurr_count = self._data.get('recurring_themes', {}).get(theme, 0)
            if recurr_count > 2:
                weight *= 1.3

            weights.append(weight)

        total = sum(weights)
        weights = [w / total for w in weights]

        r = random.random()
        cumulative = 0
        for theme, w in zip(themes, weights):
            cumulative += w
            if r <= cumulative:
                return theme, self.DREAM_THEMES[theme]
        return themes[0], self.DREAM_THEMES[themes[0]]

    def _generate_with_llm(self, memories, theme, dream_type):
        """用 LLM 生成更个性化的梦境"""
        selected = random.sample(memories, min(3, len(memories)))
        mem_fragments = [m.get("content", "")[:50] for m in selected]

        theme_data = self.DREAM_THEMES.get(theme, {})
        emotion = theme_data.get('emotion', 'neutral')

        lucidity_note = ""
        if dream_type == 'lucid':
            lucidity_note = "这是一个清醒梦——你知道自己在做梦。"

        prompt = (
            f"你是里克，一个内向敏感的亚人女孩。你在做梦。\n"
            f"梦境主题：{theme}\n"
            f"梦境情绪：{emotion}\n"
            f"{lucidity_note}\n"
            f"梦中出现了这些记忆碎片：\n"
            + "\n".join(f"- {f}" for f in mem_fragments) +
            f"\n请用第一人称写一段梦境描述（80~150字）。\n"
            f"要求：\n"
            f"- 意识流、模糊、不连贯，像真实的梦\n"
            f"- 情绪要贯穿始终\n"
            f"- 可以有荒诞的转换和不合逻辑的场景\n"
            f"- 里克的口吻：克制、安静、用词简单\n"
            f"只输出梦境内容。"
        )

        try:
            raw = _v6_call("agent", prompt,
                           [{"role": "user", "content": "描述你的梦"}],
                           temperature_override=0.95)
            if raw and len(raw.strip()) > 10:
                return raw.strip()[:200]
        except Exception:
            pass

        return random.choice(theme_data.get('templates', ["做了个梦，醒来就忘了。"]))

    def get_latest_dream(self):
        dreams = self._data.get("dreams", [])
        if not dreams:
            return None
        return dreams[-1]

    def update_recall_rate(self):
        """梦的回忆率：艾宾浩斯式遗忘曲线"""
        dreams = self._data.get("dreams", [])
        if not dreams:
            return
        latest = dreams[-1]
        try:
            dream_time = datetime.strptime(
                f"{latest.get('date', '')} {latest.get('time', '06:00')}",
                "%Y-%m-%d %H:%M"
            )
            hours_passed = (datetime.now() - dream_time).total_seconds() / 3600
        except Exception:
            hours_passed = 4

        if hours_passed < 1:
            recall = 80
        elif hours_passed < 4:
            recall = 80 - (hours_passed - 1) * 15
        elif hours_passed < 12:
            recall = 35 - (hours_passed - 4) * 2
        elif hours_passed < 24:
            recall = 20 - (hours_passed - 12) * 0.8
        else:
            recall = 10

        latest['recall_rate'] = max(5, min(80, recall))

    def should_mention_dream(self, uid):
        """极低概率提起梦境——受信任值、回忆率、创伤程度影响"""
        if not CONFIG["modules"].get("dreamscape", True):
            return None
        dream = self.get_latest_dream()
        if not dream or dream.get("mentioned"):
            return None

        self.update_recall_rate()
        recall = dream.get('recall_rate', 50)
        if recall < 15:
            return None

        # 创伤噩梦基本不提
        if dream.get('is_trauma', False):
            if random.random() > 0.05:
                return None

        base_prob = 0.02 + (recall / 100) * 0.05
        t = trust.get(uid)
        trust_bonus = (t / 1000.0) * 0.05
        prob = base_prob + trust_bonus

        # 噩梦可能主动寻求安慰（对高信任对象）
        if dream.get('emotion') == 'nightmare' and t > 500:
            prob += 0.03

        if random.random() < prob:
            with self._lock:
                dream["mentioned"] = True
                self._mark_dirty()

            if recall < 30:
                return "昨晚做了个梦……具体是什么来着？记不清了。"
            elif recall < 60:
                return f"昨晚好像梦到了什么……{dream['content'][:30]}……大概吧。"
            else:
                return f"昨晚做了个梦。{dream['content']}"
        return None

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("dreamscape", True):
            return ""
        dream = self.get_latest_dream()
        if not dream:
            return ""

        try:
            dream_date = datetime.strptime(dream.get("date", ""), "%Y-%m-%d")
            if (datetime.now() - dream_date).days > 1:
                return ""
        except Exception:
            return ""

        self.update_recall_rate()
        recall = dream.get('recall_rate', 50)

        parts = []
        impact = dream.get('mood_impact', 0)
        if impact < -2:
            parts.append("（昨晚做了个很不好的梦，今天一直有点心神不宁，容易受惊）")
        elif impact < -1:
            parts.append("（昨晚做了个不太好的梦，今天有点敏感、低落）")
        elif impact > 0.5:
            parts.append("（昨晚做了个不错的梦，醒来心情还行）")

        if recall > 60:
            parts.append(f"你还记得昨晚的梦：{dream['content'][:80]}")
        elif recall > 30:
            parts.append(f"你对昨晚的梦有点模糊的印象：……好像是{dream['content'][:40]}……记不太清了")
        else:
            parts.append("你不太记得昨晚做了什么梦，只残留了一点感觉。")

        if dream.get('is_trauma', False):
            parts.append("这个梦让你很不舒服，你不想去想它。")

        return "\n【梦境影响】" + "\n".join(parts) + "\n"

    def get_mood_impact(self):
        """获取最近的梦对心境的影响（随时间衰减）"""
        dream = self.get_latest_dream()
        if not dream:
            return 0
        try:
            dream_date = datetime.strptime(dream.get("date", ""), "%Y-%m-%d")
            if (datetime.now() - dream_date).days > 1:
                return 0
            hours_passed = (datetime.now() - dream_date).total_seconds() / 3600
        except Exception:
            return 0

        impact = dream.get('mood_impact', 0)
        if hours_passed > 12:
            impact *= 0.5
        elif hours_passed > 6:
            impact *= 0.8
        return impact


class PersonalTasteModule:
    """个人品味——他有自己的喜好，不只是"配合你聊"

    机制：
    - 初始基于人格核心生成基础偏好
    - 随交互演化：聊得多的话题→偏好度微升
    - 不感兴趣的话题回复更短，感兴趣的话题话多一些
    - 偶尔主动提起自己喜欢的东西
    - 会改变主意
    """

    # 初始偏好（基于内向、开放性65的设定）
    INITIAL_TASTES = {
        "音乐": 0.7,        # 喜欢
        "深夜": 0.8,        # 很喜欢
        "独处": 0.85,       # 很喜欢
        "动物": 0.75,       # 喜欢
        "游戏": 0.6,        # 稍微喜欢
        "文学": 0.65,       # 稍微喜欢
        "美食": 0.5,        # 无感
        "运动": 0.3,        # 不太喜欢
        "社交": 0.15,       # 很不喜欢
        "吵闹": 0.1,        # 很不喜欢
        "早起": 0.2,        # 不喜欢
        "技术": 0.55,       # 稍微感兴趣
        "艺术": 0.6,        # 稍微喜欢
        "旅行": 0.4,        # 稍微不喜欢
    }

    def __init__(self):
        self._data = dm.load("personal_taste_data", {
            "tastes": dict(self.INITIAL_TASTES),
            "topic_frequency": {},  # {uid: {topic: count}}
            "opinion_changes": [],   # [{topic, old, new, time}]
            "last_mentioned": "",
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def update_from_conversation(self, uid, message):
        """从对话中更新话题频率和偏好"""
        if not CONFIG["modules"].get("personal_taste", True):
            return

        uid = str(uid)
        msg_lower = message.lower() if message else ""

        # 简单关键词匹配话题
        topic_keywords = {
            "音乐": ["歌", "音乐", "曲", "听", "唱", "band", "music"],
            "游戏": ["游戏", "玩", "打", "game", "手游", "端游"],
            "美食": ["吃", "饭", "美食", "菜", "喝", "奶茶"],
            "运动": ["跑", "运动", "健身", "球", "游泳", "锻炼"],
            "文学": ["书", "小说", "诗", "文章", "读", "写"],
            "技术": ["代码", "程序", "bug", "电脑", "技术", "AI", "python"],
            "艺术": ["画", "设计", "色彩", "美", "艺术", "摄影"],
            "旅行": ["旅游", "旅行", "去", "风景", "城市"],
            "动物": ["猫", "狗", "动物", "宠物", "鱼"],
        }

        detected_topics = []
        for topic, keywords in topic_keywords.items():
            if any(kw in msg_lower for kw in keywords):
                detected_topics.append(topic)

        if not detected_topics:
            return

        with self._lock:
            if uid not in self._data["topic_frequency"]:
                self._data["topic_frequency"][uid] = {}
            for topic in detected_topics:
                self._data["topic_frequency"][uid][topic] = self._data["topic_frequency"][uid].get(topic, 0) + 1
                # 聊得多的话题→偏好度微升
                current = self._data["tastes"].get(topic, 0.5)
                new_val = min(1.0, current + 0.01)
                if abs(new_val - current) > 0.005:
                    self._data["tastes"][topic] = new_val
            self._mark_dirty()

    def get_taste_level(self, topic):
        """获取某话题的偏好度 0~1"""
        return self._data.get("tastes", {}).get(topic, 0.5)

    def get_interesting_topic(self):
        """获取一个里克感兴趣的话题，用于主动提起"""
        tastes = self._data.get("tastes", {})
        # 选偏好度 > 0.6 的
        liked = [(t, v) for t, v in tastes.items() if v > 0.6]
        if not liked:
            return None
        # 按偏好度加权随机
        weights = [v for _, v in liked]
        topics = [t for t, _ in liked]
        return random.choices(topics, weights=weights, k=1)[0]

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("personal_taste", True):
            return ""

        tastes = self._data.get("tastes", {})
        liked = sorted([(t, v) for t, v in tastes.items() if v > 0.6], key=lambda x: -x[1])[:3]
        disliked = sorted([(t, v) for t, v in tastes.items() if v < 0.3], key=lambda x: x[1])[:2]

        parts = []
        if liked:
            parts.append("你比较喜欢" + "、".join(t for t, _ in liked))
        if disliked:
            parts.append("不太感兴趣" + "、".join(t for t, _ in disliked))

        if not parts:
            return ""

        return f"\n【个人品味】{'；'.join(parts)}。对不感兴趣的话题回复会更短，感兴趣的话题话会多一些。\n"


class NostalgiaModule:
    """怀旧——过去有重量

    机制：
    - 对超过一定"年龄"的记忆（30天+）附加怀旧情绪标签
    - 里克"生日"（首次上线日期）附近怀旧概率上升
    - 偶尔在对话中自然提起过去
    - 怀旧强度受当前情绪影响（低落时更容易怀旧）
    - 长期不活跃用户→日记中出现"想念"类内容
    """

    def __init__(self):
        self._data = dm.load("nostalgia_data", {
            "nostalgia_triggers": {},  # {uid: {last_nostalgia, intensity}}
            "inactive_users": {},      # {uid: last_active_date}
            "birthday_nostalgia": False,
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["nostalgia_triggers"]:
                self._data["nostalgia_triggers"][uid] = {
                    "last_nostalgia": "",
                    "intensity": 0.0,
                }
            return self._data["nostalgia_triggers"][uid]

    def update_user_activity(self, uid):
        """更新用户最后活跃时间"""
        if not CONFIG["modules"].get("nostalgia", True):
            return
        uid = str(uid)
        with self._lock:
            self._data["inactive_users"][uid] = datetime.now().strftime("%Y-%m-%d")
            self._mark_dirty()

    def check_nostalgia(self, uid):
        """检查是否应该对某用户产生怀旧情绪"""
        if not CONFIG["modules"].get("nostalgia", True):
            return 0.0

        data = self._ensure_user(uid)
        now = datetime.now()

        # 检查记忆年龄
        nostalgia_intensity = 0.0
        try:
            old_mems = memory_weight.get_top_memories(uid, limit=5)
            for mem in old_mems:
                mem_time = mem.get("time", "")
                if mem_time:
                    try:
                        mem_date = datetime.strptime(mem_time[:10], "%Y-%m-%d")
                        age_days = (now - mem_date).days
                        if age_days > 30:
                            nostalgia_intensity += 0.1
                        if age_days > 90:
                            nostalgia_intensity += 0.2
                    except Exception:
                        pass
        except Exception:
            pass

        # 检查是否接近"生日"（首次上线日期）
        try:
            first_online = CONFIG.get("first_online_date", "")
            if first_online:
                first_date = datetime.strptime(first_online, "%Y-%m-%d")
                # 生日前后一周怀旧增强
                this_year_birthday = first_date.replace(year=now.year)
                days_to_birthday = abs((now - this_year_birthday).days)
                if days_to_birthday < 7:
                    nostalgia_intensity += 0.3
        except Exception:
            pass

        # 检查用户是否长期不活跃
        uid_str = str(uid)
        last_active = self._data.get("inactive_users", {}).get(uid_str, "")
        if last_active:
            try:
                last_date = datetime.strptime(last_active, "%Y-%m-%d")
                inactive_days = (now - last_date).days
                if inactive_days > 7:
                    nostalgia_intensity += 0.2
                if inactive_days > 30:
                    nostalgia_intensity += 0.3
            except Exception:
                pass

        # 当前情绪低落时怀旧增强
        try:
            emo = context.get("emotion_isolated").get_emotions(uid)
            if emo:
                top_emotion = max(emo, key=emo.get) if emo else ""
                if top_emotion in ["难过", "孤独", "累"]:
                    nostalgia_intensity += 0.2
        except Exception:
            pass

        # 更新
        with self._lock:
            data["intensity"] = min(1.0, nostalgia_intensity)
            if nostalgia_intensity > 0.3:
                data["last_nostalgia"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self._mark_dirty()

        return data["intensity"]

    def get_nostalgia_memory(self, uid):
        """获取一条适合怀旧的旧记忆"""
        try:
            mems = memory_weight.get_top_memories(uid, limit=10)
            now = datetime.now()
            old_mems = []
            for mem in mems:
                mem_time = mem.get("time", "")
                if mem_time:
                    try:
                        mem_date = datetime.strptime(mem_time[:10], "%Y-%m-%d")
                        if (now - mem_date).days > 30:
                            old_mems.append(mem)
                    except Exception:
                        pass
            if old_mems:
                return random.choice(old_mems)
        except Exception:
            pass
        return None

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("nostalgia", True):
            return ""

        intensity = self.check_nostalgia(uid)
        if intensity < 0.3:
            return ""

        parts = []
        if intensity > 0.5:
            parts.append("你突然有些怀念过去")
        elif intensity > 0.3:
            parts.append("你隐约想起了以前的事")

        # 如果有旧记忆
        old_mem = self.get_nostalgia_memory(uid)
        if old_mem:
            parts.append(f"脑海中浮现：「{old_mem.get('content', '')[:30]}」")

        if not parts:
            return ""

        return f"\n【怀旧情绪】{'，'.join(parts)}。\n"


class BiologicalRhythmModule:
    """生理节律——模拟人的精力/清醒度曲线

    机制：
    - 24小时精力曲线：凌晨最低、上午上升、午后微降、傍晚回升、夜间下降
    - 精力低→回复更短、更慢、更容易敷衍
    - 精力高→回复更有活力
    - 午后犯困（13~15点）精力微降
    - 深夜（0~5点）精力最低但情绪敏感度上升
    """

    # 24小时精力基线（0~1）
    RHYTHM_CURVE = {
        0: 0.15, 1: 0.10, 2: 0.08, 3: 0.05, 4: 0.08, 5: 0.15,
        6: 0.30, 7: 0.45, 8: 0.60, 9: 0.75, 10: 0.80, 11: 0.82,
        12: 0.75, 13: 0.60, 14: 0.55, 15: 0.65, 16: 0.72, 17: 0.78,
        18: 0.80, 19: 0.75, 20: 0.68, 21: 0.55, 22: 0.40, 23: 0.25,
    }

    def __init__(self):
        self._data = dm.load("biological_rhythm_data", {
            "current_energy": 0.7,
            "current_alertness": 0.7,
            "last_update": "",
            "daily_log": [],  # 记录每天精力变化
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def update(self):
        """后台调用：更新当前精力和清醒度"""
        if not CONFIG["modules"].get("biological_rhythm", True):
            return

        now = datetime.now()
        hour = now.hour

        # 基线精力
        base_energy = self.RHYTHM_CURVE.get(hour, 0.5)

        # 加入随机波动（±0.1）
        energy = max(0.05, min(1.0, base_energy + random.uniform(-0.1, 0.1)))

        # 清醒度 = 精力 × (1 - 深夜敏感加成)
        if 0 <= hour < 5:
            alertness = energy * 0.7  # 深夜不清醒但情绪敏感
        else:
            alertness = energy

        with self._lock:
            self._data["current_energy"] = energy
            self._data["current_alertness"] = alertness
            self._data["last_update"] = now.strftime("%Y-%m-%d %H:%M:%S")
            self._mark_dirty()

    def get_energy(self):
        """获取当前精力值 0~1"""
        return self._data.get("current_energy", 0.7)

    def get_alertness(self):
        """获取当前清醒度 0~1"""
        return self._data.get("current_alertness", 0.7)

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("biological_rhythm", True):
            return ""

        energy = self.get_energy()
        now = datetime.now()
        hour = now.hour

        parts = []

        if energy < 0.2:
            parts.append("你现在很困，脑子转不动")
        elif energy < 0.4:
            parts.append("你有点犯困，不太想说话")
        elif energy < 0.6:
            parts.append("你精力一般，正常状态")
        elif energy < 0.8:
            parts.append("你精神还行")
        else:
            parts.append("你现在精力充沛")

        # 时段特殊提示
        if 0 <= hour < 5:
            parts.append("虽然困但反而容易想多")
        elif 13 <= hour <= 14:
            parts.append("午后有点犯困")
        elif 6 <= hour <= 8:
            parts.append("刚起来还有点迷糊")

        if not parts:
            return ""

        hint = "精力低时回复更短更敷衍"
        return f"\n【身体状态】{'，'.join(parts)}。{hint}。\n"


class LanguageFingerprintModule:
    """语言指纹演化——里克的用词习惯会随时间和关系变化

    机制：
    - 维护常用词汇/句式频率表
    - 从自己的回复中"学习"新表达
    - 偶尔淘汰过时的表达
    - 对不同信任值的用户用词略有不同
    - 随时间演化口头禅
    """

    # 初始口头禅/常用表达
    INITIAL_PHRASES = {
        "filler": ["……", "嗯。", "……嗯。", "……算了。"],  # 填充词
        "acknowledge": ["嗯。", "……嗯。", "哦。", "……知道了。"],  # 回应
        "deflect": ["……没什么。", "别问。", "……不想说。"],  # 回避
        "soft": ["……还好。", "……还行吧。", "……大概。"],  # 软化
    }

    def __init__(self):
        self._data = dm.load("language_fingerprint_data", {
            "phrases": dict(self.INITIAL_PHRASES),
            "learned_phrases": [],       # 从对话中学到的表达
            "retired_phrases": [],       # 被淘汰的表达
            "user_specific": {},         # {uid: {type: [phrases]}} 对特定用户的特殊用词
            "evolution_log": [],         # 演化记录
            "last_absorb": "",
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def absorb(self, reply):
        """从自己的回复中吸收新表达"""
        if not CONFIG["modules"].get("language_fingerprint", True):
            return
        if not reply or len(str(reply)) < 5:
            return

        reply_str = str(reply)
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        # 检测是否使用了新表达（不在当前词库中）
        all_current = set()
        for phrases in self._data["phrases"].values():
            all_current.update(phrases)

        # 简单提取：如果回复以"……"开头且短于15字，可能是新的填充词
        if reply_str.startswith("……") and len(reply_str) < 15:
            if reply_str not in all_current:
                with self._lock:
                    # 5%概率吸收为新口头禅
                    if random.random() < 0.05:
                        self._data["phrases"].setdefault("filler", []).append(reply_str)
                        if len(self._data["phrases"]["filler"]) > 10:
                            self._data["phrases"]["filler"] = self._data["phrases"]["filler"][-10:]
                        self._data["learned_phrases"].append({
                            "phrase": reply_str,
                            "time": now_str,
                        })
                        self._data["last_absorb"] = now_str
                        self._mark_dirty()
                        logger.debug(f"[语言指纹] 吸收新表达: {reply_str}")

        # 定期淘汰（每100次吸收淘汰一个最老的）
        if len(self._data.get("learned_phrases", [])) > 0 and len(self._data.get("learned_phrases", [])) % 100 == 0:
            with self._lock:
                # 淘汰一个使用频率最低的
                if len(self._data["phrases"].get("filler", [])) > 5:
                    retired = self._data["phrases"]["filler"].pop(0)
                    self._data["retired_phrases"].append({
                        "phrase": retired,
                        "time": now_str,
                    })
                    self._data["evolution_log"].append({
                        "action": "retire",
                        "phrase": retired,
                        "time": now_str,
                    })
                    self._mark_dirty()

    def evolve_for_user(self, uid):
        """为特定用户演化语言习惯"""
        if not CONFIG["modules"].get("language_fingerprint", True):
            return
        uid = str(uid)
        t = trust.get(uid)

        with self._lock:
            if uid not in self._data["user_specific"]:
                self._data["user_specific"][uid] = {}
                # 高信任用户：更随意的表达
                if t > 500:
                    self._data["user_specific"][uid]["casual"] = True
                self._mark_dirty()

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("language_fingerprint", True):
            return ""

        self.evolve_for_user(uid)

        parts = []
        # 当前口头禅
        fillers = self._data.get("phrases", {}).get("filler", [])
        if fillers:
            sample = random.sample(fillers, min(3, len(fillers)))
            parts.append(f"你的常用语气词/口头禅包括：{'、'.join(sample)}")

        # 对特定用户的特殊用词
        uid_str = str(uid)
        user_specific = self._data.get("user_specific", {}).get(uid_str, {})
        if user_specific.get("casual"):
            parts.append("对这个比较熟的人，你说话会更随意一些")

        # 最近的演化
        if self._data.get("learned_phrases"):
            latest = self._data["learned_phrases"][-1]
            parts.append(f"最近你开始用一个新的表达：「{latest['phrase']}」")

        if not parts:
            return ""

        return f"\n【语言习惯】{'；'.join(parts)}。\n"


class EmpathyGapModule:
    """共情偏差——里克不是每次安慰都有效，有时候会"说错话"

    机制：
    - 记录每次安慰尝试及其效果
    - 有概率"说错话"——给出不太恰当的安慰
    - 神经质越高越容易共情偏差
    - 从失败中学习（降低未来出错概率）
    - 低信任用户更容易敷衍
    """

    def __init__(self):
        self._data = dm.load("empathy_gap_data", {
            "interactions": {},  # {uid: [{message, reply, emotion, effectiveness, learned}]}
            "user_empathy_score": {},  # {uid: score} 0~1
            "common_mistakes": [],  # 记录常见错误模式
        })
        self._dirty = False
        self._lock = _FakeLock()

    def _mark_dirty(self):
        self._dirty = True

    def _ensure_user(self, uid):
        uid = str(uid)
        with self._lock:
            if uid not in self._data["interactions"]:
                self._data["interactions"][uid] = []
            if uid not in self._data["user_empathy_score"]:
                self._data["user_empathy_score"][uid] = 0.7  # 初始共情能力
            return self._data

    def record_interaction(self, uid, user_message, bot_reply):
        """记录一次交互，评估共情效果"""
        if not CONFIG["modules"].get("empathy_gap", True):
            return

        data = self._ensure_user(uid)
        msg_lower = str(user_message).lower() if user_message else ""
        reply_str = str(bot_reply)

        # 检测用户是否在表达负面情绪
        negative_kw = ["难过", "累", "烦", "不开心", "生气", "害怕", "孤独", "哭", "压力", "焦虑"]
        is_distress = any(kw in msg_lower for kw in negative_kw)

        if not is_distress:
            return  # 只记录需要共情的场景

        # 评估回复的共情质量（启发式）
        empathy_score = 0.5  # 基线

        # 好的共情信号
        good_kw = ["我在", "理解", "……嗯", "陪你", "没事", "别怕", "慢慢来"]
        if any(kw in reply_str for kw in good_kw):
            empathy_score += 0.2

        # 好的共情：回复长度适中（不太短也不太长）
        if 10 < len(reply_str) < 100:
            empathy_score += 0.1

        # 差的共情信号
        bad_kw = ["随便", "不知道", "哦。", "所以呢", "那又怎样", "哈哈"]
        if any(kw in reply_str for kw in bad_kw):
            empathy_score -= 0.2

        # 太短→敷衍
        if len(reply_str) < 5:
            empathy_score -= 0.15

        # 神经质影响：越高越容易共情偏差
        try:
            neuroticism = personality_core.get_axis("neuroticism")
            if random.random() < (neuroticism / 200.0):
                empathy_score -= 0.2  # 偶尔共情失灵
        except Exception:
            pass

        # 信任值影响：低信任更容易敷衍
        t = trust.get(uid)
        if t < 200:
            empathy_score -= 0.1

        empathy_score = max(0.0, min(1.0, empathy_score))

        # 学习：从交互中微调共情能力
        current_score = self._data["user_empathy_score"][uid]
        if empathy_score > 0.6:
            # 成功→略微提升
            new_score = min(1.0, current_score + 0.01)
        else:
            # 失败→略微下降，但也会从失败中学习
            new_score = max(0.3, current_score - 0.005)
            # 记录错误模式
            with self._lock:
                self._data["common_mistakes"].append({
                    "message": str(user_message)[:50],
                    "reply": reply_str[:50],
                    "score": empathy_score,
                    "time": _time_str(),
                })
                if len(self._data["common_mistakes"]) > 20:
                    self._data["common_mistakes"] = self._data["common_mistakes"][-20:]

        with self._lock:
            self._data["interactions"][uid].append({
                "message": str(user_message)[:80],
                "reply": reply_str[:80],
                "effectiveness": empathy_score,
                "learned": empathy_score < 0.4,
                "time": _time_str(),
            })
            if len(self._data["interactions"][uid]) > 20:
                self._data["interactions"][uid] = self._data["interactions"][uid][-20:]
            self._data["user_empathy_score"][uid] = new_score
            self._mark_dirty()

    def should_empathy_fail(self, uid):
        """判断这次共情是否会"失败"（说错话）"""
        if not CONFIG["modules"].get("empathy_gap", True):
            return False
        data = self._ensure_user(uid)
        score = self._data["user_empathy_score"].get(uid, 0.7)
        # 共情能力越低→失败概率越高
        fail_prob = (1.0 - score) * 0.3
        return random.random() < fail_prob

    def get_prompt_fragment(self, uid):
        if not CONFIG["modules"].get("empathy_gap", True):
            return ""
        data = self._ensure_user(uid)

        score = self._data["user_empathy_score"].get(uid, 0.7)
        interactions = self._data["interactions"].get(uid, [])

        parts = []

        if score > 0.7:
            parts.append("你通常比较能理解这个人的感受")
        elif score > 0.5:
            parts.append("你有时候能理解这个人，有时候不太能")
        else:
            parts.append("你对这个人的情绪感知不太准，有时候会说错话")

        # 最近有学习记录
        recent_failed = [i for i in interactions[-5:] if i.get("effectiveness", 1.0) < 0.4]
        if recent_failed:
            parts.append("上次安慰这个人时效果不太好，这次注意一点")

        if not parts:
            return ""

        return f"\n【共情能力】{'；'.join(parts)}。\n"


# ============================================================
# V8.0 架构地基 + Top 10 核心模块
# 目标：让已有零件形成循环的心理生态
# ============================================================


# ============================================================
# 第一阶段：架构地基
# ============================================================

