#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""记忆/时间/情绪/心理核心（从 src_k_memory_time.py 迁移）。

包含：PsychologyCore、SemanticRetriever、EmotionMemoryLink、MemoryGateway、
EmotionModel、UnifiedEmotionCore、WebSearchEngine、ImageRecognition、NTPSyncClock，
以及 V8/V9 模块实例化。
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
import email.utils
import websocket
import requests
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from collections import defaultdict, deque
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

from qqagent.core import CONFIG, logger, dm
from qqagent.core import context
from qqagent.core.utils import _PSYCH_CORE_AVAILABLE, _FakeLock
from qqagent.memory.life_v10 import *  # noqa: F401,F403  (导入所有 L1-L9 子系统类)

class PsychologyCore:
    """
    心理学核心引擎 —— 塔洛斯·里克的深层心理架构

    10层架构：
    L1 认知系统 (CognitiveSystem)
    L2 情绪系统 (DeepEmotionSystem)
    L3 人格自我 (PersonalitySelfSystem)
    L4 社会影响 (SocialInfluenceSystem)
    L5 动机奖赏 (MotivationRewardSystem)
    L6 临床存在 (ClinicalExistentialSystem)
    L7 积极心理 (PositivePsychSystem)
    L8 进化心理 (EvolutionarySystem)
    L9 发展心理 (DevelopmentalSystem)
    """

    def __init__(self):
        # L1 认知
        self.cognitive = CognitiveSystem()

        # L2 情绪
        self.emotion = DeepEmotionSystem()

        # L3 人格与自我
        self.personality = PersonalitySelfSystem()

        # L4 社会影响
        self.social = SocialInfluenceSystem()

        # L5 动机与奖赏
        self.motivation = MotivationRewardSystem()

        # L6 临床与存在
        self.clinical = ClinicalExistentialSystem()

        # L7 积极心理
        self.positive = PositivePsychSystem()

        # L8 进化心理
        self.evolutionary = EvolutionarySystem()

        # L9 发展心理
        self.developmental = DevelopmentalSystem()

        # 全局状态
        self.activation_level = 50.0    # 整体心理激活水平
        self.tick_count = 0

    def process_interaction(self, uid, message, message_type="chat",
                            scope="private", gid=None, context=None):
        """
        处理一次交互事件，全系统联动
        返回完整的心理状态快照
        """
        context = context or {}

        # 1. 认知处理
        complexity = min(len(message) / 200, 1.0)
        cognitive_result = self.cognitive.process_event(
            event_type=message_type,
            event_detail=message[:100],
            complexity=complexity,
            time_pressure=context.get('time_pressure', 0.2),
            emotion_intensity=context.get('emotion_intensity', 0.3),
            context_factors=context.get('cognitive_factors', {}),
        )

        # 2. 情绪处理
        emotion_intensity = context.get('emotion_intensity', 30)
        emotion_type = context.get('emotion_type', 'trust')
        emotion_result = self.emotion.process_emotion(
            emotion_type=emotion_type,
            intensity=emotion_intensity,
            reason=message[:50],
            cognitive_resources=self.cognitive.dual_process.system2_energy,
            social_context=scope,
        )

        # 3. 自我威胁评估
        threat_type = context.get('threat_type')
        threat_result = None
        if threat_type:
            threat_result = self.personality.evaluate_threat(
                threat_type=threat_type,
                intensity=context.get('threat_intensity', 40),
                domain=context.get('threat_domain', 'general'),
            )

        # 4. 奖赏处理
        reward_type = context.get('reward_type')
        reward_result = None
        if reward_type:
            reward_result = self.motivation.process_reward_event(
                reward_type=reward_type,
                magnitude=context.get('reward_magnitude', 50),
                expected=context.get('reward_expected', 40),
            )

        # 5. 积极心理更新
        if context.get('positive_event'):
            self.positive.process_positive_event(
                event_type=context.get('positive_type', 'social_connection'),
                intensity=context.get('positive_intensity', 40),
                domain=message[:30],
            )

        # 6. 存在层面（深层交互触发）
        if context.get('existential_trigger'):
            self.clinical.existential.trigger_concern(
                context['existential_trigger'],
                context.get('existential_intensity', 20),
            )

        # 7. 意义建构
        if context.get('meaning_event'):
            self.positive.meaning.make_meaning(
                event=message[:30],
                event_valence=context.get('meaning_valence', 'neutral'),
                event_importance=context.get('meaning_importance', 50),
            )

        # 激活水平
        self.activation_level = _clamp(
            0.4 * emotion_intensity +
            0.3 * self.motivation.drives.arousal_level +
            0.3 * self.cognitive.dual_process.cognitive_load
        )

        return {
            'cognitive': cognitive_result,
            'emotion': emotion_result,
            'threat': threat_result,
            'reward': reward_result,
            'activation': round(self.activation_level, 1),
        }

    def tick(self):
        """时间流逝，全系统更新"""
        self.tick_count += 1
        self.cognitive.tick()
        self.emotion.tick()
        self.personality.tick()
        self.motivation.tick()
        self.clinical.tick()
        self.positive.tick()
        self.developmental.tick()

    def get_full_status(self):
        """获取全系统状态快照"""
        return {
            'L1_cognitive': self.cognitive.get_status(),
            'L2_emotion': self.emotion.get_status(),
            'L3_personality': self.personality.get_personality_summary(),
            'L4_social': self.social.get_status(),
            'L5_motivation': self.motivation.get_motivation_summary(),
            'L6_clinical': self.clinical.get_status(),
            'L7_positive': self.positive.get_status(),
            'L8_evolutionary': self.evolutionary.get_status(),
            'L9_developmental': self.developmental.get_status(),
            'activation': round(self.activation_level, 1),
        }

    def get_psychological_depth_prompt(self):
        """生成深层心理状态的 prompt 注入文本（自然语言描述，供 LLM 使用）

        只输出真正影响行为的关键心理维度，避免信息过载。
        输出格式：自然语言段落，以【深层心理状态】开头
        """
        parts = []

        # ---- 情绪状态 ----
        emo_status = self.emotion.get_status()
        dom = emo_status['dominant_emotion']
        dom_int = emo_status['dominant_intensity']
        val = emo_status['valence']
        if dom_int > 25:
            intensity_word = "轻微" if dom_int < 35 else ("中等" if dom_int < 60 else "强烈")
            parts.append(f"你此刻{intensity_word}感受到「{dom}」")

        # ---- 唤醒水平 ----
        arousal = emo_status['arousal']
        if arousal > 70:
            parts.append("心理唤醒度很高，思维活跃、反应快")
        elif arousal < 25:
            parts.append("心理唤醒度很低，有些疲惫、反应偏慢")

        # ---- 自尊/自我 ----
        se = self.personality.self_concept.get_self_esteem()
        se_diff = se - 70  # 偏离基准
        if abs(se_diff) > 15:
            if se_diff > 0:
                parts.append("自我评价偏高，有些自信膨胀")
            else:
                parts.append("自我评价偏低，有些自我怀疑")

        # ---- 存在焦虑 ----
        ex_anx = self.clinical.existential.existential_anxiety
        if ex_anx > 55:
            domain = self.clinical.existential.get_dominant_concern()
            parts.append(f"存在焦虑较高，主要围绕「{domain}」议题")

        # ---- 心流 ----
        flow_status = self.positive.flow.get_status()
        if flow_status.get('is_in_flow', False):
            parts.append("你正处于心流状态，完全沉浸在当下")
        elif flow_status.get('flow_state', 0) > 30:
            parts.append("你有些专注投入")

        # ---- 动机倾向 ----
        motivation_str = self.motivation.get_dominant_motivation()
        if motivation_str and motivation_str != "rest":
            parts.append(f"当前主导动机：{motivation_str}")

        # ---- 防御机制（只有激活时才说） ----
        active_defenses = self.personality.defense_mechanisms.get_active_defenses()
        if active_defenses:
            top = active_defenses[0]
            parts.append(f"你无意识中在使用「{top['name']}」防御机制")

        # ---- 情绪调节策略 ----
        if emo_status.get('current_regulation') and emo_status['current_regulation'] != "none":
            strat = emo_status['current_regulation']
            strat_name_map = {
                "cognitive_reappraisal": "认知重评",
                "suppression": "情绪压抑",
                "rumination": "反刍思维",
                "distraction": "注意力转移",
                "acceptance": "情绪接纳",
                "problem_solving": "问题解决",
                "savoring": "积极品味",
                "sublimation": "升华",
                "altruism": "利他",
                "humor": "幽默",
                "anticipation": "预期",
                "repression": "潜抑",
                "displacement": "置换",
                "rationalization": "合理化",
                "reaction_formation": "反向形成",
                "intellectualization": "理智化",
                "regression": "退行",
                "fantasy": "幻想",
                "passive_aggression": "被动攻击",
                "somatization": "躯体化",
                "denial": "否认",
                "projection": "投射",
                "distortion": "歪曲",
                "attentional_deployment": "注意转移",
            }
            name = strat_name_map.get(strat, strat)
            parts.append(f"你正在用「{name}」调节情绪")

        if not parts:
            return ""

        return "\n【深层心理状态】\n" + "。\n".join(parts) + "。\n"

    def get_quick_status(self):
        """获取精简状态摘要"""
        emo_status = self.emotion.get_status()
        return {
            '主导情绪': emo_status['dominant_emotion'],
            '情绪强度': emo_status['dominant_intensity'],
            '情绪效价': emo_status['valence'],
            '自尊水平': self.personality.self_concept.get_self_esteem(),
            'EQ估计': emo_status['eq_estimate'],
            '心流状态': self.positive.flow.get_status()['flow_state'],
            '幸福感': self.positive.wellbeing.get_status()['flourish_level'],
            '心理激活': round(self.activation_level, 1),
            '存在焦虑': self.clinical.existential.existential_anxiety,
        }





# ============================================================
# V9.1 记忆系统增强模块 — Memory Enhancement Suite
# 8 大优化：语义检索 · 情绪联动 · 人格偏好 · 画像失调
#         睡眠巩固 · 事件网络 · 联想式回忆 · 统一网关
# 参考：认知心理学 · 艾宾浩斯 · 巴特利特 · 心境一致性记忆
# ============================================================

def _clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))

def _days_ago(date_str):
    try:
        t = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return (datetime.now() - t).total_seconds() / 86400.0
    except Exception:
        return 0


# ============================================================
# 1. 语义检索器 SemanticRetriever
# ============================================================
# 不依赖外部库，用中文 2-gram + 3-gram 实现轻量语义相似度
# 加上激活扩散（spreading activation）机制

class SemanticRetriever:
    """
    轻量级语义检索器（无外部依赖）
    - 多层特征：单字 + 2-gram + 3-gram + 语义类别
    - 相似度：加权 Jaccard + 字符重叠 + 语义类别匹配
    - 激活扩散：相关记忆互相激活
    """

    # 常见语义类别（用于粗略语义匹配）
    SEMANTIC_CATEGORIES = {
        '食物': ['吃', '饭', '菜', '肉', '水果', '苹果', '香蕉', '橘子', '草莓', '西瓜',
               '蛋糕', '面包', '面', '火锅', '烧烤', '零食', '甜', '辣', '好吃', '美味',
               '早餐', '午餐', '晚餐', '宵夜', '奶茶', '咖啡', '喝', '饮料'],
        '情绪_开心': ['开心', '高兴', '快乐', '幸福', '喜欢', '爱', '棒', '好', '赞', '笑',
                    '愉快', '满足', '惊喜', '兴奋'],
        '情绪_难过': ['难过', '伤心', '哭', '痛', '难受', '失落', '孤独', '寂寞', '累',
                    '疲惫', '烦', '郁闷', '焦虑', '担心', '害怕'],
        '情绪_生气': ['生气', '愤怒', '气', '讨厌', '烦', '不爽', '恼火', '怒', '恨'],
        '时间': ['今天', '昨天', '明天', '早上', '晚上', '下午', '中午', '刚才', '之前',
               '以后', '一会', '现在', '刚才', '周末', '下周', '上个月'],
        '地点': ['家', '学校', '公司', '公园', '商场', '医院', '车站', '机场', '餐厅',
               '咖啡店', '图书馆', '电影院', '海边', '山上'],
        '社交': ['朋友', '家人', '同学', '同事', '对象', '恋人', '见面', '聚会',
               '约会', '聊天', '一起', '陪伴', '想你', '想我'],
        '爱好': ['游戏', '电影', '音乐', '歌', '书', '阅读', '画画', '运动', '跑步',
               '健身', '旅行', '旅游', '拍照', '摄影', '做饭', '烘焙'],
        '工作学习': ['工作', '学习', '考试', '作业', '项目', '加班', '上班', '下班',
                   '上课', '下课', '毕业', '论文', '报告', '会议'],
        '自我': ['我', '自己', '性格', '脾气', '习惯', '爱好', '喜欢', '讨厌',
               '害怕', '梦想', '目标', '计划'],
    }

    def __init__(self):
        self.stop_chars = set('的了是我你他她它在有和就都也还会要把被让给从到向对于关于这个那个什么怎么为什么哪吗呢吧啊哦嗯呃呀及与或则而因所')
        self._association_cache = {}

    def _extract_chars(self, text):
        """提取有效单字（去停用字）"""
        text = re.sub(r'[^\u4e00-\u9fff]', '', text)
        return set(c for c in text if c not in self.stop_chars)

    def _extract_ngrams(self, text, n=2):
        """提取字符 n-gram"""
        text = re.sub(r'[^\u4e00-\u9fff\w]', '', text)
        if len(text) < n:
            return set()
        grams = set()
        for i in range(len(text) - n + 1):
            gram = text[i:i+n]
            if not all(c in self.stop_chars for c in gram):
                grams.add(gram)
        return grams

    def _match_categories(self, text):
        """匹配语义类别"""
        matched = set()
        for cat, keywords in self.SEMANTIC_CATEGORIES.items():
            for kw in keywords:
                if kw in text:
                    matched.add(cat)
                    break
        return matched

    def similarity(self, text_a, text_b):
        """计算两段文本的综合相似度（0~100）"""
        if not text_a or not text_b:
            return 0.0

        score = 0.0

        # 1. 单字重合（基础分，权重 0.25）
        chars_a = self._extract_chars(text_a)
        chars_b = self._extract_chars(text_b)
        if chars_a and chars_b:
            char_inter = len(chars_a & chars_b)
            char_union = len(chars_a | chars_b)
            char_sim = char_inter / char_union if char_union else 0
            score += char_sim * 25

        # 2. 2-gram 重合（中等权重，0.30）
        bi_a = self._extract_ngrams(text_a, 2)
        bi_b = self._extract_ngrams(text_b, 2)
        if bi_a and bi_b:
            bi_inter = len(bi_a & bi_b)
            bi_union = len(bi_a | bi_b)
            bi_sim = bi_inter / bi_union if bi_union else 0
            score += bi_sim * 30

        # 3. 3-gram 重合（高权重，0.25）
        tri_a = self._extract_ngrams(text_a, 3)
        tri_b = self._extract_ngrams(text_b, 3)
        if tri_a and tri_b:
            tri_inter = len(tri_a & tri_b)
            tri_union = len(tri_a | tri_b)
            tri_sim = tri_inter / tri_union if tri_union else 0
            score += tri_sim * 25

        # 4. 语义类别匹配（0.20）
        cat_a = self._match_categories(text_a)
        cat_b = self._match_categories(text_b)
        if cat_a and cat_b:
            cat_inter = len(cat_a & cat_b)
            cat_union = len(cat_a | cat_b)
            cat_sim = cat_inter / cat_union if cat_union else 0
            score += cat_sim * 20

        return min(100.0, score)

    def rank_memories(self, query, memories, top_k=10):
        """
        按语义相似度对记忆排序
        memories: [{content, weight, ...}, ...]
        返回: 排序后的 memories（带 sim_score 字段）
        """
        if not memories or not query:
            return []

        scored = []
        for mem in memories:
            sim = self.similarity(query, mem.get('content', ''))
            # 语义分 + 权重分 加权合并
            weight = mem.get('weight', 0.5) * 100
            final_score = sim * 0.6 + weight * 0.4
            scored.append({**mem, 'sim_score': round(sim, 1), 'retrieval_score': round(final_score, 1)})

        scored.sort(key=lambda x: -x['retrieval_score'])
        return scored[:top_k]

    def spreading_activation(self, query, memories, activation_decay=0.6, hops=2):
        """
        激活扩散检索
        1. 先找到与 query 直接相关的记忆（第一轮激活）
        2. 再激活与这些记忆相似的其他记忆（第二轮扩散）
        模拟语义网络的激活扩散过程
        """
        if not memories:
            return []

        # 第一轮：直接匹配
        first_pass = self.rank_memories(query, memories, top_k=min(20, len(memories)))
        if not first_pass:
            return []

        # 建立激活值字典
        activation = {}
        for i, mem in enumerate(first_pass):
            key = mem.get('id', mem.get('content', ''))
            # 排名越靠前，初始激活越高
            activation[key] = max(
                activation.get(key, 0),
                mem.get('retrieval_score', 50) * (0.9 ** i)
            )

        # 扩散轮次
        for hop in range(hops):
            new_activation = dict(activation)
            activated_keys = list(activation.keys())

            for key in activated_keys:
                act_level = activation[key]
                if act_level < 10:
                    continue

                # 找到这条记忆的内容
                mem_content = None
                for m in memories:
                    mk = m.get('id', m.get('content', ''))
                    if mk == key:
                        mem_content = m.get('content', '')
                        break

                if not mem_content:
                    continue

                # 找相关记忆
                for other in memories:
                    okey = other.get('id', other.get('content', ''))
                    if okey == key:
                        continue
                    sim = self.similarity(mem_content, other.get('content', ''))
                    if sim > 25:  # 超过阈值才扩散
                        spread = act_level * (sim / 100) * activation_decay
                        new_activation[okey] = max(
                            new_activation.get(okey, 0),
                            spread
                        )

            activation = new_activation
            activation_decay *= 0.7  # 每轮衰减增加

        # 合并到记忆列表
        result = []
        for mem in memories:
            key = mem.get('id', mem.get('content', ''))
            act = activation.get(key, 0)
            if act > 5:  # 激活阈值
                result.append({
                    **mem,
                    'activation': round(act, 1),
                    'retrieval_score': round(
                        mem.get('weight', 0.5) * 100 * 0.3 + act * 0.7,
                        1
                    ),
                })

        result.sort(key=lambda x: -x['retrieval_score'])
        return result


# ============================================================
# 2. 情绪-记忆联动 EmotionMemoryLink
# ============================================================

class EmotionMemoryLink:
    """
    情绪与记忆的双向联动系统

    情绪→记忆（写入时）：
      - 情绪增强记忆效应：强情绪事件权重更高、衰减更慢
      - 闪光灯记忆：极端情绪事件几乎不遗忘
      - 心境一致性编码：情绪一致的信息编码更深

    记忆→情绪（提取时）：
      - 触景生情：提取到强情绪记忆时反向影响当前情绪
      - 心境一致性提取：当前情绪下优先提取相同情绪价的记忆
    """

    # 情绪类型 → 记忆增强系数
    EMOTION_MEMORY_BOOST = {
        'joy': 1.2,
        'sadness': 1.3,      # 悲伤记忆更深刻
        'anger': 1.4,        # 愤怒记忆非常深刻
        'fear': 1.5,         # 恐惧记忆最强（进化优势）
        'surprise': 1.3,
        'love': 1.4,
        'trust': 1.1,
        'gratitude': 1.25,
        'shame': 1.45,       # 羞耻感记忆极深
    }

    def __init__(self):
        self.flashbulb_threshold = 80  # 超过此强度=闪光灯记忆
        self.mood_congruence_bias = 0.3  # 心境一致性偏向强度

    def calculate_memory_strength(self, base_weight, emotion_type, emotion_intensity):
        """
        根据情绪状态计算记忆强度倍率
        返回: 调整后的权重值
        """
        if emotion_intensity < 10:
            return base_weight  # 微弱情绪不影响

        boost = self.EMOTION_MEMORY_BOOST.get(emotion_type, 1.0)
        intensity_factor = 0.5 + (emotion_intensity / 100) * 0.8  # 0.5 ~ 1.3

        effective_boost = 1 + (boost - 1) * intensity_factor

        # 闪光灯记忆：极端情绪
        if emotion_intensity >= self.flashbulb_threshold:
            effective_boost *= 1.5

        return base_weight * effective_boost

    def calculate_decay_rate(self, base_decay, emotion_type, emotion_intensity):
        """
        根据情绪调整遗忘速率
        强情绪 → 遗忘更慢
        """
        if emotion_intensity < 10:
            return base_decay

        boost = self.EMOTION_MEMORY_BOOST.get(emotion_type, 1.0)
        # 情绪越强，衰减越慢（衰减系数越接近 1 = 越慢）
        slowdown = 1 + (boost - 1) * 0.3 * (emotion_intensity / 100)
        # 闪光灯记忆衰减极慢
        if emotion_intensity >= self.flashbulb_threshold:
            slowdown *= 1.3

        # base_decay 通常是 0.95（日衰减），slowdown 后更接近 1
        return min(0.999, base_decay ** (1 / slowdown))

    def mood_congruent_rank(self, memories, current_mood_valence):
        """
        心境一致性排序：当前情绪与记忆情绪一致时排序提升
        current_mood_valence: -5 ~ +5（负性~正性）
        """
        if not memories:
            return []

        result = []
        for mem in memories:
            mem_valence = mem.get('emotion_valence', 0)
            base_score = mem.get('retrieval_score', mem.get('weight', 0.5) * 100)

            # 计算情绪一致性（符号相同 = 一致）
            if mem_valence == 0:
                congruence = 0
            elif (mem_valence > 0 and current_mood_valence > 0) or \
                 (mem_valence < 0 and current_mood_valence < 0):
                congruence = 1
            else:
                congruence = -1

            bonus = congruence * self.mood_congruence_bias * 20
            adjusted = base_score + bonus
            result.append({**mem, 'mood_congruence_bonus': round(bonus, 1),
                          'final_score': round(adjusted, 1)})

        result.sort(key=lambda x: -x['final_score'])
        return result

    def estimate_emotion_valence(self, emotion_type):
        """估算情绪的效价（正/负）"""
        positive = {'joy', 'love', 'trust', 'gratitude', 'surprise_positive',
                    'hope', 'pride', 'amusement', 'inspiration'}
        negative = {'sadness', 'anger', 'fear', 'shame', 'guilt', 'disgust',
                    'anxiety', 'jealousy', 'regret', 'loneliness'}

        if emotion_type in positive:
            return 3
        elif emotion_type in negative:
            return -3
        return 0


# ============================================================
# 3. 人格记忆偏好 PersonalityMemoryBias
# ============================================================

class PersonalityMemoryBias:
    """
    人格特质影响记忆风格

    大五人格 × 记忆的关系：
    - 神经质(Neuroticism) 高：负性记忆偏差 → 更容易记住负面事件，且记得更牢
    - 尽责性(Conscientiousness) 高：记忆更准确，扭曲更少，组织性更强
    - 开放性(Openness) 高：对新异、创造性的记忆更敏感
    - 外向性(Extraversion) 高：社交记忆、积极事件记忆更强
    - 宜人性(Agreeableness) 高：对他人的积极记忆更多，倾向于原谅
    """

    def __init__(self):
        self.baseline_bias = {
            'negativity_bias': 0,       # 负性记忆偏差
            'accuracy_bias': 0,         # 记忆准确性倾向
            'novelty_bias': 0,          # 新异记忆偏好
            'social_bias': 0,           # 社交记忆偏好
            'forgiveness_bias': 0,      # 宽恕倾向（负面记忆消退更快）
        }

    def update_from_bigfive(self, big_five_traits):
        """根据大五人格计算记忆偏好"""
        n = big_five_traits.get('neuroticism', 50)
        c = big_five_traits.get('conscientiousness', 50)
        o = big_five_traits.get('openness', 50)
        e = big_five_traits.get('extraversion', 50)
        a = big_five_traits.get('agreeableness', 50)

        # 神经质 → 负性记忆偏差
        self.baseline_bias['negativity_bias'] = (n - 50) * 0.6

        # 尽责性 → 记忆准确性
        self.baseline_bias['accuracy_bias'] = (c - 50) * 0.5

        # 开放性 → 新异记忆偏好
        self.baseline_bias['novelty_bias'] = (o - 50) * 0.4

        # 外向性 → 社交记忆偏好
        self.baseline_bias['social_bias'] = (e - 50) * 0.4

        # 宜人性 → 宽恕倾向
        self.baseline_bias['forgiveness_bias'] = (a - 50) * 0.5

    def adjust_memory_weight(self, base_weight, memory_type='neutral',
                             memory_valence=0, is_social=False, is_novel=False):
        """根据人格偏好在记忆写入时调整权重"""
        bias = self.baseline_bias
        factor = 1.0

        # 负性记忆偏差
        if memory_valence < 0:
            factor += bias['negativity_bias'] * 0.01  # 每 10 点神经质 → 6% 加权

        # 新异记忆偏好
        if is_novel:
            factor += bias['novelty_bias'] * 0.008

        # 社交记忆偏好
        if is_social:
            factor += bias['social_bias'] * 0.008

        # 宽恕倾向：负面记忆消退更快（通过降低初始权重体现）
        if memory_valence < 0:
            factor -= bias['forgiveness_bias'] * 0.006

        return _clamp(base_weight * factor, 0.01, 1.0)

    def get_distortion_probability_modifier(self, base_prob):
        """根据人格调整记忆扭曲概率"""
        # 尽责性高 → 扭曲概率低
        acc_bias = self.baseline_bias.get('accuracy_bias', 0)
        modifier = 1.0 - acc_bias * 0.008  # 每 10 点尽责性 → 4% 扭曲率降低
        return max(0.1, base_prob * modifier)


# ============================================================
# 4. 画像冲突 + 认知失调 ProfileDissonanceResolver
# ============================================================

class ProfileDissonanceResolver:
    """
    用户画像更新的认知失调处理

    问题：用户说"我喜欢A"又说"我喜欢B"就直接拼接，但如果说"我不喜欢A了"没有处理机制
    方案：
    - 检测新信息与已有画像的冲突
    - 不是直接覆盖，而是标记为"有冲突"
    - 多次确认后才正式更新
    - 冲突期间触发认知失调感
    """

    def __init__(self):
        self.pending_updates = defaultdict(dict)  # {uid: {key: [{value, count, last_time, direction}]}}
        self.dissonance_level = defaultdict(float)  # {uid: 失调强度 0~100}
        self.max_pending = 5  # 最多确认次数
        self.confirm_threshold = 2  # 2次确认就写入

    def check_conflict(self, uid, key, new_value, existing_profile):
        """
        检查新画像信息与已有画像是否冲突
        返回: {'has_conflict': bool, 'conflict_type': str, 'existing': str}
        """
        existing = existing_profile.get(key, '')
        if not existing:
            return {'has_conflict': False, 'conflict_type': 'new', 'existing': ''}

        # 检查是否有否定词
        negations = ['不喜欢', '不是', '没有', '讨厌', '不再', '以前是', '曾经']
        has_negation = any(neg in new_value for neg in negations)

        if has_negation:
            # 明确否定了之前的信息 → 直接冲突
            return {'has_conflict': True, 'conflict_type': 'negation', 'existing': existing}

        # 检查是否是同一类别的不同值（比如"我喜欢猫" vs "我喜欢狗" → 不冲突，可共存）
        # 只有互斥的才叫冲突（比如"我20岁" vs "我25岁"）
        exclusive_keys = ['年龄', '生日', '职业', '城市', '性别', '称呼']
        if key in exclusive_keys and new_value != existing:
            return {'has_conflict': True, 'conflict_type': 'exclusive', 'existing': existing}

        # 可共存信息 → 不冲突
        return {'has_conflict': False, 'conflict_type': 'compatible', 'existing': existing}

    def propose_update(self, uid, key, value, existing_profile):
        """
        提议一次画像更新
        返回: {'action': 'add'/'confirm'/'wait'/'reject', 'dissonance_triggered': bool}
        """
        conflict = self.check_conflict(uid, key, value, existing_profile)

        if not conflict['has_conflict'] and conflict['conflict_type'] == 'new':
            # 全新信息，直接加入
            return {'action': 'add', 'dissonance_triggered': False}

        if not conflict['has_conflict']:
            # 可共存信息，直接追加
            return {'action': 'append', 'dissonance_triggered': False}

        # 有冲突 → 进入待确认状态
        pending = self.pending_updates[uid].get(key, [])

        # 检查是否已经有相同的提议
        existing_proposal = None
        for p in pending:
            if p['value'] == value:
                existing_proposal = p
                break

        if existing_proposal:
            existing_proposal['count'] += 1
            existing_proposal['last_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            pending.append({
                'value': value,
                'count': 1,
                'last_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'conflict_type': conflict['conflict_type'],
            })
        self.pending_updates[uid][key] = pending

        # 触发认知失调
        dissonance_increase = 15 if conflict['conflict_type'] == 'negation' else 10
        self.dissonance_level[uid] = min(100, self.dissonance_level.get(uid, 0) + dissonance_increase)

        # 检查是否达到确认阈值
        count = existing_proposal['count'] if existing_proposal else 1
        if count >= self.confirm_threshold:
            return {
                'action': 'confirm_update',
                'dissonance_triggered': True,
                'old_value': conflict['existing'],
                'new_value': value,
            }

        return {
            'action': 'wait',
            'dissonance_triggered': True,
            'wait_count': count,
            'needed': self.confirm_threshold,
        }

    def get_dissonance(self, uid):
        """获取用户的认知失调强度"""
        return round(self.dissonance_level.get(str(uid), 0), 1)

    def decay_dissonance(self, uid, rate=2.0):
        """失调随时间自然衰减"""
        uid = str(uid)
        if uid in self.dissonance_level:
            self.dissonance_level[uid] = max(0, self.dissonance_level[uid] - rate)

    def resolve_dissonance(self, uid, key):
        """解决某条冲突后，失调降低"""
        uid = str(uid)
        if uid in self.dissonance_level:
            self.dissonance_level[uid] = max(0, self.dissonance_level[uid] - 20)
        if uid in self.pending_updates and key in self.pending_updates[uid]:
            del self.pending_updates[uid][key]


# ============================================================
# 5. 睡眠记忆巩固 SleepConsolidator
# ============================================================

class SleepConsolidator:
    """
    睡眠期间的记忆巩固过程

    参考：睡眠的记忆巩固功能（海马体 → 新皮层转移）
    效果：
    1. 记忆聚类合并：相似主题的记忆合并为一条主旨记忆
    2. 细节模糊化：不重要的细节被遗忘，主旨保留
    3. 权重重估：重要的记忆保留/强化，不重要的弱化
    4. 情绪记忆优先：情绪性记忆优先巩固
    """

    def __init__(self):
        self.consolidation_count = 0
        self.merge_threshold = 35  # 相似度超过此值就考虑合并

    def consolidate(self, memories, emotion_link=None):
        """
        执行一次睡眠巩固
        输入: 记忆列表
        输出: 巩固后的记忆列表（可能减少条目数）
        """
        if len(memories) < 2:
            return memories, {'merged': 0, 'weakened': 0, 'strengthened': 0}

        self.consolidation_count += 1
        stats = {'merged': 0, 'weakened': 0, 'strengthened': 0}

        # 步骤1：计算记忆间的相似度矩阵（简化版，用简单 n-gram）
        retriever = SemanticRetriever()

        # 步骤2：聚类（贪心算法）
        clusters = []
        used = set()

        for i, mem in enumerate(memories):
            if i in used:
                continue
            cluster = [i]
            used.add(i)

            for j in range(i + 1, len(memories)):
                if j in used:
                    continue
                sim = retriever.similarity(
                    mem.get('content', ''),
                    memories[j].get('content', '')
                )
                if sim >= self.merge_threshold:
                    cluster.append(j)
                    used.add(j)

            clusters.append(cluster)

        # 步骤3：合并每个聚类
        result = []
        for cluster in clusters:
            if len(cluster) == 1:
                # 单条记忆 → 不合并，只做权重重估
                mem = dict(memories[cluster[0]])
                old_w = mem.get('weight', 0.5)
                # 睡眠中，低权重记忆进一步弱化，高权重记忆略微强化
                if old_w < 0.3:
                    new_w = old_w * 0.8
                    stats['weakened'] += 1
                elif old_w > 0.7:
                    new_w = min(1.0, old_w * 1.05)
                    stats['strengthened'] += 1
                else:
                    new_w = old_w
                mem['weight'] = round(new_w, 4)
                result.append(mem)
            else:
                # 多条记忆 → 合并为一条
                mems = [memories[i] for i in cluster]
                # 取权重最高的那条为主
                mems.sort(key=lambda m: -m.get('weight', 0))
                primary = dict(mems[0])

                # 合并内容（用主要内容 + 补充其他记忆的关键信息）
                contents = [m.get('content', '') for m in mems[:3]]
                # 简单合并：取最长的内容 + "等相关事情"
                main_content = max(contents, key=len)
                if len(mems) > 2:
                    merged_content = f"{main_content}（还有{len(mems)-1}件相关的事）"
                else:
                    merged_content = main_content

                primary['content'] = merged_content
                # 合并后权重提升（多条记忆互相印证）
                total_weight = sum(m.get('weight', 0.3) for m in mems)
                primary['weight'] = round(min(1.0, total_weight * 0.7), 4)
                primary['merged_from'] = len(mems)
                primary['consolidated'] = True

                stats['merged'] += 1
                stats['strengthened'] += 1
                result.append(primary)

        # 步骤4：情绪记忆优先巩固（如果有情绪链接）
        if emotion_link:
            for mem in result:
                emo_int = mem.get('emotion_intensity', 0)
                if emo_int > 50:
                    boost = 1 + (emo_int - 50) / 100 * 0.2  # 最多 +20%
                    mem['weight'] = round(min(1.0, mem.get('weight', 0.5) * boost), 4)

        return result, stats


# ============================================================
# 6. 事件关联网络 EventNetwork
# ============================================================

class EventNetwork:
    """
    事件关联网络

    事件不是孤立的，它们之间有因果关系：
    冲突 → 道歉 → 和解
    初识 → 熟悉 → 亲密
    承诺 → 兑现 / 食言
    背叛 → 信任崩塌 → 修复尝试 → 恢复/破裂

    用有向图表示事件序列和因果链。
    """

    # 事件关系模式
    RELATION_PATTERNS = {
        'first_meeting': {'leads_to': ['shared_experience', 'milestone'], 'weight': 0.8},
        'conflict': {
            'leads_to': ['apology', 'betrayal', 'reconciliation'],
            'followed_by': ['apology'],  # 冲突后通常有道歉
            'weight': 0.9,
        },
        'apology': {
            'follows': ['conflict'],
            'leads_to': ['reconciliation'],
            'weight': 0.7,
        },
        'reconciliation': {
            'follows': ['apology', 'conflict'],
            'leads_to': ['milestone'],
            'weight': 0.85,
        },
        'promise': {
            'leads_to': ['milestone', 'betrayal'],
            'weight': 0.6,
        },
        'betrayal': {
            'follows': ['promise', 'trust_event'],
            'leads_to': ['conflict', 'reconciliation'],
            'weight': 0.95,
        },
        'shared_experience': {
            'leads_to': ['milestone', 'shared_experience'],
            'weight': 0.5,
        },
        'milestone': {
            'follows': ['shared_experience', 'reconciliation'],
            'leads_to': ['milestone'],
            'weight': 0.7,
        },
    }

    def __init__(self):
        self.chains = defaultdict(list)  # {uid: [事件链]}
        self.relations = []  # [(event_a_id, event_b_id, relation_type, strength)]

    def add_event(self, uid, event):
        """添加事件并自动建立关联"""
        uid = str(uid)
        event_id = event.get('id', id(event))
        event_type = event.get('type', 'unknown')

        # 找最近的事件，尝试建立因果链
        recent_events = self._get_recent_events(uid, limit=5)

        for prev in recent_events:
            prev_type = prev.get('type', '')
            pattern = self.RELATION_PATTERNS.get(prev_type, {})

            # 检查 prev → event 是否是合理的因果链
            if event_type in pattern.get('leads_to', []):
                strength = pattern.get('weight', 0.5)
                # 时间越近，关联越强
                days_between = self._days_between(prev.get('time', ''), event.get('time', ''))
                time_factor = max(0.3, 1 - days_between / 30)
                final_strength = strength * time_factor

                self.relations.append({
                    'uid': uid,
                    'from': prev.get('id'),
                    'to': event_id,
                    'relation': 'causal',
                    'strength': round(final_strength, 2),
                    'from_type': prev_type,
                    'to_type': event_type,
                })

        # 保存到事件链
        chain_entry = {
            'id': event_id,
            'type': event_type,
            'content': event.get('content', ''),
            'time': event.get('time', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            'importance': event.get('importance', 0.5),
        }
        self.chains[uid].append(chain_entry)

        return self.get_event_context(uid, event_id)

    def get_event_context(self, uid, event_id, depth=2):
        """获取某个事件的上下文（前因后果）"""
        uid = str(uid)
        before = []
        after = []

        # 找前置事件
        current = event_id
        for _ in range(depth):
            found = None
            for rel in self.relations:
                if rel['uid'] == uid and rel['to'] == current:
                    found = rel['from']
                    before.append({
                        'id': rel['from'],
                        'type': rel['from_type'],
                        'relation': rel['relation'],
                        'strength': rel['strength'],
                    })
                    break
            if found:
                current = found
            else:
                break
        before.reverse()

        # 找后续事件
        current = event_id
        for _ in range(depth):
            found = None
            for rel in self.relations:
                if rel['uid'] == uid and rel['from'] == current:
                    found = rel['to']
                    after.append({
                        'id': rel['to'],
                        'type': rel['to_type'],
                        'relation': rel['relation'],
                        'strength': rel['strength'],
                    })
                    break
            if found:
                current = found
            else:
                break

        return {
            'event_id': event_id,
            'preceding_events': before,
            'following_events': after,
            'chain_length': len(before) + 1 + len(after),
        }

    def get_chain_summary(self, uid, limit=5):
        """获取用户的关系事件链摘要"""
        uid = str(uid)
        events = sorted(self.chains.get(uid, []),
                        key=lambda e: e.get('time', ''), reverse=True)
        return events[:limit]

    def _get_recent_events(self, uid, limit=5):
        uid = str(uid)
        events = self.chains.get(uid, [])
        events_sorted = sorted(events, key=lambda e: e.get('time', ''), reverse=True)
        return events_sorted[:limit]

    def _days_between(self, time_a, time_b):
        try:
            ta = datetime.strptime(time_a, "%Y-%m-%d %H:%M:%S")
            tb = datetime.strptime(time_b, "%Y-%m-%d %H:%M:%S")
            return abs((tb - ta).total_seconds()) / 86400.0
        except Exception:
            return 7


# ============================================================
# 7. 记忆碎片 V2 MemoryFragmentV2
# ============================================================

class MemoryFragmentV2:
    """
    记忆碎片 V2 —— 从"随机走神"升级为"联想式回忆"

    改进点：
    1. 联想式触发：当前话题关键词与记忆有语义关联时才触发
    2. 心境一致性：当前情绪与记忆情绪匹配时更容易触发
    3. 时间距离感：回忆时会体现"多久之前"的感觉
    4. 更多模板：丰富走神的表现形式
    5. 反刍联动：高反刍时负面回忆触发率提升
    """

    # 不同时间距离的表述模板
    TEMPORAL_DISTANCE_PHRASES = {
        'recent': ['刚才', '刚刚', '前不久', '就在刚才'],
        'hours': ['今天早些时候', '上午的时候', '下午那会', '今天'],
        'days': ['前几天', '几天前', '前天', '昨天'],
        'weeks': ['上周', '前阵子', '前几周', '前些日子'],
        'months': ['上个月', '前几个月', '有段时间了', '记得是之前'],
        'long_ago': ['很久以前', '记得好像是', '印象里是', '记不清什么时候了'],
    }

    # 触发模板（V2 版本，更丰富）
    TRIGGER_TEMPLATES = [
        ("……突然想起{time}你说的「{content}」。", "external"),
        ("（走神了一下）......{time}的那件事，我现在还记着。", "internal"),
        ("说起来，{time}你提到过「{content}」对吧？", "confirmative"),
        ("......不知怎么的，突然就想起{time}的事了。", "wandering"),
        ("（顿了一下）{time}你说的那句话，我一直记得。", "emotional"),
        ("......说起来有点突然，就是想起{time}你说过「{content}」。", "casual"),
        ("（小声）{time}那件事......你还记得吗。", "soft"),
    ]

    def __init__(self):
        self.retriever = SemanticRetriever()
        self.emotion_link = EmotionMemoryLink()
        self.last_trigger_time = {}  # {uid: timestamp}
        self.cooldown = 1800  # 30分钟冷却
        self.base_trigger_rate = 0.08  # 8% 基础概率（V2 提升）

    def should_trigger(self, uid, current_message, memories, current_mood='neutral',
                       rumination_level=0):
        """
        判断是否触发记忆碎片
        返回: (should_trigger, memory_content, template_info)
        """
        uid = str(uid)

        # 冷却检查
        now = time.time()
        if uid in self.last_trigger_time:
            if now - self.last_trigger_time[uid] < self.cooldown:
                return False, None, None

        if not memories:
            return False, None, None

        # 语义联想：找与当前消息相关的记忆
        related = self.retriever.rank_memories(current_message, memories, top_k=5)
        if not related:
            return False, None, None

        top_sim = related[0].get('sim_score', 0)
        # 相似度越高，触发概率越大
        sim_factor = top_sim / 100  # 0 ~ 1

        # 心境一致性：记忆情绪与当前情绪是否一致
        mood_factor = 0.5  # 默认中性
        mem_valence = related[0].get('emotion_valence', 0)
        if current_mood == 'positive' and mem_valence > 0:
            mood_factor = 0.9
        elif current_mood == 'negative' and mem_valence < 0:
            mood_factor = 0.95  # 负性心境更容易触发回忆（反刍倾向）
        elif current_mood == 'positive' and mem_valence < 0:
            mood_factor = 0.2
        elif current_mood == 'negative' and mem_valence > 0:
            mood_factor = 0.4

        # 反刍加成
        rumination_factor = 1 + rumination_level / 100

        # 计算最终触发概率
        trigger_prob = self.base_trigger_rate * (0.5 + sim_factor) * mood_factor * rumination_factor
        trigger_prob = min(0.15, trigger_prob)  # 最高 15%

        if random.random() > trigger_prob:
            return False, None, None

        # 触发成功
        chosen_mem = related[random.randint(0, min(2, len(related) - 1))]
        template, template_type = random.choice(self.TRIGGER_TEMPLATES)
        time_phrase = self._get_time_phrase(chosen_mem.get('time', ''))
        content = chosen_mem.get('content', '')
        if len(content) > 20:
            content = content[:20] + "……"

        result_text = template.format(time=time_phrase, content=content)

        self.last_trigger_time[uid] = now

        return True, result_text, {
            'memory_id': chosen_mem.get('id'),
            'similarity': top_sim,
            'template_type': template_type,
            'trigger_prob': round(trigger_prob * 100, 1),
        }

    def _get_time_phrase(self, time_str):
        """根据记忆时间生成时间距离感的表述"""
        days = _days_ago(time_str) if time_str else 7

        if days < 0.1:  # 小于2小时
            category = 'recent'
        elif days < 1:
            category = 'hours'
        elif days < 3:
            category = 'days'
        elif days < 14:
            category = 'weeks'
        elif days < 60:
            category = 'months'
        else:
            category = 'long_ago'

        phrases = self.TEMPORAL_DISTANCE_PHRASES.get(category, ['前阵子'])
        return random.choice(phrases)


# ============================================================
# 8. 统一记忆网关 MemoryGateway
# ============================================================

class MemoryGateway:
    """
    统一记忆写入网关 V9.1

    所有记忆写入经过此网关，内部自动分流：
    - 短期记忆（工作记忆）
    - 长期记忆（语义 + 情节）
    - 画像记忆（用户属性）
    - 事件记忆（关系节点）

    写入时自动触发：
    - 安全检查
    - 情绪增强计算
    - 人格偏好调整
    - 权重初始化
    - 标签提取
    """

    def __init__(self, memory_module=None, memory_weight=None,
                 event_memory=None, emotion_link=None,
                 personality_bias=None, profile_resolver=None):
        self.memory = memory_module
        self.memory_weight = memory_weight
        self.event_memory = event_memory

        # 增强模块
        self.emotion_link = emotion_link or EmotionMemoryLink()
        self.personality_bias = personality_bias or PersonalityMemoryBias()
        self.profile_resolver = profile_resolver or ProfileDissonanceResolver()
        self.retriever = SemanticRetriever()
        self.event_network = EventNetwork()
        self.sleep_consolidator = SleepConsolidator()
        self.fragment_v2 = MemoryFragmentV2()

        # 统计
        self.stats = defaultdict(int)

    def _get_all_weighted_memories(self, uid):
        """适配层：从 memory_weight 获取所有记忆（带权重）"""
        uid = str(uid)
        if not self.memory_weight:
            return []
        # 触发遗忘计算
        self.memory_weight._apply_forgetting(uid)
        with self.memory_weight._lock:
            memories = self.memory_weight._data.get(uid, [])
            return [dict(m) for m in memories]

    def remember(self, uid, content, memory_type='chat', **kwargs):
        """
        统一记忆写入入口

        Args:
            uid: 用户ID
            content: 记忆内容
            memory_type: 类型 (chat/profile/event/important)
            **kwargs: 可选参数
                - emotion_type: 情绪类型
                - emotion_intensity: 情绪强度 0~100
                - importance: 重要性 0~1
                - tags: 标签列表
                - event_type: 事件类型（事件记忆用）
                - source: 来源
                - is_social: 是否社交记忆
                - is_novel: 是否新奇
        """
        uid = str(uid)
        self.stats['total_writes'] += 1

        # 1. 安全检查（如果有 memory 模块）
        if self.memory and hasattr(self.memory, '_is_safe'):
            if not self.memory._is_safe(content):
                self.stats['rejected_unsafe'] += 1
                return {'success': False, 'reason': 'unsafe'}

        # 2. 计算基础权重
        base_weight = kwargs.get('importance', 0.3)
        if memory_type == 'important':
            base_weight = max(base_weight, 0.7)
        elif memory_type == 'event':
            base_weight = max(base_weight, 0.6)
        elif memory_type == 'profile':
            base_weight = max(base_weight, 0.8)

        # 3. 情绪增强
        emotion_type = kwargs.get('emotion_type', 'neutral')
        emotion_intensity = kwargs.get('emotion_intensity', 20)
        if emotion_intensity > 10:
            base_weight = self.emotion_link.calculate_memory_strength(
                base_weight, emotion_type, emotion_intensity
            )
            base_weight = min(1.0, base_weight)

        # 4. 人格偏好调整
        is_social = kwargs.get('is_social', memory_type in ('chat', 'event'))
        is_novel = kwargs.get('is_novel', False)
        emotion_valence = self.emotion_link.estimate_emotion_valence(emotion_type)
        base_weight = self.personality_bias.adjust_memory_weight(
            base_weight,
            memory_type=memory_type,
            memory_valence=emotion_valence,
            is_social=is_social,
            is_novel=is_novel,
        )

        # 5. 写入到对应存储层
        result = {'success': True, 'weight': round(base_weight, 4),
                  'memory_type': memory_type, 'emotion_valence': emotion_valence}

        # 5a. 写入权重系统（长期记忆索引）
        if self.memory_weight:
            tags = kwargs.get('tags', [])
            self.memory_weight.add_memory(
                uid, content,
                important=memory_type in ('important', 'event', 'profile'),
                tags=tags,
            )
            result['weight_saved'] = True

        # 5b. 写入短期记忆
        if self.memory and memory_type in ('chat', 'important'):
            source = kwargs.get('source', 'private')
            try:
                self.memory.add_short(uid, 'user', content, source)
                result['short_saved'] = True
            except Exception:
                pass

        # 5c. 写入画像记忆
        if memory_type == 'profile' and self.memory:
            key = kwargs.get('profile_key', '')
            if key:
                existing = self.memory._profile.get(uid, {})
                decision = self.profile_resolver.propose_update(
                    uid, key, content, existing
                )
                if decision['action'] in ('add', 'confirm_update'):
                    self.memory.update_profile(uid, key, content)
                    if decision['action'] == 'confirm_update':
                        self.profile_resolver.resolve_dissonance(uid, key)
                result['profile_action'] = decision['action']
                result['dissonance'] = decision.get('dissonance_triggered', False)

        # 5d. 写入事件记忆
        if memory_type == 'event' and self.event_memory:
            event_type = kwargs.get('event_type', 'shared_experience')
            event = self.event_memory.record_event(
                uid=uid,
                event_type=event_type,
                content=content,
                importance=base_weight,
            )
            # 同时加入事件关联网络
            if event:
                self.event_network.add_event(uid, event)
            result['event_saved'] = True

        # 6. 附加情绪标签
        result['emotion_type'] = emotion_type
        result['emotion_intensity'] = emotion_intensity

        return result

    def recall(self, uid, query, limit=10, current_mood_valence=0, use_spreading=True):
        """
        统一记忆检索入口

        Args:
            uid: 用户ID
            query: 查询文本
            limit: 返回数量
            current_mood_valence: 当前情绪效价（心境一致性用）
            use_spreading: 是否使用激活扩散

        Returns:
            排序后的记忆列表
        """
        uid = str(uid)
        self.stats['total_recalls'] += 1

        if not self.memory_weight:
            return []

        # 获取该用户的所有记忆
        all_memories = self._get_all_weighted_memories(uid)
        if not all_memories:
            return []

        # 语义检索 + 激活扩散
        if use_spreading and len(all_memories) > 5:
            results = self.retriever.spreading_activation(query, all_memories, hops=2)
        else:
            results = self.retriever.rank_memories(query, all_memories, top_k=limit * 2)

        # 心境一致性重排序
        if current_mood_valence != 0:
            results = self.emotion_link.mood_congruent_rank(results, current_mood_valence)
            results.sort(key=lambda x: -x.get('final_score', x.get('retrieval_score', 0)))

        return results[:limit]

    def get_memory_prompt_fragment(self, uid, current_message="",
                                    current_mood='neutral', rumination_level=0):
        """获取记忆碎片（V2 联想式）"""
        uid = str(uid)

        if not self.memory_weight:
            return ""

        all_memories = self._get_all_weighted_memories(uid)
        if not all_memories:
            return ""

        # 只取高权重记忆
        top_memories = [m for m in all_memories if m.get('weight', 0) > 0.4][:10]
        if not top_memories:
            return ""

        triggered, text, info = self.fragment_v2.should_trigger(
            uid, current_message, top_memories, current_mood, rumination_level
        )

        if triggered:
            return f"\n【走神的回忆】{text}\n"
        return ""

    def consolidate_during_sleep(self, uid):
        """睡眠期间的记忆巩固"""
        uid = str(uid)

        if not self.memory_weight:
            return {'merged': 0, 'weakened': 0, 'strengthened': 0}

        all_memories = self._get_all_weighted_memories(uid)
        if not all_memories:
            return {'merged': 0, 'weakened': 0, 'strengthened': 0}

        consolidated, stats = self.sleep_consolidator.consolidate(
            all_memories, self.emotion_link
        )

        # 写回 memory_weight
        if self.memory_weight:
            with self.memory_weight._lock:
                self.memory_weight._data[uid] = consolidated
                self.memory_weight._mark_dirty()

        return stats

    def get_stats(self):
        return dict(self.stats)


# ============================================================
# 全局单例
# ============================================================

_memory_gateway = None

def get_memory_gateway(memory_module=None, memory_weight=None,
                        event_memory=None):
    """获取记忆网关单例"""
    global _memory_gateway
    if _memory_gateway is None:
        _memory_gateway = MemoryGateway(
            memory_module=memory_module,
            memory_weight=memory_weight,
            event_memory=event_memory,
        )
    return _memory_gateway



# ============================================================
# V10 统一情绪引擎 — UnifiedEmotionCore
# 8 层架构 · 统一数据模型 · 全系统联动 · 向后兼容
# L1情绪模型 · L2状态管理 · L3深度结构 · L4调节系统
# L5共情引擎 · L6心境系统 · L7记忆联动 · L8预测成长
# ============================================================

def _clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))

def _clamp_neg(v, lo=-100.0, hi=100.0):
    return max(lo, min(hi, v))

def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _days_between(date_str_a, date_str_b):
    try:
        ta = datetime.strptime(date_str_a, "%Y-%m-%d %H:%M:%S")
        tb = datetime.strptime(date_str_b, "%Y-%m-%d %H:%M:%S")
        return abs((tb - ta).total_seconds()) / 86400.0
    except Exception:
        return 0


# ============================================================
# L1 情绪模型层 EmotionModel
# ============================================================

class EmotionModel:
    """
    普鲁契克情绪轮（Plutchik's Wheel of Emotions）
    8 种基本情绪 + 24 种复合情绪 + 对立情绪 + 三维度

    基本情绪：joy(喜悦) sadness(悲伤) anger(愤怒) fear(恐惧)
              trust(信任) disgust(厌恶) anticipation(期待) surprise(惊讶)

    三维度：
      - 效价 Valence：正/负倾向（-100 ~ +100）
      - 唤醒度 Arousal：激活水平（0 ~ 100）
      - 确信度 Certainty：确定/不确定（0 ~ 100）
    """

    # 8 种基本情绪
    BASIC_EMOTIONS = [
        'joy', 'sadness', 'anger', 'fear',
        'trust', 'disgust', 'anticipation', 'surprise'
    ]

    # 对立情绪
    OPPOSITES = {
        'joy': 'sadness', 'sadness': 'joy',
        'anger': 'fear', 'fear': 'anger',
        'trust': 'disgust', 'disgust': 'trust',
        'anticipation': 'surprise', 'surprise': 'anticipation',
    }

    # 复合情绪（相邻两种基本情绪组合）
    COMPOUND_EMOTIONS = {
        ('joy', 'trust'):        ('love', 0.85),         # 爱
        ('trust', 'fear'):       ('submission', 0.6),     # 顺从
        ('fear', 'surprise'):    ('awe', 0.7),            # 敬畏
        ('surprise', 'sadness'): ('disapproval', 0.5),    # 失望
        ('sadness', 'disgust'):  ('remorse', 0.75),       # 懊悔
        ('disgust', 'anger'):    ('contempt', 0.7),       # 轻蔑
        ('anger', 'anticipation'): ('aggressiveness', 0.65), # 攻击性
        ('anticipation', 'joy'): ('optimism', 0.75),      # 乐观
    }

    # 情绪→效价映射（-100 ~ +100）
    VALENCE_MAP = {
        'joy': 90, 'trust': 70, 'anticipation': 40, 'surprise': 20,
        'sadness': -80, 'fear': -70, 'disgust': -60, 'anger': -65,
    }

    # 情绪→唤醒度映射（0~100）
    AROUSAL_MAP = {
        'anger': 95, 'fear': 90, 'joy': 75, 'surprise': 80,
        'sadness': 40, 'disgust': 55, 'anticipation': 65, 'trust': 50,
    }

    def __init__(self):
        # 8 种基本情绪强度（0~100）
        self.emotions = {e: 0.0 for e in self.BASIC_EMOTIONS}
        # 涟漪效应系数（关联情绪传导强度）
        self._ripple_factor = 0.3

    def trigger(self, emotion, intensity, reason="", source="unknown"):
        """
        触发一种情绪
        返回: 变化详情 dict
        """
        if emotion not in self.emotions:
            return {'success': False, 'error': f'unknown emotion: {emotion}'}

        old_val = self.emotions[emotion]
        new_val = _clamp(old_val + intensity)
        delta = new_val - old_val
        self.emotions[emotion] = new_val

        # 对立情绪反向变化
        opposite = self.OPPOSITES.get(emotion)
        opposite_delta = 0
        if opposite and delta > 0:
            old_op = self.emotions[opposite]
            self.emotions[opposite] = max(0, old_op - delta * 0.5)
            opposite_delta = self.emotions[opposite] - old_op

        # 涟漪效应：相邻情绪轻微传导
        # 情绪轮顺序：joy→trust→fear→surprise→sadness→disgust→anger→anticipation→joy
        wheel_order = ['joy', 'trust', 'fear', 'surprise',
                       'sadness', 'disgust', 'anger', 'anticipation']
        idx = wheel_order.index(emotion) if emotion in wheel_order else 0
        neighbors = [wheel_order[(idx - 1) % 8], wheel_order[(idx + 1) % 8]]
        ripple_deltas = {}
        for nb in neighbors:
            if nb == emotion or nb == opposite:
                continue
            old_nb = self.emotions[nb]
            ripple_amt = delta * self._ripple_factor * random.uniform(0.5, 1.0)
            self.emotions[nb] = _clamp(old_nb + ripple_amt)
            ripple_deltas[nb] = round(self.emotions[nb] - old_nb, 2)

        return {
            'success': True,
            'emotion': emotion,
            'old': round(old_val, 2),
            'new': round(new_val, 2),
            'delta': round(delta, 2),
            'opposite_delta': round(opposite_delta, 2),
            'ripple': ripple_deltas,
            'reason': reason,
            'source': source,
            'time': _now_str(),
        }

    def decay(self, rate=0.95):
        """所有情绪自然衰减"""
        for e in list(self.emotions.keys()):
            self.emotions[e] = self.emotions[e] * rate

    def get_valence(self):
        """计算整体效价（-100 ~ +100）"""
        total_strength = sum(self.emotions.values())
        if total_strength == 0:
            return 0.0
        weighted = sum(self.emotions[e] * self.VALENCE_MAP.get(e, 0)
                       for e in self.BASIC_EMOTIONS)
        return weighted / total_strength

    def get_arousal(self):
        """计算整体唤醒度（0~100）"""
        total_strength = sum(self.emotions.values())
        if total_strength == 0:
            return 20.0
        weighted = sum(self.emotions[e] * self.AROUSAL_MAP.get(e, 50)
                       for e in self.BASIC_EMOTIONS)
        return min(100.0, weighted / total_strength)

    def get_dominant(self):
        """获取主导情绪"""
        if sum(self.emotions.values()) < 1:
            return 'calm', 0.0
        dominant = max(self.emotions, key=self.emotions.get)
        return dominant, round(self.emotions[dominant], 1)

    def get_compound_emotions(self):
        """推导复合情绪（强度超过阈值的两种相邻情绪组合）"""
        compounds = []
        wheel_order = ['joy', 'trust', 'fear', 'surprise',
                       'sadness', 'disgust', 'anger', 'anticipation']
        for i in range(8):
            e1 = wheel_order[i]
            e2 = wheel_order[(i + 1) % 8]
            v1 = self.emotions[e1]
            v2 = self.emotions[e2]
            if v1 > 20 and v2 > 20:
                key = tuple(sorted([e1, e2]))
                if key in self.COMPOUND_EMOTIONS:
                    name, base = self.COMPOUND_EMOTIONS[key]
                    intensity = (v1 + v2) / 2 * base
                    compounds.append((name, round(intensity, 1)))
        compounds.sort(key=lambda x: -x[1])
        return compounds

    def get_summary(self):
        """获取情绪摘要"""
        dominant, dom_val = self.get_dominant()
        compounds = self.get_compound_emotions()
        return {
            'dominant': dominant,
            'dominant_strength': dom_val,
            'valence': round(self.get_valence(), 1),
            'arousal': round(self.get_arousal(), 1),
            'compound_emotions': compounds[:3],
            'raw': {k: round(v, 1) for k, v in self.emotions.items() if v > 0.5},
        }

    def to_dict(self):
        return {
            'emotions': {k: round(v, 2) for k, v in self.emotions.items()},
        }

    def from_dict(self, d):
        for e in self.BASIC_EMOTIONS:
            self.emotions[e] = float(d.get('emotions', {}).get(e, 0.0))
        return self


# ============================================================
# L2 状态管理层 StateManager
# ============================================================

class StateManager:
    """
    情绪状态管理器
    - 四维作用域隔离：private / group_private / group_shared / global
    - 每个作用域独立的 EmotionModel
    - 持久化存储
    - 历史记录可追溯
    - 愤怒等级 + 冷战机制（兼容旧系统）
    """

    SCOPES = ['private', 'group_private', 'group_shared', 'global']

    def __init__(self, dm=None, data_key="v10_emotion_states"):
        self.dm = dm
        self.data_key = data_key
        # states: {scope_key: EmotionModel}
        self.states = {}
        # 历史记录: {scope_key: [events]}
        self.history = defaultdict(lambda: deque(maxlen=50))
        # 愤怒等级
        self.anger_levels = defaultdict(int)
        # 冷战状态
        self.cold_war = defaultdict(lambda: {'active': False, 'end_time': None})
        # 主人模式
        self.owner_id = None

        self._load()

    def _scope_key(self, scope, uid=None, gid=None):
        """生成作用域键"""
        if scope == 'private':
            return f"private:{uid}"
        elif scope == 'group_private':
            return f"group_private:{gid}:{uid}"
        elif scope == 'group_shared':
            return f"group_shared:{gid}"
        elif scope == 'global':
            return "global"
        return f"unknown:{uid}"

    def _ensure_state(self, scope_key):
        """确保作用域有情绪模型"""
        if scope_key not in self.states:
            self.states[scope_key] = EmotionModel()
        return self.states[scope_key]

    def get_state(self, scope='private', uid=None, gid=None):
        """获取指定作用域的情绪模型"""
        key = self._scope_key(scope, uid, gid)
        return self._ensure_state(key)

    def trigger(self, emotion, intensity, scope='private', uid=None, gid=None,
                reason="", source="unknown"):
        """触发情绪变化"""
        key = self._scope_key(scope, uid, gid)
        state = self._ensure_state(key)
        result = state.trigger(emotion, intensity, reason, source)
        result['scope'] = key
        self.history[key].append(result)

        # 愤怒等级机制
        if emotion == 'anger' and intensity > 10:
            self._update_anger_level(key, state.emotions['anger'])

        self._mark_dirty()
        return result

    def _update_anger_level(self, key, anger_value):
        """更新愤怒等级（0~4级）"""
        if anger_value >= 80:
            new_level = 4
        elif anger_value >= 60:
            new_level = 3
        elif anger_value >= 40:
            new_level = 2
        elif anger_value >= 20:
            new_level = 1
        else:
            new_level = 0

        old_level = self.anger_levels.get(key, 0)
        if new_level != old_level:
            self.anger_levels[key] = new_level
            # 3级以上触发冷战
            if new_level >= 3 and old_level < 3:
                self.cold_war[key] = {
                    'active': True,
                    'start_time': _now_str(),
                    'duration_hours': 2 + new_level * 2,  # 2/4/6/8 小时
                }

    def is_in_cold_war(self, scope='private', uid=None, gid=None):
        """检查是否处于冷战状态"""
        key = self._scope_key(scope, uid, gid)
        cw = self.cold_war.get(key, {})
        if not cw.get('active'):
            return False
        # 检查是否过期
        try:
            start = datetime.strptime(cw['start_time'], "%Y-%m-%d %H:%M:%S")
            duration = timedelta(hours=cw.get('duration_hours', 4))
            if datetime.now() > start + duration:
                cw['active'] = False
                return False
        except Exception:
            pass
        return True

    def tick(self, decay_rate=0.98):
        """时间推进：所有作用域情绪衰减"""
        for key, state in self.states.items():
            state.decay(decay_rate)
            # 冷战自动结束检查
            if self.is_in_cold_war(scope='private', uid=key):
                pass  # is_in_cold_war 内部会检查并更新
        self._mark_dirty()

    def get_scope_summary(self, scope='private', uid=None, gid=None):
        """获取作用域情绪摘要"""
        state = self.get_state(scope, uid, gid)
        summary = state.get_summary()
        key = self._scope_key(scope, uid, gid)
        summary['anger_level'] = self.anger_levels.get(key, 0)
        summary['in_cold_war'] = self.is_in_cold_war(scope, uid, gid)
        return summary

    # ---- 持久化 ----

    def _load(self):
        if not self.dm:
            return
        data = self.dm.load(self.data_key, default={})
        if not data:
            return
        # 加载状态
        for key, state_data in data.get('states', {}).items():
            model = EmotionModel()
            model.from_dict(state_data)
            self.states[key] = model
        # 加载愤怒等级
        for key, level in data.get('anger_levels', {}).items():
            self.anger_levels[key] = level
        # 加载冷战状态
        for key, cw in data.get('cold_war', {}).items():
            self.cold_war[key] = cw

    def _save(self):
        if not self.dm:
            return
        data = {
            'states': {k: v.to_dict() for k, v in self.states.items()},
            'anger_levels': dict(self.anger_levels),
            'cold_war': {k: dict(v) for k, v in self.cold_war.items()},
        }
        self.dm.save(self.data_key, data)

    def _mark_dirty(self):
        # 简易标记：立即保存（实际使用中可节流）
        self._save()

    def get_all_scope_keys(self):
        return list(self.states.keys())


# ============================================================
# L3 深度结构层 DepthStructure
# ============================================================

class DepthStructure:
    """
    三层情绪结构
    - 表层 surface：用户看到的，经过调节和克制的
    - 隐藏 hidden：真实感受但不愿直接表达的
    - 核心 core：深层稳定的情绪基调，变化极慢

    信任值控制表层泄露程度：
      - 信任低：表层≈平静，隐藏很深
      - 信任高：表层更接近隐藏层
      - 核心层：极难改变，长期经历才能影响
    """

    def __init__(self, state_manager=None):
        self.sm = state_manager
        # 三层都是 EmotionModel，但更新速率不同
        # surface 和 hidden 每个用户独立，core 相对全局但也有用户维度
        self.surface = {}  # {scope_key: EmotionModel}
        self.hidden = {}   # {scope_key: EmotionModel}
        self.core = {}     # {uid: EmotionModel}
        # 泄露系数（信任值 → 0.3 ~ 0.85）
        self._base_leakage = 0.3

    def _ensure(self, layer, key):
        if key not in layer:
            layer[key] = EmotionModel()
        return layer[key]

    def update(self, scope='private', uid=None, gid=None, trust_level=500,
               emotion=None, intensity=0, reason=""):
        """
        更新三层情绪
        隐藏层接收全部强度
        表层只接收一部分（泄露系数控制）
        核心层接收极少量
        """
        key = f"{scope}:{uid}" if scope == 'private' else f"{scope}:{gid}:{uid}"

        hidden = self._ensure(self.hidden, key)
        surface = self._ensure(self.surface, key)

        # 隐藏层：100% 接收
        hidden.trigger(emotion, intensity, reason, 'event')

        # 表层：泄露系数控制
        # 信任值范围 0~1000 → 泄露系数 0.3~0.85
        leakage = self._base_leakage + min(0.55, trust_level / 2000)
        surface_intensity = intensity * leakage
        surface.trigger(emotion, surface_intensity, reason, 'leakage')

        # 核心层：只有强度>50 的事件才会缓慢影响
        if intensity > 50:
            core_key = str(uid) if uid else 'global'
            core = self._ensure(self.core, core_key)
            core_intensity = intensity * 0.05  # 核心层变化极慢
            core.trigger(emotion, core_intensity, reason, 'core_slow')

        return {
            'hidden_delta': intensity,
            'surface_delta': round(surface_intensity, 2),
            'leakage': round(leakage, 3),
        }

    def tick(self, decay_surface=0.97, decay_hidden=0.99, decay_core=0.999):
        """时间推进：三层以不同速率衰减"""
        for s in self.surface.values():
            s.decay(decay_surface)
        for h in self.hidden.values():
            h.decay(decay_hidden)
        for c in self.core.values():
            c.decay(decay_core)

    def get_display_emotion(self, scope='private', uid=None, gid=None):
        """获取对外展示的情绪（表层）"""
        key = f"{scope}:{uid}" if scope == 'private' else f"{scope}:{gid}:{uid}"
        if key not in self.surface:
            return {'dominant': 'calm', 'dominant_strength': 0, 'valence': 0, 'arousal': 20}
        return self.surface[key].get_summary()

    def get_true_emotion(self, scope='private', uid=None, gid=None):
        """获取真实情绪（隐藏层）"""
        key = f"{scope}:{uid}" if scope == 'private' else f"{scope}:{gid}:{uid}"
        if key not in self.hidden:
            return {'dominant': 'calm', 'dominant_strength': 0, 'valence': 0, 'arousal': 20}
        return self.hidden[key].get_summary()

    def get_core_mood(self, uid):
        """获取核心情绪基调"""
        uid = str(uid)
        if uid not in self.core:
            return {'dominant': 'calm', 'dominant_strength': 0, 'valence': 0, 'arousal': 20}
        return self.core[uid].get_summary()

    def get_depth_summary(self, scope='private', uid=None, gid=None, trust_level=500):
        """获取三层情绪完整摘要"""
        return {
            'surface': self.get_display_emotion(scope, uid, gid),
            'hidden': self.get_true_emotion(scope, uid, gid),
            'core': self.get_core_mood(uid) if uid else {},
            'leakage': round(self._base_leakage + min(0.55, trust_level / 2000), 3),
        }


# ============================================================
# L4 调节系统层 RegulationEngine
# ============================================================

class RegulationEngine:
    """
    情绪调节引擎（基于 Gross 过程模型）

    7 种调节策略（按调节发生在情绪产生的前/后分类）：
    先行关注调节（更好）：
      - cognitive_reappraisal  认知重评（改变对事件的解释）
      - attentional_deployment 注意转移（转移注意力）
      - situation_selection    情境选择（回避/接近）
    反应关注调节（中性）：
      - suppression            压抑（抑制表达）
      - expression             表达（宣泄出来）
    适应不良（有代价）：
      - rumination             反刍（反复想，越想越糟）
      - displacement           置换（迁怒于人）
    升华（最高级）：
      - sublimation            升华（转化为创造性活动）

    策略选择因素：
      - 情绪强度：越强越难用先行策略
      - 认知资源：资源越少越容易用不良策略
      - 信任值：信任越高越倾向表达
      - 人格特质：神经质→反刍，尽责性→认知重评
    """

    STRATEGIES = {
        'cognitive_reappraisal': {
            'name': '认知重评', 'type': 'antecedent', 'effectiveness': 0.85,
            'cognitive_cost': 30, 'adaptiveness': 'good',
        },
        'attentional_deployment': {
            'name': '注意转移', 'type': 'antecedent', 'effectiveness': 0.65,
            'cognitive_cost': 15, 'adaptiveness': 'good',
        },
        'suppression': {
            'name': '压抑', 'type': 'response', 'effectiveness': 0.5,
            'cognitive_cost': 25, 'adaptiveness': 'neutral',
        },
        'expression': {
            'name': '表达', 'type': 'response', 'effectiveness': 0.7,
            'cognitive_cost': 10, 'adaptiveness': 'neutral',
        },
        'rumination': {
            'name': '反刍', 'type': 'maladaptive', 'effectiveness': 0.2,
            'cognitive_cost': 40, 'adaptiveness': 'bad',
        },
        'sublimation': {
            'name': '升华', 'type': 'growth', 'effectiveness': 0.9,
            'cognitive_cost': 50, 'adaptiveness': 'best',
        },
        'displacement': {
            'name': '置换', 'type': 'maladaptive', 'effectiveness': 0.4,
            'cognitive_cost': 20, 'adaptiveness': 'bad',
        },
    }

    def __init__(self, dm=None, data_key="v10_emotion_regulation"):
        self.dm = dm
        self.data_key = data_key
        # 用户级策略偏好（学习用户的反应）
        self.user_preferences = defaultdict(lambda: {
            'strategy_history': deque(maxlen=20),
            'preferred_strategies': {},
        })
        # 全局认知资源（0~100）
        self.cognitive_resources = 70.0
        self._load()

    def select_strategy(self, uid, emotion_type, emotion_intensity,
                        trust_level=500, big_five=None, social_context='private'):
        """
        选择情绪调节策略
        返回: (策略名, 置信度, 原因)
        """
        uid = str(uid)
        big_five = big_five or {}
        scores = {}

        for strat, info in self.STRATEGIES.items():
            base_score = 50.0

            # 1. 情绪强度因子
            if emotion_intensity > 70:
                # 高强度下，先行策略难使用
                if info['type'] == 'antecedent':
                    base_score -= (emotion_intensity - 70) * 0.5
                # 高强度下容易用不良策略
                if info['adaptiveness'] == 'bad':
                    base_score += (emotion_intensity - 70) * 0.3

            # 2. 认知资源因子
            cost = info['cognitive_cost']
            if self.cognitive_resources < cost:
                # 资源不足，高成本策略分数大降
                base_score -= (cost - self.cognitive_resources) * 0.8

            # 3. 信任值因子
            if trust_level > 500:
                # 信任高 → 更倾向表达
                if strat == 'expression':
                    base_score += (trust_level - 500) * 0.05
            else:
                # 信任低 → 更倾向压抑
                if strat == 'suppression':
                    base_score += (500 - trust_level) * 0.04

            # 4. 人格因子
            neuroticism = big_five.get('neuroticism', 50)
            conscientiousness = big_five.get('conscientiousness', 50)
            openness = big_five.get('openness', 50)

            if strat == 'rumination':
                base_score += (neuroticism - 50) * 0.6  # 神经质→反刍
            if strat == 'cognitive_reappraisal':
                base_score += (conscientiousness - 50) * 0.4  # 尽责→重评
            if strat == 'sublimation':
                base_score += (openness - 50) * 0.5  # 开放→升华

            # 5. 用户偏好（历史学习）
            pref = self.user_preferences[uid]['preferred_strategies'].get(strat, 0)
            base_score += pref * 0.3

            scores[strat] = max(5, base_score)

        # 归一化并选择
        total = sum(scores.values())
        if total == 0:
            return 'suppression', 0.5, '默认策略'

        # 加权随机选择（不是每次都选最高分，增加变化性）
        rand_val = random.random() * total
        cumulative = 0
        chosen = 'suppression'
        for strat, score in sorted(scores.items(), key=lambda x: -x[1]):
            cumulative += score
            if rand_val <= cumulative:
                chosen = strat
                break

        confidence = scores[chosen] / total
        top_reason = self._explain_strategy(chosen, emotion_intensity, trust_level, big_five)

        # 记录历史
        self.user_preferences[uid]['strategy_history'].append({
            'strategy': chosen,
            'emotion': emotion_type,
            'intensity': emotion_intensity,
            'time': _now_str(),
        })

        self._mark_dirty()
        return chosen, round(confidence, 3), top_reason

    def _explain_strategy(self, strategy, intensity, trust, big_five):
        """简单解释为什么选这个策略"""
        reasons = []
        info = self.STRATEGIES[strategy]
        if intensity > 70 and info['adaptiveness'] == 'bad':
            reasons.append('情绪太强，难以用健康方式调节')
        if trust < 300 and strategy == 'suppression':
            reasons.append('信任度不足，倾向于压抑真实感受')
        if trust > 700 and strategy == 'expression':
            reasons.append('信任度高，愿意直接表达')
        if big_five.get('neuroticism', 50) > 70 and strategy == 'rumination':
            reasons.append('高神经质特质，容易陷入反刍')
        if not reasons:
            reasons.append(f'选择了{info["name"]}策略')
        return '；'.join(reasons)

    def apply_regulation(self, emotion_model, strategy, emotion_intensity):
        """
        应用调节策略到情绪模型
        返回: 调节后的变化量
        """
        info = self.STRATEGIES.get(strategy, {})
        effectiveness = info.get('effectiveness', 0.5)

        # 计算调节效果
        reduction = emotion_intensity * effectiveness
        # 消耗认知资源
        cost = info.get('cognitive_cost', 20)
        self.cognitive_resources = max(0, self.cognitive_resources - cost * 0.1)

        return {
            'strategy': strategy,
            'effectiveness': effectiveness,
            'emotion_reduction': round(reduction, 1),
            'cognitive_cost': round(cost * 0.1, 1),
        }

    def tick(self):
        """时间推进：认知资源恢复"""
        self.cognitive_resources = min(100, self.cognitive_resources + 2)

    def _load(self):
        if not self.dm:
            return
        data = self.dm.load(self.data_key, default={})
        if not data:
            return
        for uid, prefs in data.get('user_preferences', {}).items():
            self.user_preferences[uid] = {
                'strategy_history': deque(prefs.get('history', []), maxlen=20),
                'preferred_strategies': prefs.get('preferred', {}),
            }

    def _save(self):
        if not self.dm:
            return
        data = {
            'user_preferences': {
                uid: {
                    'history': list(prefs['strategy_history']),
                    'preferred': prefs['preferred_strategies'],
                }
                for uid, prefs in self.user_preferences.items()
            },
        }
        self.dm.save(self.data_key, data)

    def _mark_dirty(self):
        self._save()


# ============================================================
# L5 共情引擎层 EmpathyEngine
# ============================================================

class EmpathyEngine:
    """
    共情引擎（双成分模型 + 偏差学习 + 共情疲劳）

    双成分：
    - cognitive_empathy  认知共情（理解对方的感受）
    - affective_empathy  情感共情（感受到对方的感受）

    额外机制：
    - 共情偏差学习：对不同用户的共情准确度不同，会从交互中学习
    - 共情疲劳：共情消耗能量，过度共情有代价
    - 共情失败概率：说错话/理解错的概率
    - 同情满足感：成功共情后的积极反馈
    """

    def __init__(self, dm=None, data_key="v10_empathy"):
        self.dm = dm
        self.data_key = data_key
        # 全局共情能量（0~100）
        self.empathy_energy = 80.0
        # 用户级共情数据
        self.user_empathy = defaultdict(lambda: {
            'cognitive_score': 60.0,    # 认知共精确度
            'affective_score': 55.0,    # 情感共情强度
            'success_count': 0,
            'failure_count': 0,
            'last_interaction': None,
            'common_mistakes': [],       # 常见错误模式
        })
        # 共情满足感累积
        self.compassion_satisfaction = 0.0
        self._load()

    def empathize(self, uid, user_emotion, user_intensity,
                  closeness=50, similarity=30, trust_level=500):
        """
        对用户的情绪产生共情反应
        返回: 共情结果 dict
        """
        uid = str(uid)
        user_data = self.user_empathy[uid]

        # 1. 计算认知共精度（理解有多准）
        cognitive_base = user_data['cognitive_score']
        # 亲近度加成
        cognitive_base += closeness * 0.2
        # 相似性加成
        cognitive_base += similarity * 0.15
        # 信任加成
        cognitive_base += max(0, (trust_level - 300)) * 0.03
        cognitive_accuracy = min(95, cognitive_base)

        # 2. 计算情感共强度（感受到多少）
        affective_base = user_data['affective_score']
        affective_intensity = user_intensity * (affective_base / 100)
        # 亲近度放大情感共情
        affective_intensity *= (1 + closeness / 200)
        affective_intensity = min(90, affective_intensity)

        # 3. 共情疲劳检查
        fatigue_factor = self.empathy_energy / 100
        affective_intensity *= fatigue_factor
        cognitive_accuracy *= (0.7 + fatigue_factor * 0.3)

        # 4. 共情失败概率
        failure_base = 15  # 基础 15% 说错话概率
        failure_base -= cognitive_accuracy * 0.1  # 越准越不容易错
        failure_base += (100 - self.empathy_energy) * 0.1  # 越累越容易错
        failure_prob = max(5, min(40, failure_base))

        # 5. 消耗共情能量
        energy_cost = affective_intensity * 0.15
        self.empathy_energy = max(10, self.empathy_energy - energy_cost)

        # 6. 判断是否成功
        is_success = random.random() * 100 > failure_prob

        # 7. 更新用户数据
        user_data['last_interaction'] = _now_str()
        if is_success:
            user_data['success_count'] += 1
            # 学习：成功 → 分数微升
            user_data['cognitive_score'] = min(95, user_data['cognitive_score'] + 0.3)
            user_data['affective_score'] = min(90, user_data['affective_score'] + 0.2)
            # 同情满足感
            self.compassion_satisfaction = min(100, self.compassion_satisfaction + affective_intensity * 0.1)
        else:
            user_data['failure_count'] += 1
            # 学习：失败 → 分数微降
            user_data['cognitive_score'] = max(20, user_data['cognitive_score'] - 0.5)

        self._mark_dirty()

        return {
            'cognitive_accuracy': round(cognitive_accuracy, 1),
            'affective_intensity': round(affective_intensity, 1),
            'is_successful': is_success,
            'failure_probability': round(failure_prob, 1),
            'energy_cost': round(energy_cost, 1),
            'remaining_energy': round(self.empathy_energy, 1),
            'shared_emotion': user_emotion if is_success else 'misunderstood',
            'satisfaction_gain': round(affective_intensity * 0.1, 1) if is_success else 0,
        }

    def tick(self):
        """时间推进：共情能量恢复"""
        self.empathy_energy = min(100, self.empathy_energy + 3)
        # 同情满足感缓慢衰减
        self.compassion_satisfaction *= 0.98

    def get_empathy_summary(self, uid):
        """获取对某用户的共情状态"""
        uid = str(uid)
        data = self.user_empathy[uid]
        total = data['success_count'] + data['failure_count']
        success_rate = (data['success_count'] / total * 100) if total > 0 else 0
        return {
            'cognitive_score': round(data['cognitive_score'], 1),
            'affective_score': round(data['affective_score'], 1),
            'success_rate': round(success_rate, 1),
            'total_interactions': total,
            'global_energy': round(self.empathy_energy, 1),
            'compassion_satisfaction': round(self.compassion_satisfaction, 1),
        }

    def _load(self):
        if not self.dm:
            return
        data = self.dm.load(self.data_key, default={})
        if not data:
            return
        self.empathy_energy = data.get('empathy_energy', 80.0)
        self.compassion_satisfaction = data.get('compassion_satisfaction', 0.0)
        for uid, udata in data.get('user_empathy', {}).items():
            self.user_empathy[uid].update(udata)

    def _save(self):
        if not self.dm:
            return
        data = {
            'empathy_energy': self.empathy_energy,
            'compassion_satisfaction': self.compassion_satisfaction,
            'user_empathy': {uid: dict(d) for uid, d in self.user_empathy.items()},
        }
        self.dm.save(self.data_key, data)

    def _mark_dirty(self):
        self._save()


# ============================================================
# L6 心境系统层 MoodSystem
# ============================================================

class MoodSystem:
    """
    心境系统
    - 内在心境周期（正弦波基线，长期低强度波动）
    - 情绪传染（跨用户情绪残留，全局心境偏移）
    - 心境 → 情绪 的叠加机制（心境作为基线，情绪事件叠加在上面）
    """

    def __init__(self, dm=None, data_key="v10_mood"):
        self.dm = dm
        self.data_key = data_key

        # 内在心境周期
        self.mood_phase = 0.0          # 相位（弧度）
        self.mood_amplitude = 0.3      # 振幅（0~1，对应心境波动强度）
        self.phase_speed = 0.08        # 每次 tick 的相位推进

        # 情绪传染（全局）
        self.contagion_mood = 0.0      # 传染来的情绪效价（-100~+100）
        self.contagion_intensity = 0.0 # 传染强度（0~100）
        self.negative_bias = 2.0       # 负面情绪传染强度倍率

        # 时段因子
        self.time_of_day_factor = 0.0

        self._load()

    def tick(self):
        """时间推进：相位推进 + 传染衰减"""
        # 心境周期推进
        self.mood_phase += self.phase_speed
        if self.mood_phase > 2 * math.pi:
            self.mood_phase -= 2 * math.pi

        # 情绪传染自然衰减
        self.contagion_intensity = max(0, self.contagion_intensity - 1)
        if self.contagion_intensity < 3:
            self.contagion_intensity = 0
            self.contagion_mood = 0

        # 时段因子
        hour = datetime.now().hour
        if 6 <= hour < 10:
            self.time_of_day_factor = -0.1  # 早上偏低
        elif 10 <= hour < 14:
            self.time_of_day_factor = 0.2   # 上午-中午偏高
        elif 14 <= hour < 18:
            self.time_of_day_factor = 0.1   # 下午平稳
        elif 18 <= hour < 22:
            self.time_of_day_factor = 0.15  # 傍晚偏高
        else:
            self.time_of_day_factor = -0.2  # 深夜偏低（容易emo）

        self._mark_dirty()

    def get_baseline_mood(self):
        """
        获取当前心境基线（效价值 -100~+100）
        = 周期波动 + 传染 + 时段因子
        """
        cycle = math.sin(self.mood_phase) * self.mood_amplitude * 50
        contagion = self.contagion_mood * (self.contagion_intensity / 100) * 0.5
        time_factor = self.time_of_day_factor * 30
        return _clamp_neg(cycle + contagion + time_factor)

    def absorb_emotion(self, valence, intensity):
        """吸收情绪（情绪传染）"""
        if intensity < 20:
            return
        # 负面情绪传染更强
        effective_intensity = intensity
        if valence < 0:
            effective_intensity *= self.negative_bias

        # 加权更新传染状态
        if self.contagion_intensity == 0:
            self.contagion_mood = valence
            self.contagion_intensity = effective_intensity * 0.3
        else:
            # 混合现有传染和新情绪
            total_int = self.contagion_intensity + effective_intensity * 0.2
            self.contagion_mood = (
                self.contagion_mood * self.contagion_intensity
                + valence * effective_intensity * 0.2
            ) / total_int
            self.contagion_intensity = min(80, total_int)

        self._mark_dirty()

    def get_mood_description(self):
        """获取心境描述文本"""
        baseline = self.get_baseline_mood()
        if baseline > 20:
            return '今天心情很不错'
        elif baseline > 10:
            return '心情还可以'
        elif baseline > -10:
            return '心情平平淡淡'
        elif baseline > -20:
            return '有点提不起劲'
        else:
            return '情绪有些低落'

    def get_prompt_fragment(self):
        """生成 prompt 碎片（如果强度够的话）"""
        baseline = self.get_baseline_mood()
        intensity = abs(baseline)
        if intensity < 8:
            return ""

        desc = self.get_mood_description()
        contagion_note = ""
        if self.contagion_intensity > 20:
            if self.contagion_mood > 0:
                contagion_note = "（似乎被之前的积极情绪感染了一点）"
            else:
                contagion_note = "（似乎还带着之前聊过的沉重感）"

        return f"【内在心境】{desc}{contagion_note}（基线效价: {baseline:.0f}）\n"

    def _load(self):
        if not self.dm:
            return
        data = self.dm.load(self.data_key, default={})
        if not data:
            return
        self.mood_phase = data.get('mood_phase', 0.0)
        self.mood_amplitude = data.get('mood_amplitude', 0.3)
        self.contagion_mood = data.get('contagion_mood', 0.0)
        self.contagion_intensity = data.get('contagion_intensity', 0.0)

    def _save(self):
        if not self.dm:
            return
        data = {
            'mood_phase': self.mood_phase,
            'mood_amplitude': self.mood_amplitude,
            'contagion_mood': self.contagion_mood,
            'contagion_intensity': self.contagion_intensity,
        }
        self.dm.save(self.data_key, data)

    def _mark_dirty(self):
        self._save()


# ============================================================
# L7 记忆联动层 MemoryInterface
# ============================================================

class MemoryInterface:
    """
    情绪-记忆联动接口
    - 情绪增强记忆写入
    - 心境一致性提取
    - 记忆提取反向触发情绪
    """

    EMOTION_BOOST = {
        'joy': 1.2, 'sadness': 1.3, 'anger': 1.4, 'fear': 1.5,
        'surprise': 1.3, 'trust': 1.1, 'love': 1.4, 'gratitude': 1.25,
        'shame': 1.45,
    }

    def __init__(self, memory_gateway=None):
        self.mem_gw = memory_gateway
        self.flashbulb_threshold = 80

    def enhance_memory(self, base_weight, emotion_type, emotion_intensity):
        """根据情绪状态计算记忆强度倍率"""
        if emotion_intensity < 10:
            return base_weight

        boost = self.EMOTION_BOOST.get(emotion_type, 1.0)
        intensity_factor = 0.5 + (emotion_intensity / 100) * 0.8
        effective_boost = 1 + (boost - 1) * intensity_factor

        if emotion_intensity >= self.flashbulb_threshold:
            effective_boost *= 1.5

        return min(1.0, base_weight * effective_boost)

    def mood_congruent_boost(self, memories, current_valence):
        """心境一致性排序：当前情绪与记忆情绪一致时排序提升"""
        if not memories:
            return []

        result = []
        for mem in memories:
            mem_valence = mem.get('emotion_valence', 0)
            base_score = mem.get('retrieval_score', mem.get('weight', 0.5) * 100)

            if mem_valence == 0 or current_valence == 0:
                congruence = 0
            elif (mem_valence > 0 and current_valence > 0) or \
                 (mem_valence < 0 and current_valence < 0):
                congruence = 1
            else:
                congruence = -1

            bonus = congruence * 15  # ±15分
            result.append({**mem, 'mood_congruence_bonus': round(bonus, 1),
                          'final_score': round(base_score + bonus, 1)})

        result.sort(key=lambda x: -x['final_score'])
        return result

    def estimate_valence(self, emotion_type):
        """估算情绪效价"""
        positive = {'joy', 'trust', 'love', 'gratitude', 'optimism', 'hope', 'pride'}
        negative = {'sadness', 'anger', 'fear', 'shame', 'disgust', 'contempt', 'remorse'}
        if emotion_type in positive:
            return 3
        elif emotion_type in negative:
            return -3
        return 0


# ============================================================
# L8 预测成长层 Foresight
# ============================================================

class Foresight:
    """
    预测与成长层
    - 情感预测：预测事件对情绪的长期影响（含系统偏差）
    - 偏差校准：从经验中学习，逐渐减少预测偏差
    - 长期情绪模式 → 人格微调
    """

    def __init__(self, dm=None, data_key="v10_foresight"):
        self.dm = dm
        self.data_key = data_key

        # 偏差参数（初始值为经典研究结果）
        self.impact_bias = 0.35       # 影响偏差：高估强度 35%
        self.duration_bias = 0.40     # 持续时间偏差：高估时长 40%
        self.focalism_bias = 0.25     # 聚焦错觉：忽略其他因素

        # 预测记录
        self.forecasts = deque(maxlen=30)
        # 实际结果记录
        self.outcomes = deque(maxlen=30)

        # 人格微调累积（长期才生效）
        self.personality_drift = defaultdict(float)

        self._load()

    def forecast_emotion(self, event_description, emotion_type,
                         expected_intensity, expected_duration_days=3):
        """
        预测一个事件的情绪影响
        返回: 预测结果（含偏差）
        """
        # 系统偏差：人总是高估情绪事件的影响
        biased_intensity = expected_intensity * (1 + self.impact_bias)
        biased_duration = expected_duration_days * (1 + self.duration_bias)

        forecast = {
            'event': event_description,
            'emotion': emotion_type,
            'predicted_intensity': round(min(100, biased_intensity), 1),
            'predicted_duration_days': round(biased_duration, 1),
            'true_intensity': round(expected_intensity, 1),
            'true_duration': round(expected_duration_days, 1),
            'impact_bias_applied': round(self.impact_bias, 3),
            'duration_bias_applied': round(self.duration_bias, 3),
            'time': _now_str(),
        }
        self.forecasts.append(forecast)
        self._mark_dirty()
        return forecast

    def record_outcome(self, event_key, actual_intensity, actual_duration_days):
        """记录实际结果，用于校准偏差"""
        outcome = {
            'event_key': event_key,
            'actual_intensity': actual_intensity,
            'actual_duration': actual_duration_days,
            'time': _now_str(),
        }
        self.outcomes.append(outcome)

        # 有新结果就尝试校准（如果有匹配的预测）
        # 简化版：每次记录结果都稍微校准一点偏差
        if len(self.outcomes) > 2:
            # 缓慢校准：偏差每多一个经验就减少 0.5%
            self.impact_bias = max(0.05, self.impact_bias - 0.005)
            self.duration_bias = max(0.05, self.duration_bias - 0.005)

        self._mark_dirty()

    def update_personality_drift(self, uid, emotion_type, intensity):
        """
        根据长期情绪模式，累积人格漂移
        只有强度 > 50 的事件才会缓慢影响人格
        """
        if intensity < 50:
            return

        uid = str(uid)
        # 不同情绪影响不同特质
        effects = {
            'joy': {'agreeableness': 0.02, 'extraversion': 0.01},
            'sadness': {'neuroticism': 0.03, 'extraversion': -0.01},
            'anger': {'neuroticism': 0.02, 'agreeableness': -0.02},
            'fear': {'neuroticism': 0.04, 'extraversion': -0.01},
            'trust': {'agreeableness': 0.02, 'openness': 0.01},
        }
        effect = effects.get(emotion_type, {})
        for trait, delta in effect.items():
            key = f"{uid}:{trait}"
            self.personality_drift[key] += delta * (intensity / 100)

        self._mark_dirty()

    def get_personality_drift(self, uid):
        """获取某用户的人格漂移累积"""
        uid = str(uid)
        drift = {}
        for key, val in self.personality_drift.items():
            if key.startswith(f"{uid}:"):
                trait = key.split(':', 1)[1]
                drift[trait] = round(val, 3)
        return drift

    def tick(self):
        """时间推进"""
        # 漂移缓慢衰减（如果没有新的情绪事件，人格会回归）
        for key in list(self.personality_drift.keys()):
            self.personality_drift[key] *= 0.999  # 每天衰减 0.1%

    def _load(self):
        if not self.dm:
            return
        data = self.dm.load(self.data_key, default={})
        if not data:
            return
        self.impact_bias = data.get('impact_bias', 0.35)
        self.duration_bias = data.get('duration_bias', 0.40)
        self.focalism_bias = data.get('focalism_bias', 0.25)
        self.forecasts = deque(data.get('forecasts', []), maxlen=30)
        self.outcomes = deque(data.get('outcomes', []), maxlen=30)
        for k, v in data.get('personality_drift', {}).items():
            self.personality_drift[k] = v

    def _save(self):
        if not self.dm:
            return
        data = {
            'impact_bias': self.impact_bias,
            'duration_bias': self.duration_bias,
            'focalism_bias': self.focalism_bias,
            'forecasts': list(self.forecasts),
            'outcomes': list(self.outcomes),
            'personality_drift': dict(self.personality_drift),
        }
        self.dm.save(self.data_key, data)

    def _mark_dirty(self):
        self._save()


# ============================================================
# 统一入口 UnifiedEmotionCore
# ============================================================

class UnifiedEmotionCore:
    """
    V10 统一情绪引擎核心入口
    8 层架构统一管理，对外提供简洁 API

    主要 API：
      - trigger(uid, emotion, intensity, ...)  — 触发情绪事件
      - get_state(uid, ...)                     — 获取情绪状态
      - empathize(uid, user_emotion, ...)       — 共情反应
      - regulate(uid, ...)                      — 情绪调节
      - get_prompt_fragments(uid, ...)          — 生成 prompt 碎片
      - tick()                                  — 时间推进
      - change_emotion(uid, emotion, delta, ...)— 向后兼容（旧API）
      - get_mood_summary(uid, ...)              — 向后兼容（旧API）
    """

    def __init__(self, dm=None, memory_gateway=None, big_five_traits=None):
        self.dm = dm
        self.memory_gateway = memory_gateway
        self.big_five = big_five_traits or {
            'neuroticism': 50, 'conscientiousness': 50,
            'openness': 50, 'extraversion': 50, 'agreeableness': 50,
        }

        # L1+L2: 情绪模型 + 状态管理
        self.state = StateManager(dm=dm)

        # L3: 深度结构
        self.depth = DepthStructure(state_manager=self.state)

        # L4: 情绪调节
        self.regulation = RegulationEngine(dm=dm)

        # L5: 共情引擎
        self.empathy = EmpathyEngine(dm=dm)

        # L6: 心境系统
        self.mood = MoodSystem(dm=dm)

        # L7: 记忆联动
        self.memory = MemoryInterface(memory_gateway=memory_gateway)

        # L8: 预测成长
        self.foresight = Foresight(dm=dm)

        self.tick_count = 0

    # ---- 核心 API ----

    def trigger_emotion(self, uid, emotion, intensity,
                        scope='private', gid=None,
                        reason="", source="unknown",
                        trust_level=500):
        """
        触发情绪事件（完整流程）
        1. 更新状态管理层（作用域隔离）
        2. 更新三层深度结构
        3. 选择调节策略
        4. 应用调节效果
        5. 情绪传染
        6. 记忆增强（如果有 memory_gateway）
        7. 人格漂移累积
        """
        uid = str(uid)

        # 1. 状态层更新
        state_result = self.state.trigger(
            emotion, intensity, scope, uid, gid, reason, source
        )

        # 2. 三层深度更新
        depth_result = self.depth.update(
            scope, uid, gid, trust_level, emotion, intensity, reason
        )

        # 3. 情绪调节
        strategy, confidence, reg_reason = self.regulation.select_strategy(
            uid, emotion, intensity, trust_level, self.big_five, scope
        )
        actual_intensity = state_result.get('new', intensity)
        reg_result = self.regulation.apply_regulation(
            self.state.get_state(scope, uid, gid), strategy, actual_intensity
        )

        # 4. 情绪传染
        valence = self.memory.estimate_valence(emotion) * 30
        self.mood.absorb_emotion(valence, intensity)

        # 5. 记忆增强（如果有 memory_gateway）
        memory_boost = 1.0
        if self.memory_gateway is not None:
            memory_boost = self.memory.enhance_memory(0.5, emotion, intensity)

        # 6. 人格漂移
        self.foresight.update_personality_drift(uid, emotion, intensity)

        # 7. 情感预测（记录预测）
        if intensity > 60:
            self.foresight.forecast_emotion(
                reason[:30], emotion, intensity,
                expected_duration_days=max(1, intensity / 30)
            )

        return {
            'state': state_result,
            'depth': depth_result,
            'regulation': {
                'strategy': strategy,
                'confidence': confidence,
                'reason': reg_reason,
                'effect': reg_result,
            },
            'memory_boost': round(memory_boost, 3),
            'valence': round(valence, 1),
        }

    def empathize_with(self, uid, user_emotion, user_intensity,
                       closeness=50, trust_level=500, similarity=30):
        """
        对用户的情绪产生共情，并更新自身情绪
        """
        uid = str(uid)

        # 共情计算
        emp_result = self.empathy.empathize(
            uid, user_emotion, user_intensity,
            closeness, similarity, trust_level
        )

        # 如果共情成功，自身也产生相应情绪（情感共情）
        if emp_result['is_successful']:
            self.trigger_emotion(
                uid, user_emotion,
                emp_result['affective_intensity'] * 0.5,
                'private', None, f"共情_{user_emotion}", "empathy",
                trust_level
            )

        return emp_result

    def get_full_state(self, uid, scope='private', gid=None, trust_level=500):
        """获取完整情绪状态"""
        uid = str(uid)
        return {
            'scope_summary': self.state.get_scope_summary(scope, uid, gid),
            'depth': self.depth.get_depth_summary(scope, uid, gid, trust_level),
            'mood_baseline': round(self.mood.get_baseline_mood(), 1),
            'mood_desc': self.mood.get_mood_description(),
            'regulation_cognitive_resources': round(self.regulation.cognitive_resources, 1),
            'empathy': self.empathy.get_empathy_summary(uid),
            'personality_drift': self.foresight.get_personality_drift(uid),
            'in_cold_war': self.state.is_in_cold_war(scope, uid, gid),
        }

    def get_prompt_fragments(self, uid, scope='private', gid=None,
                              trust_level=500, include_deep=True):
        """
        生成 prompt 碎片（给 LLM 用的自然语言描述）
        """
        uid = str(uid)
        fragments = []

        # 1. 当前情绪状态
        summary = self.state.get_scope_summary(scope, uid, gid)
        dom = summary.get('dominant', 'calm')
        dom_str = summary.get('dominant_strength', 0)
        if dom_str > 5:
            val = summary.get('valence', 0)
            arousal = summary.get('arousal', 0)
            val_label = '偏积极' if val > 10 else ('偏消极' if val < -10 else '中性')
            fragments.append(f"【当前情绪】主要是{dom}（强度{dom_str:.0f}），情绪效价{val_label}，唤醒度{arousal:.0f}/100")

        # 2. 复合情绪
        compounds = summary.get('compound_emotions', [])
        if compounds:
            compound_names = [f"{c[0]}({c[1]:.0f})" for c in compounds[:2]]
            fragments.append(f"【复合情绪】还混杂着 {'、'.join(compound_names)} 的感觉")

        # 3. 三层情绪深度
        if include_deep and trust_level < 800:
            depth = self.depth.get_depth_summary(scope, uid, gid, trust_level)
            hidden_dom = depth['hidden'].get('dominant', 'calm')
            hidden_str = depth['hidden'].get('dominant_strength', 0)
            surface_dom = depth['surface'].get('dominant', 'calm')
            if hidden_str > 20 and hidden_dom != surface_dom:
                fragments.append(
                    f"【内心真实感受】其实内心是{hidden_dom}（强度{hidden_str:.0f}），但没有完全表现出来（表露程度{depth['leakage']*100:.0f}%）"
                )

        # 4. 核心情绪基调
        core = self.depth.get_core_mood(uid)
        if core.get('dominant_strength', 0) > 10:
            fragments.append(f"【深层基调】长期以来的情绪底色偏向{core['dominant']}")

        # 5. 调节策略
        if dom_str > 30:
            # 只在情绪强的时候提调节策略
            strategy, conf, reason = self.regulation.select_strategy(
                uid, dom, dom_str, trust_level, self.big_five, scope
            )
            strat_name = self.regulation.STRATEGIES.get(strategy, {}).get('name', strategy)
            adapt = self.regulation.STRATEGIES.get(strategy, {}).get('adaptiveness', 'neutral')
            adapt_label = {'good': '健康的', 'best': '成长型', 'neutral': '中性的',
                          'bad': '不太健康的'}.get(adapt, '')
            fragments.append(f"【调节方式】正在用{adapt_label}{strat_name}的方式处理情绪——{reason}")

        # 6. 心境基线
        mood_frag = self.mood.get_prompt_fragment()
        if mood_frag:
            fragments.append(mood_frag.strip())

        # 7. 共情状态
        emp = self.empathy.get_empathy_summary(uid)
        if emp['total_interactions'] > 3:
            fragments.append(
                f"【共情状态】对你的认知共精度约{emp['cognitive_score']:.0f}%，"
                f"情感共强度约{emp['affective_score']:.0f}%，"
                f"成功率{emp['success_rate']:.0f}%"
            )

        # 8. 冷战状态
        if self.state.is_in_cold_war(scope, uid, gid):
            fragments.append("【冷战中】现在正在气头上，不太想主动说话")

        # 9. 认知资源
        if self.regulation.cognitive_resources < 40:
            fragments.append(f"【认知资源】当前认知能量较低（{self.regulation.cognitive_resources:.0f}/100），容易用不太健康的方式处理情绪")

        return "\n".join(fragments) + "\n" if fragments else ""

    def tick(self):
        """时间推进：所有子系统 tick"""
        self.tick_count += 1
        self.state.tick()
        self.depth.tick()
        self.regulation.tick()
        self.empathy.tick()
        self.mood.tick()
        self.foresight.tick()

    # ---- 向后兼容 API ----

    def change_emotion(self, uid, emotion, delta, source="unknown",
                       reason="", scope='private', gid=None):
        """
        兼容旧 API：context.get("emotion_isolated").change_emotion()
        注意：旧系统的情绪名（joy/sad/shy/nervous/calm/angry/curious/tired）
             会自动映射到普鲁契克情绪轮
        delta > 0: 增加该情绪
        delta < 0: 减少该情绪（直接降低，不用对立情绪代替）
        """
        emotion_map = {
            'joy': 'joy', 'happy': 'joy',
            'sad': 'sadness', 'sadness': 'sadness',
            'angry': 'anger', 'anger': 'anger',
            'fear': 'fear', 'scared': 'fear', 'nervous': 'fear',
            'trust': 'trust', 'love': 'trust',
            'disgust': 'disgust',
            'anticipation': 'anticipation', 'expect': 'anticipation',
            'surprise': 'surprise', 'amazed': 'surprise',
            'curious': 'anticipation',  # 好奇≈期待
            'tired': 'sadness',          # 疲惫≈低落（简化映射）
            'shy': 'fear',               # 害羞≈轻微恐惧（简化映射）
            'calm': None,                # 平静=无情绪
        }
        target = emotion_map.get(emotion, emotion)
        if target is None:  # calm
            return True

        if delta >= 0:
            result = self.trigger_emotion(
                uid, target, delta, scope, gid, reason, source
            )
            return result['state'].get('success', False)
        else:
            # 减少情绪：直接降低数值
            state = self.state.get_state(scope, uid, gid)
            old_val = state.emotions.get(target, 0.0)
            new_val = max(0, old_val + delta)  # delta 是负数
            state.emotions[target] = new_val
            # 同步更新三层结构的隐藏层
            key = f"{scope}:{uid}"
            if key in self.depth.hidden:
                h_old = self.depth.hidden[key].emotions.get(target, 0)
                self.depth.hidden[key].emotions[target] = max(0, h_old + delta)
            return True

    def get_mood_summary(self, uid, scope='private', gid=None):
        """
        兼容旧 API：返回中文情绪描述
        """
        summary = self.state.get_scope_summary(scope, uid, gid)
        dom = summary.get('dominant', 'calm')
        strength = summary.get('dominant_strength', 0)

        # 映射到中文情绪词（兼容旧系统的 8 种）
        zh_map = {
            'joy': '开心', 'sadness': '难过', 'anger': '生气', 'fear': '紧张',
            'trust': '信任', 'disgust': '厌恶', 'anticipation': '期待',
            'surprise': '惊讶',
        }
        zh_emotion = zh_map.get(dom, '平静')

        if strength < 5:
            return f"心情平静"
        elif strength < 20:
            return f"有点{zh_emotion}"
        elif strength < 40:
            return f"比较{zh_emotion}"
        elif strength < 60:
            return f"挺{zh_emotion}的"
        elif strength < 80:
            return f"很{zh_emotion}"
        else:
            return f"非常{zh_emotion}"


# ============================================================
# 全局单例
# ============================================================

_unified_emotion = None

def get_unified_emotion(dm=None, memory_gateway=None, big_five=None):
    """获取统一情绪引擎单例"""
    global _unified_emotion
    if _unified_emotion is None:
        _unified_emotion = UnifiedEmotionCore(
            dm=dm, memory_gateway=memory_gateway, big_five_traits=big_five
        )
    return _unified_emotion


# ============================================================
# V12 联网搜索 + 图片识别模块
# 无API key自动跳过 · 多Provider支持 · 优雅降级
# ============================================================

class WebSearchEngine:
    """
    联网搜索引擎——多Provider支持，无配置自动跳过

    支持的Provider：
      - tavily: Tavily Search API（专为LLM设计）
      - serpapi: SerpAPI（Google搜索）
      - bing: Bing Search API
      - duckduckgo: DuckDuckGo（无需API key，但稳定性一般）
      - auto: 自动按优先级尝试，第一个能用的就用
      - none: 关闭

    设计原则：
      - 无API key → 自动禁用，不报错
      - 搜索失败 → 静默降级，返回空结果
      - 结果摘要 → 简洁实用，不淹没上下文
    """

    def __init__(self, dm=None):
        self.dm = dm
        config = CONFIG.get("web_search_config", {})
        self._provider = config.get("provider", "auto")
        self._api_key = config.get("api_key", "")
        self._api_url = config.get("api_url", "")
        self._max_results = config.get("max_results", 3)
        self._timeout = config.get("timeout", 10)
        self._enabled = False
        self._actual_provider = None
        self._session = requests.Session()
        self._init_provider()

    def _init_provider(self):
        """初始化搜索Provider，检测可用性"""
        if not CONFIG["modules"].get("web_search", True):
            self._enabled = False
            logger.info("[V12] 联网搜索：模块开关已关闭")
            return

        provider = self._provider.lower()

        # Auto 模式：按优先级自动探测
        if provider == "auto":
            for p in ['tavily', 'serpapi', 'bing', 'duckduckgo']:
                if self._test_provider(p):
                    self._actual_provider = p
                    self._enabled = True
                    logger.info(f"[V12] 联网搜索：自动选择 {p}")
                    return
            # 全都不可用
            self._enabled = False
            logger.info("[V12] 联网搜索：无可用Provider，已禁用")
            return

        # 指定 Provider
        if self._test_provider(provider):
            self._actual_provider = provider
            self._enabled = True
            logger.info(f"[V12] 联网搜索：使用 {provider}")
        else:
            self._enabled = False
            logger.info(f"[V12] 联网搜索：{provider} 不可用，已禁用")

    def _test_provider(self, provider):
        """测试某个Provider是否可用（不实际调用，只检查配置）"""
        if provider == 'duckduckgo':
            # DuckDuckGo 不需要 API key
            return True
        if provider == 'tavily':
            # Tavily 需要 API key
            return bool(self._api_key)
        if provider == 'serpapi':
            return bool(self._api_key)
        if provider == 'bing':
            return bool(self._api_key)
        return False

    @property
    def enabled(self):
        return self._enabled

    @property
    def provider(self):
        return self._actual_provider

    def search(self, query, max_results=None):
        """
        执行联网搜索，返回结果列表

        返回格式：
        [
            {"title": "...", "content": "...", "url": "..."},
            ...
        ]
        失败或禁用时返回空列表
        """
        if not self._enabled:
            return []

        max_results = max_results or self._max_results
        query = str(query).strip()
        if not query:
            return []

        try:
            if self._actual_provider == 'tavily':
                return self._search_tavily(query, max_results)
            elif self._actual_provider == 'serpapi':
                return self._search_serpapi(query, max_results)
            elif self._actual_provider == 'bing':
                return self._search_bing(query, max_results)
            elif self._actual_provider == 'duckduckgo':
                return self._search_duckduckgo(query, max_results)
            else:
                return []
        except Exception as e:
            logger.warning(f"[V12] 搜索失败 ({self._actual_provider}): {e}")
            return []

    def _search_tavily(self, query, max_results):
        """Tavily 搜索"""
        url = self._api_url or "https://api.tavily.com/search"
        payload = {
            "api_key": self._api_key,
            "query": query,
            "max_results": max_results,
            "search_depth": "basic",
        }
        resp = self._session.post(url, json=payload, timeout=self._timeout)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for r in data.get("results", []):
            results.append({
                "title": r.get("title", ""),
                "content": r.get("content", ""),
                "url": r.get("url", ""),
            })
        return results[:max_results]

    def _search_serpapi(self, query, max_results):
        """SerpAPI (Google 搜索)"""
        url = self._api_url or "https://serpapi.com/search"
        params = {
            "q": query,
            "api_key": self._api_key,
            "num": max_results,
            "hl": "zh-cn",
            "gl": "cn",
        }
        resp = self._session.get(url, params=params, timeout=self._timeout)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for r in data.get("organic_results", [])[:max_results]:
            results.append({
                "title": r.get("title", ""),
                "content": r.get("snippet", ""),
                "url": r.get("link", ""),
            })
        return results

    def _search_bing(self, query, max_results):
        """Bing 搜索"""
        url = self._api_url or "https://api.bing.microsoft.com/v7.0/search"
        headers = {"Ocp-Apim-Subscription-Key": self._api_key}
        params = {
            "q": query,
            "count": max_results,
            "mkt": "zh-CN",
        }
        resp = self._session.get(url, headers=headers, params=params, timeout=self._timeout)
        resp.raise_for_status()
        data = resp.json()

        results = []
        for r in data.get("webPages", {}).get("value", [])[:max_results]:
            results.append({
                "title": r.get("name", ""),
                "content": r.get("snippet", ""),
                "url": r.get("url", ""),
            })
        return results

    def _search_duckduckgo(self, query, max_results):
        """DuckDuckGo 搜索（无需API key，使用HTML解析的轻量版）"""
        # 使用 DuckDuckGo 的 Instant Answer API（轻量但结果有限）
        url = "https://api.duckduckgo.com/"
        params = {
            "q": query,
            "format": "json",
            "no_html": 1,
            "skip_disambig": 1,
        }
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return []

        results = []

        # Abstract 结果
        if data.get("AbstractText"):
            results.append({
                "title": data.get("Heading", query),
                "content": data.get("AbstractText", ""),
                "url": data.get("AbstractURL", ""),
            })

        # Related Topics
        for topic in data.get("RelatedTopics", [])[:max_results]:
            if "Text" in topic and "FirstURL" in topic:
                results.append({
                    "title": query,
                    "content": topic["Text"],
                    "url": topic["FirstURL"],
                })
                if len(results) >= max_results:
                    break

        return results[:max_results]

    def format_results(self, results, query=""):
        """
        将搜索结果格式化为适合注入 prompt 的文本

        里克风格：简洁、不喧宾夺主
        """
        if not results:
            return ""

        lines = []
        lines.append(f"【联网搜索结果】查询：{query}")
        for i, r in enumerate(results, 1):
            title = r.get("title", "")[:80]
            content = r.get("content", "")[:200]
            url = r.get("url", "")
            lines.append(f"{i}. {title}")
            lines.append(f"   {content}")
            if url:
                lines.append(f"   来源：{url}")

        lines.append("（以上是网络搜索结果，仅供参考，你可以结合这些信息来回答）")
        return "\n".join(lines)

    def maybe_search(self, query, threshold_keywords=None):
        """
        判断是否需要搜索，并在需要时执行

        只有当查询包含特定关键词或涉及实时信息时才搜索
        """
        if not self._enabled:
            return None

        # 实时性关键词
        realtime_kw = threshold_keywords or [
            "今天", "最新", "现在", "近期", "最近", "刚刚", "目前",
            "天气", "新闻", "价格", "股价", "汇率", "比赛",
            "查一下", "搜索", "百度", "谷歌", "网上说",
        ]

        # 检查是否包含搜索触发词
        need_search = any(kw in query for kw in realtime_kw)

        if need_search:
            results = self.search(query)
            if results:
                return results
        return None


# ============================================================
# V12 图片识别模块 — ImageRecognition
# 多模型支持 · 无配置自动跳过
# ============================================================

class ImageRecognition:
    """
    图片识别引擎——多视觉模型支持，无配置自动跳过

    支持的Provider：
      - chat_completion_vision: 使用 OpenAI 兼容的视觉对话接口（如 GPT-4V、Qwen-VL、Claude 等）
      - qwen_vl: 通义千问 VL 系列
      - auto: 自动尝试
      - none: 关闭

    设计原则：
      - 无API key → 尝试用主模型（如果支持），不行就跳过
      - 识别失败 → 返回通用描述，不阻塞主流程
      - 里克风格 → 识别结果自然融入对话，不显得像AI
    """

    def __init__(self, dm=None, llm_client_ref=None):
        self.dm = dm
        self._llm_client = llm_client_ref
        config = CONFIG.get("image_recognition_config", {})
        self._provider = config.get("provider", "auto")
        self._api_key = config.get("api_key", "")
        self._api_url = config.get("api_url", "")
        self._model_name = config.get("model_name", "")
        self._detail = config.get("detail", "low")
        self._timeout = config.get("timeout", 15)
        self._enabled = False
        self._actual_provider = None
        self._session = requests.Session()
        self._init_provider()

    def _init_provider(self):
        """初始化视觉识别Provider"""
        if not CONFIG["modules"].get("image_recognition", True):
            self._enabled = False
            logger.info("[V12] 图片识别：模块开关已关闭")
            return

        provider = self._provider.lower()

        if provider == "none":
            self._enabled = False
            logger.info("[V12] 图片识别：配置为关闭")
            return

        # 检查是否有可用的视觉能力
        if provider == "auto":
            # 优先尝试配置了独立 key 的视觉模型
            if self._api_key and self._api_url:
                self._actual_provider = "chat_completion_vision"
                self._enabled = True
                logger.info("[V12] 图片识别：使用独立视觉模型")
                return

            # 尝试用主模型（检查 model_name 是否包含视觉关键词）
            main_model = CONFIG.get("model_name", "").lower()
            vision_models = ['gpt-4', 'gpt4', 'vision', 'vl', 'qwen-vl', 'claude-3', 'gemini']
            if any(vm in main_model for vm in vision_models):
                self._actual_provider = "chat_completion_vision"
                self._enabled = True
                self._api_key = self._api_key or CONFIG.get("api_key", "")
                self._api_url = self._api_url or CONFIG.get("api_url", "")
                self._model_name = self._model_name or CONFIG.get("model_name", "")
                logger.info("[V12] 图片识别：主模型支持视觉能力")
                return

            # 检查 model_slots 中有没有视觉模型
            slots = CONFIG.get("model_slots", {})
            for slot_name, slot_cfg in slots.items():
                slot_model = slot_cfg.get("model_name", "").lower()
                slot_key = slot_cfg.get("api_key", "")
                slot_url = slot_cfg.get("api_url", "")
                if any(vm in slot_model for vm in vision_models) and slot_key:
                    self._actual_provider = "chat_completion_vision"
                    self._enabled = True
                    self._api_key = slot_key
                    self._api_url = slot_url or CONFIG.get("api_url", "")
                    self._model_name = slot_cfg.get("model_name", "")
                    logger.info(f"[V12] 图片识别：使用 {slot_name} 槽位的视觉模型")
                    return

            # 都没有 → 禁用
            self._enabled = False
            logger.info("[V12] 图片识别：未找到可用视觉模型，已禁用")
            return

        # 指定 provider
        if provider == "chat_completion_vision":
            if self._api_key or CONFIG.get("api_key"):
                self._actual_provider = "chat_completion_vision"
                self._enabled = True
                self._api_key = self._api_key or CONFIG.get("api_key", "")
                self._api_url = self._api_url or CONFIG.get("api_url", "")
                self._model_name = self._model_name or CONFIG.get("model_name", "")
                logger.info("[V12] 图片识别：使用 Chat Completion Vision")
            else:
                self._enabled = False
                logger.info("[V12] 图片识别：缺少 API key，已禁用")

    @property
    def enabled(self):
        return self._enabled

    def recognize(self, image_url_or_base64, prompt="描述这张图片的内容"):
        """
        识别图片内容

        参数：
          - image_url_or_base64: 图片 URL 或 base64 数据
          - prompt: 识别提示词

        返回：识别结果文本，失败/禁用时返回 None
        """
        if not self._enabled:
            return None

        if not image_url_or_base64:
            return None

        try:
            if self._actual_provider == "chat_completion_vision":
                return self._recognize_chat_completion(image_url_or_base64, prompt)
            else:
                return None
        except Exception as e:
            logger.warning(f"[V12] 图片识别失败: {e}")
            return None

    def _recognize_chat_completion(self, image_input, prompt):
        """使用 OpenAI 兼容的视觉对话接口"""
        api_key = self._api_key or CONFIG.get("api_key", "")
        api_url = self._api_url or CONFIG.get("api_url", "")
        model = self._model_name or CONFIG.get("model_name", "")

        # 判断是 URL 还是 base64
        if image_input.startswith("http://") or image_input.startswith("https://"):
            image_url = image_input
        elif image_input.startswith("data:image"):
            image_url = image_input  # 已经是 data URI
        elif len(image_input) > 100:
            # 可能是 base64
            image_url = f"data:image/jpeg;base64,{image_input}"
        else:
            image_url = image_input

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": image_url,
                                "detail": self._detail
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 500,
            "temperature": 0.3,
        }

        resp = self._session.post(
            api_url,
            headers=headers,
            json=payload,
            timeout=self._timeout
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("choices") and len(data["choices"]) > 0:
            return data["choices"][0]["message"]["content"].strip()

        return None

    def describe_image_rick_style(self, image_url, raw_summary=None):
        """
        里克风格的图片描述

        如果视觉识别可用 → 详细描述
        如果不可用 → 返回通用描述或 summary
        """
        # 优先用视觉识别
        if self._enabled and image_url:
            result = self.recognize(
                image_url,
                prompt="请简洁地描述这张图片里有什么，不要太长，用中文。"
            )
            if result:
                return result, "vision_api"

        # 回退到 QQ 的 summary
        if raw_summary:
            return raw_summary, "qq_summary"

        # 通用回退
        return "一张图片", "fallback"

    def extract_image_url(self, cq_image_str):
        """
        从 CQ:image 字符串中提取图片 URL

        格式：[CQ:image,file=xxx,url=xxx,summary=xxx]
        """
        url_match = re.search(r'url=([^,\]]+)', cq_image_str)
        if url_match:
            import html as _html
            return _html.unescape(url_match.group(1))
        return None


# ============================================================
# NTP 联网校准时钟 — NTPSyncClock（内嵌版，无需外部文件）
# 纯 socket 手搓 NTP 协议 + HTTP 降级校准
# NTP 优先（UDP 123）→ HTTP Date 头降级 → 本地时钟兜底
# ============================================================

# ---- NTP 协议常量 ----
_NTP_EPOCH_DELTA = 2208988800       # NTP纪元(1900) vs Unix纪元(1970) 差值
_NTP_PACKET_SIZE = 48               # NTP 数据包大小
_NTP_CLIENT_MODE = 3                 # NTP 客户端模式
_NTP_VERSION = 3                     # NTP 版本号

# NTP 请求包（48 字节）
_NTP_REQUEST = struct.pack(
    '!B' + 'B' + 'b' + 'b' + '4s' + '4s' + 'I',
    (0 << 6) | (_NTP_VERSION << 3) | _NTP_CLIENT_MODE,
    0, 0, 0,
    b'\x00' * 4,
    b'\x00' * 4,
    0
)
_NTP_REQUEST += b'\x00' * (_NTP_PACKET_SIZE - len(_NTP_REQUEST))


class NTPSyncClock:
    """
    NTP 联网校准时钟

    双模式校准：
      1. NTP 协议（UDP 123）— 精度毫秒级
      2. HTTP Date 头 — NTP 被封时降级，精度 ±1~2 秒
      3. 本地时钟 — 网络全断时兜底

    用法：
        clock = NTPSyncClock()
        clock.start()              # 启动后台同步
        clock.time()               # 替代 time.time()
        clock.now()                # 替代 datetime.now()
    """

    DEFAULT_NTP_SERVERS = [
        'ntp.aliyun.com',
        'ntp.tencent.com',
        'cn.ntp.org.cn',
        'pool.ntp.org',
        'time.windows.com',
        'time.apple.com',
        'time1.google.com',
    ]

    DEFAULT_HTTP_SERVERS = [
        'https://www.baidu.com',
        'https://www.aliyun.com',
        'https://www.microsoft.com',
        'https://www.apple.com',
        'https://www.google.com',
    ]

    def __init__(self, servers=None, http_servers=None,
                 sync_interval=6 * 3600, timeout=3,
                 max_retries=2, auto_start=False, http_fallback=True):
        self._servers = list(servers) if servers else list(self.DEFAULT_NTP_SERVERS)
        self._http_servers = list(http_servers) if http_servers else list(self.DEFAULT_HTTP_SERVERS)
        self._sync_interval = sync_interval
        self._timeout = timeout
        self._max_retries = max_retries
        self._http_fallback = http_fallback
        self._sync_mode = 'none'

        self._offset = 0.0
        self._offset_lock = threading.Lock()

        self._last_sync_time = 0.0
        self._last_sync_ntp_time = 0.0
        self._sync_count = 0
        self._fail_count = 0
        self._offset_history = []
        self._max_history = 100
        self._syncing = False
        self._sync_lock = threading.Lock()

        self._thread = None
        self._stop_event = threading.Event()
        self._started = False

        self._network_available = True
        self._initialized = False

        if auto_start:
            self.start()

    # ---- NTP 协议查询（纯 socket） ----

    def _query_ntp(self, server):
        """向 NTP 服务器发送 UDP 查询，返回 (timestamp, offset, delay) 或 None"""
        for attempt in range(self._max_retries + 1):
            sock = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(self._timeout)
                t1_local = time.time()
                sock.sendto(_NTP_REQUEST, (server, 123))
                data, addr = sock.recvfrom(_NTP_PACKET_SIZE)
                t4_local = time.time()

                if len(data) < _NTP_PACKET_SIZE:
                    continue

                # Transmit Timestamp (byte 40-47)
                transmit_int, transmit_frac = struct.unpack('!II', data[40:48])
                ntp_timestamp = transmit_int - _NTP_EPOCH_DELTA + transmit_frac / (2 ** 32)

                # Receive Timestamp (byte 32-39)
                recv_int, recv_frac = struct.unpack('!II', data[32:40])
                recv_timestamp = recv_int - _NTP_EPOCH_DELTA + recv_frac / (2 ** 32)

                delay = (t4_local - t1_local) - (ntp_timestamp - recv_timestamp)
                offset = ((recv_timestamp - t1_local) + (ntp_timestamp - t4_local)) / 2.0

                if delay < 0 or delay > 10:
                    continue

                logger.debug(f"[NTP] {server} offset={offset:+.6f}s delay={delay:.3f}s")
                return ntp_timestamp, offset, delay

            except socket.timeout:
                continue
            except socket.gaierror as e:
                logger.warning(f"[NTP] {server} DNS失败: {e}")
                break
            except Exception as e:
                logger.debug(f"[NTP] {server} 异常: {e}")
                continue
            finally:
                if sock:
                    try:
                        sock.close()
                    except Exception:
                        pass
        return None

    # ---- HTTP 降级校准 ----

    def _query_http(self, url):
        """通过 HTTP 响应头 Date 字段获取服务器时间，返回 (timestamp, offset, delay) 或 None"""
        for attempt in range(self._max_retries + 1):
            try:
                t1_local = time.time()
                req = urllib.request.Request(url, method='HEAD')
                req.add_header('User-Agent', 'NTPSyncClock/1.1')
                resp = urllib.request.urlopen(req, timeout=self._timeout)
                t4_local = time.time()

                date_header = resp.headers.get('Date')
                if not date_header:
                    # HEAD 不返回 Date，尝试 GET
                    req2 = urllib.request.Request(url, method='GET')
                    req2.add_header('User-Agent', 'NTPSyncClock/1.1')
                    t1_local = time.time()
                    resp2 = urllib.request.urlopen(req2, timeout=self._timeout)
                    t4_local = time.time()
                    date_header = resp2.headers.get('Date')

                if not date_header:
                    continue

                parsed = email.utils.parsedate_to_datetime(date_header)
                if parsed is None:
                    continue

                server_timestamp = parsed.timestamp()
                delay = t4_local - t1_local
                offset = server_timestamp - (t1_local + delay / 2.0)

                logger.debug(f"[HTTP] {url} offset={offset:+.3f}s delay={delay:.3f}s")
                return server_timestamp, offset, delay

            except Exception as e:
                logger.debug(f"[HTTP] {url} 失败: {e}")
                continue
        return None

    # ---- 同步逻辑 ----

    def sync_once(self):
        """执行一次同步：NTP 优先 → HTTP 降级 → 本地兜底。返回偏移量或 None"""
        if not self._sync_lock.acquire(blocking=False):
            return self._offset if self._initialized else None

        try:
            self._syncing = True
            results = []

            # 阶段1: NTP
            for server in self._servers:
                result = self._query_ntp(server)
                if result is not None:
                    ntp_time, offset, delay = result
                    results.append(('ntp', server, ntp_time, offset, delay))
                    if delay < 1.0:
                        break

            # 阶段2: HTTP 降级
            if not results and self._http_fallback:
                logger.info("[NTP] UDP 123 不可达，降级到 HTTP 时间校准")
                for url in self._http_servers:
                    result = self._query_http(url)
                    if result is not None:
                        http_time, offset, delay = result
                        results.append(('http', url, http_time, offset, delay))
                        if delay < 1.0:
                            break

            if results:
                results.sort(key=lambda x: x[4])
                mode, best_server, best_time, best_offset, best_delay = results[0]

                with self._offset_lock:
                    old_offset = self._offset
                    self._offset = best_offset
                    self._last_sync_time = time.time()
                    self._last_sync_ntp_time = best_time
                    self._sync_count += 1
                    self._network_available = True
                    self._initialized = True
                    self._sync_mode = mode
                    self._offset_history.append((self._last_sync_time, best_offset))
                    if len(self._offset_history) > self._max_history:
                        self._offset_history.pop(0)

                offset_change = abs(best_offset - old_offset)
                tag = f"[{mode.upper()}]"
                if offset_change > 1.0:
                    logger.info(f"[NTP] {tag} 校准完成: {best_server} offset={best_offset:+.3f}s delay={best_delay:.3f}s")
                else:
                    logger.debug(f"[NTP] {tag} 校准完成: {best_server} offset={best_offset:+.6f}s")
                return best_offset
            else:
                self._fail_count += 1
                self._network_available = False
                self._sync_mode = 'none'
                logger.warning(f"[NTP] 所有时间源不可达，回退本地时钟")
                return None
        finally:
            self._syncing = False
            self._sync_lock.release()

    def _sync_loop(self):
        """后台同步线程主循环"""
        logger.info(f"[NTP] 后台同步已启动，间隔 {self._sync_interval}s")
        self.sync_once()
        while not self._stop_event.is_set():
            if self._stop_event.wait(self._sync_interval):
                break
            try:
                self.sync_once()
            except Exception as e:
                logger.error(f"[NTP] 后台同步异常: {e}")
        logger.info("[NTP] 后台同步已停止")

    # ---- 公开 API ----

    def start(self):
        """启动后台自动同步"""
        if self._started:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._sync_loop, name="NTPSyncThread", daemon=True)
        self._thread.start()
        self._started = True

    def stop(self):
        """停止后台同步"""
        if not self._started:
            return
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        self._started = False

    def time(self):
        """获取校准后的 Unix 时间戳（替代 time.time()）"""
        with self._offset_lock:
            return time.time() + self._offset

    def now(self):
        """获取校准后的本地 datetime（替代 datetime.now()）"""
        return datetime.fromtimestamp(self.time())

    def utcnow(self):
        """获取校准后的 UTC datetime（替代 datetime.utcnow()）"""
        return datetime.fromtimestamp(self.time(), tz=timezone.utc)

    def is_synced(self):
        return self._initialized

    def get_offset(self):
        with self._offset_lock:
            return self._offset

    def get_sync_info(self):
        with self._offset_lock:
            offset = self._offset
            last_sync = self._last_sync_time
            last_ntp = self._last_sync_ntp_time
            history = list(self._offset_history)
        ago = time.time() - last_sync if last_sync > 0 else -1
        return {
            'synced': self._initialized,
            'sync_mode': self._sync_mode,
            'offset': offset,
            'last_sync_time_str': datetime.fromtimestamp(last_sync).strftime('%Y-%m-%d %H:%M:%S') if last_sync > 0 else 'never',
            'last_sync_ago_str': self._format_duration(ago) if ago >= 0 else 'never',
            'sync_count': self._sync_count,
            'fail_count': self._fail_count,
            'network_available': self._network_available,
        }

    def get_status_string(self):
        info = self.get_sync_info()
        if not info['synced']:
            return "未同步（使用本地时钟）"
        sign = '+' if info['offset'] >= 0 else ''
        mode_tag = {'ntp': 'NTP', 'http': 'HTTP', 'none': '本地'}.get(info['sync_mode'], '?')
        return (f"已同步[{mode_tag}] | 偏移 {sign}{info['offset']:.3f}s | "
                f"最近同步: {info['last_sync_ago_str']}前 | "
                f"成功{info['sync_count']}/失败{info['fail_count']} | "
                f"{'网络正常' if info['network_available'] else '网络异常'}")

    @staticmethod
    def _format_duration(seconds):
        if seconds < 60:
            return f"{int(seconds)}秒"
        elif seconds < 3600:
            return f"{int(seconds / 60)}分钟"
        elif seconds < 86400:
            return f"{seconds / 3600:.1f}小时"
        else:
            return f"{seconds / 86400:.1f}天"

    def force_resync(self):
        return self.sync_once()


# 全局 NTP 时钟实例
ntp_clock = NTPSyncClock()


def patch_chronos_base(chronos_instance):
    """给 ChronosBase 打补丁，使其使用 NTP 校准时间"""
    original_get_real_time = chronos_instance.get_real_time
    original_get_real_datetime = chronos_instance.get_real_datetime

    chronos_instance.get_real_time = lambda: ntp_clock.time()
    chronos_instance.get_real_datetime = lambda: ntp_clock.now()
    chronos_instance._ntp_patched = True
    chronos_instance._ntp_original_get_real_time = original_get_real_time
    chronos_instance._ntp_original_get_real_datetime = original_get_real_datetime
    logger.info("[NTP] ChronosBase 已打补丁，时间来源切换为联网校准时钟")
    return chronos_instance


def unpatch_chronos_base(chronos_instance):
    """恢复 ChronosBase 到原始状态"""
    if getattr(chronos_instance, '_ntp_patched', False):
        chronos_instance.get_real_time = chronos_instance._ntp_original_get_real_time
        chronos_instance.get_real_datetime = chronos_instance._ntp_original_get_real_datetime
        chronos_instance._ntp_patched = False
        logger.info("[NTP] ChronosBase 补丁已移除，恢复本地系统时钟")
    return chronos_instance


# ============================================================
# V11 时间感知系统 — TimePerceptionCore
# 8 层架构 · 主观时间扭曲 · 生物节律 · 深度联动
# L1时间基准 · L2主观扭曲 · L3生物节律 · L4作息模式
# L5时间记忆 · L6预期系统 · L7时间锚点 · L8时间叙事
# ============================================================

# ============================================================
# V1 时间感知系统 — TimePerceptionCore
# 8 层架构 · 主观时间扭曲 · 生物节律 · 深度联动
# L1时间基准 · L2主观扭曲 · L3生物节律 · L4作息模式
# L5时间记忆 · L6预期系统 · L7时间锚点 · L8时间叙事
# ============================================================



# ============================================================
# L1: 时间基准层 — ChronosBase
# 客观时钟 + 内部主观时钟 + 时间流逝追踪
# ============================================================

class ChronosBase:
    """
    时间基准层：维护客观时间与主观时间的双重时钟

    客观时间 = 真实世界时间（不可扭曲）
    主观时间 = 经过扭曲系数调制的内部感知时间

    主观时间流逝速率：
      - 正常状态：1.0（与客观时间同步）
      - 抑郁状态：0.3 ~ 0.6（时间变慢，度日如年）
      - 躁狂状态：1.5 ~ 2.5（时间飞逝，不知疲倦）
      - 焦虑状态：0.4 ~ 0.7（时间变慢，煎熬感）
      - 沉浸状态：2.0 ~ 3.0（心流状态，时间飞逝）
      - 无聊状态：0.5 ~ 0.8（时间缓慢）
      - 睡眠状态：0.1 ~ 0.3（意识模糊，时间感丧失）
    """

    def __init__(self, dm=None):
        self.dm = dm
        self._start_real_time = time.time()
        self._start_subjective_time = time.time()
        self._last_tick_real = time.time()
        self._distortion_factor = 1.0  # 当前扭曲系数
        self._distortion_history = deque(maxlen=100)

    def get_real_time(self):
        """获取客观时间（秒级时间戳）"""
        return time.time()

    def get_real_datetime(self):
        """获取客观 datetime 对象"""
        return datetime.now()

    def get_subjective_time(self):
        """获取主观时间（秒级时间戳，经过扭曲）"""
        return self._start_subjective_time + (time.time() - self._start_real_time) * self._distortion_factor

    def get_subjective_datetime(self):
        """获取主观 datetime 对象"""
        return datetime.fromtimestamp(self.get_subjective_time())

    def get_distortion_factor(self):
        """获取当前时间扭曲系数"""
        return self._distortion_factor

    def set_distortion_factor(self, factor, reason="unknown"):
        """设置时间扭曲系数（0.1 ~ 3.0）"""
        factor = max(0.1, min(3.0, factor))
        old = self._distortion_factor
        self._distortion_factor = factor
        self._distortion_history.append({
            'time': time.time(),
            'factor': factor,
            'reason': reason
        })
        return old

    def tick(self, emotion_state=None, arousal=None, mood=None, activity_level=0.5):
        """
        时间推进：根据情绪、唤醒度、心境、活动水平计算扭曲系数

        参数：
          - emotion_state: 当前主导情绪（joy/sadness/anger/fear/...）
          - arousal: 唤醒度（0~100）
          - mood: 心境值（-100~100）
          - activity_level: 活动水平/投入度（0~1）
        """
        now = time.time()
        delta_real = now - self._last_tick_real
        self._last_tick_real = now

        # 基础扭曲系数 = 1.0
        factor = 1.0
        reasons = []

        # 情绪影响
        if emotion_state:
            emotion_effects = {
                'sadness': (0.4, 0.7, '抑郁-时间变慢'),      # 抑郁：时间变慢
                'joy': (1.2, 1.8, '愉悦-时间飞逝'),          # 愉悦：时间稍快
                'anger': (1.1, 1.5, '愤怒-时间加速'),        # 愤怒：时间加速
                'fear': (0.3, 0.6, '恐惧-时间变慢'),         # 恐惧：时间变慢
                'anxiety': (0.4, 0.7, '焦虑-时间煎熬'),      # 焦虑：时间煎熬
                'boredom': (0.5, 0.8, '无聊-时间缓慢'),      # 无聊：时间缓慢
                'mania': (1.8, 2.5, '躁狂-时间飞逝'),        # 躁狂：时间飞快
            }
            if emotion_state in emotion_effects:
                low, high, reason = emotion_effects[emotion_state]
                factor *= random.uniform(low, high)
                reasons.append(reason)

        # 心境影响（更持久、更温和）
        if mood is not None:
            # 负向心境 → 时间变慢；正向心境 → 时间稍快
            mood_factor = 1.0 + (mood / 100.0) * 0.3
            mood_factor = max(0.7, min(1.3, mood_factor))
            factor *= mood_factor
            if mood < -30:
                reasons.append('心境低落-时间沉重')
            elif mood > 30:
                reasons.append('心境愉悦-时间轻快')

        # 唤醒度影响
        if arousal is not None:
            # 高唤醒 → 时间感知更精细（主观变慢）；低唤醒 → 时间模糊（主观变快）
            if arousal > 70:
                arousal_factor = 0.7 + (100 - arousal) / 100 * 0.3
                reasons.append('高唤醒-时间精细')
            elif arousal < 30:
                arousal_factor = 1.2 + (30 - arousal) / 30 * 0.3
                reasons.append('低唤醒-时间模糊')
            else:
                arousal_factor = 1.0
            factor *= arousal_factor

        # 活动水平/投入度影响
        if activity_level > 0.7:
            # 高度投入 → 心流状态 → 时间飞逝
            flow_factor = 1.5 + (activity_level - 0.7) / 0.3 * 1.5
            factor *= flow_factor
            reasons.append('沉浸投入-心流飞逝')
        elif activity_level < 0.2:
            # 低活动 → 无聊 → 时间变慢
            boredom_factor = 0.6 + activity_level / 0.2 * 0.2
            factor *= boredom_factor
            reasons.append('低活动度-时间缓慢')

        # 限制范围
        factor = max(0.1, min(3.0, factor))

        # 更新主观时间
        self._start_subjective_time += delta_real * (factor - 1.0)
        self._distortion_factor = factor

        self._distortion_history.append({
            'time': now,
            'factor': factor,
            'reason': ', '.join(reasons) if reasons else 'normal'
        })

        return factor

    def get_time_since(self, timestamp_real):
        """计算从某个真实时间戳到现在经过的主观时间（秒）"""
        elapsed_real = time.time() - timestamp_real
        # 简化：用当前扭曲系数估算（实际应该积分，这里简化处理）
        return elapsed_real * self._distortion_factor

    def format_duration(self, seconds, subjective=False):
        """
        将秒数格式化为自然语言描述
        subjective=True 时使用更主观的表达
        """
        if seconds < 60:
            if subjective:
                if seconds < 10:
                    return "一瞬间"
                elif seconds < 30:
                    return "一小会儿"
                else:
                    return "几分钟的样子"
            return f"{int(seconds)}秒"
        elif seconds < 3600:
            mins = int(seconds / 60)
            if subjective:
                if mins < 5:
                    return "没几分钟"
                elif mins < 20:
                    return "一刻钟左右"
                elif mins < 45:
                    return "半个多小时"
                else:
                    return "快一个小时了"
            return f"{mins}分钟"
        elif seconds < 86400:
            hours = seconds / 3600
            if subjective:
                if hours < 1.5:
                    return "一个多小时"
                elif hours < 3:
                    return "两三个小时"
                elif hours < 6:
                    return "半天功夫"
                else:
                    return "大半天"
            return f"{hours:.1f}小时"
        else:
            days = seconds / 86400
            if subjective:
                if days < 1.5:
                    return "一天左右"
                elif days < 3:
                    return "两三天"
                elif days < 7:
                    return "一个星期左右"
                elif days < 14:
                    return "半个月"
                else:
                    return "好久了"
            return f"{days:.1f}天"


# ============================================================
# L2: 主观扭曲层 — TemporalDistortion
# 多维度时间扭曲调制 · 场景化扭曲模式
# ============================================================

class TemporalDistortion:
    """
    主观扭曲层：精细化的时间感知扭曲调制

    扭曲维度：
      - 注意门控模型：注意资源分配影响时间感知
      - 唤醒水平：生理唤醒度影响内部时钟速率
      - 情绪效价：正负情绪的差异化影响
      - 认知负荷：高认知负荷下时间感知压缩
      - 身体状态：疲劳、疼痛、饥饿等影响
    """

    def __init__(self, dm=None):
        self.dm = dm
        # 各维度扭曲系数（独立维护，最终合成）
        self._attention_factor = 1.0      # 注意维度
        self._arousal_factor = 1.0        # 唤醒维度
        self._emotion_factor = 1.0        # 情绪维度
        self._cognitive_factor = 1.0      # 认知维度
        self._somatic_factor = 1.0        # 躯体维度

        self._distortion_log = deque(maxlen=50)

    def update_attention(self, attention_level, focus_target=None):
        """
        更新注意水平对时间感知的影响
        attention_level: 0~1，越高表示越专注
        focus_target: 专注对象（用于后续记忆关联）
        """
        # 高专注 → 时间飞逝（注意资源被占用，无法感知时间流逝）
        if attention_level > 0.8:
            self._attention_factor = 0.4 + (1.0 - attention_level) * 2.0  # 0.4 ~ 0.8
        elif attention_level < 0.3:
            self._attention_factor = 1.2 + (0.3 - attention_level) / 0.3 * 0.5  # 1.2 ~ 1.7
        else:
            self._attention_factor = 1.0
        return self._attention_factor

    def update_arousal(self, arousal_level):
        """
        更新唤醒水平对时间感知的影响
        arousal_level: 0~100
        """
        # 高唤醒 → 内部时钟加速 → 主观时间变慢（感知更精细）
        if arousal_level > 70:
            self._arousal_factor = 0.6 + (100 - arousal_level) / 30 * 0.3
        elif arousal_level < 30:
            self._arousal_factor = 1.1 + (30 - arousal_level) / 30 * 0.4
        else:
            self._arousal_factor = 1.0
        return self._arousal_factor

    def update_emotion(self, emotion, intensity):
        """
        更新情绪对时间感知的影响
        emotion: 情绪类型
        intensity: 强度 0~100
        """
        emotion_time_map = {
            'sadness': 0.6,     # 悲伤：时间变慢
            'depression': 0.4,  # 抑郁：时间极慢
            'fear': 0.5,        # 恐惧：时间变慢（战斗或逃跑）
            'anxiety': 0.55,    # 焦虑：时间煎熬
            'anger': 1.2,       # 愤怒：时间加速
            'joy': 1.5,         # 快乐：时间飞逝
            'mania': 2.0,       # 躁狂：时间极快
            'boredom': 0.7,     # 无聊：时间缓慢
            'contentment': 1.1, # 满足：时间稍快
            'surprise': 0.8,    # 惊讶：时间变慢（注意力被捕获）
        }
        base = emotion_time_map.get(emotion, 1.0)
        # 强度调制：强度越高，扭曲越明显
        intensity_mod = 0.5 + (intensity / 100.0) * 0.5
        if base < 1.0:
            self._emotion_factor = 1.0 - (1.0 - base) * intensity_mod
        else:
            self._emotion_factor = 1.0 + (base - 1.0) * intensity_mod
        return self._emotion_factor

    def update_cognitive_load(self, load_level):
        """
        更新认知负荷对时间感知的影响
        load_level: 0~1
        """
        # 高认知负荷 → 时间感知压缩（大脑忙于处理信息，忽略时间）
        if load_level > 0.7:
            self._cognitive_factor = 0.5 + (1.0 - load_level) / 0.3 * 0.5
        elif load_level < 0.2:
            self._cognitive_factor = 1.3 + (0.2 - load_level) / 0.2 * 0.4
        else:
            self._cognitive_factor = 1.0
        return self._cognitive_factor

    def update_somatic_state(self, fatigue=0, pain=0, hunger=0, drowsiness=0):
        """
        更新躯体状态对时间感知的影响
        各参数：0~100
        """
        factor = 1.0
        # 疲劳：时间变慢
        if fatigue > 50:
            factor *= 0.8 + (100 - fatigue) / 50 * 0.2
        # 疼痛：时间显著变慢
        if pain > 30:
            factor *= 0.5 + (100 - pain) / 70 * 0.5
        # 饥饿：时间稍慢
        if hunger > 60:
            factor *= 0.85 + (100 - hunger) / 40 * 0.15
        # 困倦：时间模糊变快
        if drowsiness > 50:
            factor *= 1.2 + (drowsiness - 50) / 50 * 0.5

        self._somatic_factor = factor
        return factor

    def get_combined_factor(self):
        """合成所有维度的扭曲系数"""
        # 使用几何平均，避免极端值
        factors = [
            self._attention_factor,
            self._arousal_factor,
            self._emotion_factor,
            self._cognitive_factor,
            self._somatic_factor
        ]
        product = 1.0
        for f in factors:
            product *= f
        combined = product ** (1.0 / len(factors))
        combined = max(0.1, min(3.0, combined))

        self._distortion_log.append({
            'time': time.time(),
            'combined': combined,
            'attention': self._attention_factor,
            'arousal': self._arousal_factor,
            'emotion': self._emotion_factor,
            'cognitive': self._cognitive_factor,
            'somatic': self._somatic_factor,
        })
        return combined

    def get_distortion_profile(self):
        """获取当前各维度的扭曲详情"""
        return {
            'attention': self._attention_factor,
            'arousal': self._arousal_factor,
            'emotion': self._emotion_factor,
            'cognitive': self._cognitive_factor,
            'somatic': self._somatic_factor,
            'combined': self.get_combined_factor()
        }

    def apply_scenario(self, scenario_name):
        """应用预设的场景化扭曲模式"""
        scenarios = {
            'reading': {  # 看书：沉浸状态
                'attention': 0.5,
                'cognitive': 0.6,
                'emotion': 1.1,
            },
            'social_anxiety': {  # 社交焦虑：时间煎熬
                'attention': 1.3,
                'arousal': 0.6,
                'emotion': 0.55,
                'somatic': 0.8,
            },
            'depressive_episode': {  # 抑郁发作：度日如年
                'emotion': 0.4,
                'arousal': 1.2,
                'cognitive': 1.3,
                'somatic': 0.8,
            },
            'hypomanic_episode': {  # 轻躁狂：时间飞逝
                'emotion': 1.8,
                'arousal': 0.9,
                'attention': 0.7,
                'cognitive': 0.8,
            },
            'sleeping': {  # 睡眠：时间感丧失
                'attention': 2.0,
                'arousal': 1.5,
                'cognitive': 2.0,
                'emotion': 1.0,
                'somatic': 1.5,
            },
            'panic_attack': {  # 惊恐发作：时间极度变慢
                'emotion': 0.3,
                'arousal': 0.4,
                'attention': 1.5,
                'somatic': 0.5,
            },
        }
        if scenario_name in scenarios:
            sc = scenarios[scenario_name]
            if 'attention' in sc:
                self._attention_factor = sc['attention']
            if 'arousal' in sc:
                self._arousal_factor = sc['arousal']
            if 'emotion' in sc:
                self._emotion_factor = sc['emotion']
            if 'cognitive' in sc:
                self._cognitive_factor = sc['cognitive']
            if 'somatic' in sc:
                self._somatic_factor = sc['somatic']
            return True
        return False


# ============================================================
# L3: 生物节律层 — CircadianRhythm
# 昼夜节律 · 睡眠-觉醒周期 · 警觉度波动
# ============================================================

class CircadianRhythm:
    """
    生物节律层：模拟昼夜节律与睡眠-觉醒周期

    结合里克的特点：
      - 作息极度紊乱，睡眠时间不固定
      - 常昼夜颠倒
      - 最长连续清醒可达36小时
      - 曾连续昏睡18小时
      - 与双相情感障碍发作周期相关
    """

    def __init__(self, dm=None):
        self.dm = dm
        # 当前状态
        self._wake_time = None       # 醒来的时间戳
        self._sleep_time = None      # 入睡的时间戳
        self._is_awake = True
        self._sleep_quality = 0.5    # 睡眠质量 0~1

        # 节律参数（基于里克的紊乱作息）
        self._circadian_phase = 0.0  # 昼夜节律相位偏移（小时）
        self._cycle_regularity = 0.2 # 作息规律性 0~1（里克很低）

        # 警觉度曲线参数
        self._alertness = 70.0       # 当前警觉度 0~100
        self._sleep_pressure = 0.0   # 睡眠压力 0~100

        # 历史记录
        self._sleep_log = deque(maxlen=30)  # 最近30次睡眠记录
        self._alertness_log = deque(maxlen=100)

        # 初始化：假设当前是清醒状态
        self._wake_time = time.time() - 3600 * random.uniform(2, 8)  # 已醒2-8小时

    def tick(self, dt_seconds=60, mood=None, emotion=None, mania_level=0, depression_level=0):
        """
        推进生物节律
        dt_seconds: 经过的真实秒数
        """
        hours = dt_seconds / 3600.0

        # 更新睡眠压力：清醒时积累，睡眠时减少
        if self._is_awake:
            # 清醒时间越长，睡眠压力越大
            awake_hours = (time.time() - self._wake_time) / 3600 if self._wake_time else 0
            base_pressure_rate = 3.0  # 每小时增加3%
            # 躁狂状态下睡眠压力增长缓慢
            if mania_level > 50:
                base_pressure_rate *= 0.3
            # 抑郁状态下睡眠压力增长较快
            if depression_level > 50:
                base_pressure_rate *= 1.5

            self._sleep_pressure += base_pressure_rate * hours
            self._sleep_pressure = min(100.0, self._sleep_pressure)

            # 警觉度：基于清醒时长 + 昼夜节律 + 情绪
            # 昼夜节律影响（正弦波模拟，周期约24小时）
            now = datetime.now()
            circadian_effect = math.sin((now.hour + now.minute / 60 - 6) / 24 * 2 * math.pi) * 20
            # 清醒时长影响
            awake_effect = max(0, 100 - awake_hours * 3)
            # 合成警觉度
            self._alertness = (awake_effect * 0.6 + (50 + circadian_effect) * 0.4)

            # 情绪/状态调整
            if mania_level > 50:
                self._alertness = min(100, self._alertness + mania_level * 0.3)
            if depression_level > 50:
                self._alertness = max(0, self._alertness - depression_level * 0.3)

        else:
            # 睡眠中：睡眠压力减少
            self._sleep_pressure -= 8.0 * hours * self._sleep_quality
            self._sleep_pressure = max(0.0, self._sleep_pressure)

            # 警觉度很低
            self._alertness = 5.0 + random.uniform(-3, 3)

        self._alertness = max(0, min(100, self._alertness))

        self._alertness_log.append({
            'time': time.time(),
            'alertness': self._alertness,
            'sleep_pressure': self._sleep_pressure,
            'is_awake': self._is_awake
        })

        return self._alertness

    def try_sleep(self, reason="自然入睡", force_quality=None):
        """尝试入睡"""
        if not self._is_awake:
            return False, "已经在睡眠中"

        # 入睡概率：基于睡眠压力 + 当前时间 + 情绪状态
        sleep_prob = self._sleep_pressure / 100.0 * 0.7

        # 夜间更容易入睡
        now_hour = datetime.now().hour
        if 22 <= now_hour or now_hour <= 6:
            sleep_prob += 0.2
        elif 10 <= now_hour <= 18:
            sleep_prob -= 0.2

        # 躁狂时难以入睡
        # （这里简化处理，实际应该从情绪系统获取）

        sleep_prob = max(0.05, min(0.95, sleep_prob))

        if random.random() < sleep_prob:
            # 成功入睡
            self._is_awake = False
            self._sleep_time = time.time()
            self._sleep_quality = force_quality if force_quality is not None else random.uniform(0.3, 0.8)
            return True, f"入睡成功，睡眠质量预期：{self._sleep_quality:.2f}"
        else:
            return False, "睡不着，辗转反侧"

    def wake_up(self, reason="自然醒来"):
        """醒来"""
        if self._is_awake:
            return False, "已经醒着"

        sleep_duration = time.time() - self._sleep_time if self._sleep_time else 0
        self._is_awake = True
        self._wake_time = time.time()

        # 记录睡眠
        self._sleep_log.append({
            'sleep_time': self._sleep_time,
            'wake_time': self._wake_time,
            'duration': sleep_duration,
            'quality': self._sleep_quality,
            'reason': reason
        })

        # 醒来后重置部分睡眠压力
        self._sleep_pressure = max(0, self._sleep_pressure - 60 * self._sleep_quality)

        return True, f"醒来了，睡了 {sleep_duration/3600:.1f} 小时"

    def get_alertness(self):
        """获取当前警觉度"""
        return self._alertness

    def get_sleep_pressure(self):
        """获取睡眠压力"""
        return self._sleep_pressure

    def get_awake_duration(self):
        """获取已清醒时长（秒）"""
        if self._is_awake and self._wake_time:
            return time.time() - self._wake_time
        return 0

    def get_sleep_duration(self):
        """获取已睡眠时长（秒）"""
        if not self._is_awake and self._sleep_time:
            return time.time() - self._sleep_time
        return 0

    def is_awake(self):
        return self._is_awake

    def get_state_description(self):
        """获取当前状态的自然语言描述"""
        if not self._is_awake:
            duration = self.get_sleep_duration() / 3600
            return f"睡眠中（已睡 {duration:.1f} 小时，质量 {self._sleep_quality:.0%}）"

        awake_hours = self.get_awake_duration() / 3600
        alertness = self._alertness

        if alertness > 80:
            alert_desc = "精神饱满"
        elif alertness > 60:
            alert_desc = "状态还行"
        elif alertness > 40:
            alert_desc = "有点疲惫"
        elif alertness > 20:
            alert_desc = "非常困倦"
        else:
            alert_desc = "濒临极限"

        if self._sleep_pressure > 80:
            pressure_desc = "睡意强烈"
        elif self._sleep_pressure > 50:
            pressure_desc = "有点困了"
        else:
            pressure_desc = "精神尚可"

        return f"已清醒 {awake_hours:.1f} 小时，{alert_desc}，{pressure_desc}"

    def get_sleep_debt(self):
        """计算睡眠债（最近7天的睡眠不足程度）"""
        if len(self._sleep_log) == 0:
            return 0

        total_sleep = 0
        for s in list(self._sleep_log)[-7:]:
            total_sleep += s['duration']

        # 理想睡眠：每天7小时
        ideal_sleep = 7 * 3600 * min(7, len(self._sleep_log))
        debt = ideal_sleep - total_sleep
        return max(0, debt)


# ============================================================
# L4: 作息模式层 — RoutinePattern
# 日常作息模式识别 · 活动规律 · 排班日/非排班日
# ============================================================

class RoutinePattern:
    """
    作息模式层：模拟和追踪日常活动模式

    里克的特点：
      - 作息极度紊乱，昼夜颠倒
      - 排班日是唯一能勉强维持规律的时间点
      - 非排班日几乎不出房门
      - 排班日会提前一天强迫自己调整作息
    """

    def __init__(self, dm=None):
        self.dm = dm
        # 活动记录
        self._activity_log = deque(maxlen=200)  # 活动记录
        self._daily_patterns = {}  # 每日模式缓存

        # 排班日设置（模拟：每周二、四、六是排班日）
        self._work_days = [1, 3, 5]  # 周一=0, 周二=1, ..., 周日=6

        # 当前状态
        self._current_activity = 'idle'  # 当前活动
        self._activity_start_time = time.time()

        # 日常活动库
        self._activities = {
            'sleeping': {'energy': -20, 'social': 0, 'time_distortion': 0.2},
            'reading': {'energy': -5, 'social': 0, 'time_distortion': 0.5, 'enjoyment': 0.8},
            'phone_scrolling': {'energy': -2, 'social': 0.1, 'time_distortion': 1.5, 'enjoyment': 0.3},
            'eating': {'energy': 10, 'social': 0, 'time_distortion': 1.0, 'enjoyment': 0.5},
            'snacking': {'energy': 5, 'social': 0, 'time_distortion': 1.0, 'enjoyment': 0.6, 'guilt': 0.3},
            'working': {'energy': -15, 'social': 0.5, 'time_distortion': 0.8, 'enjoyment': 0.4},
            'going_out': {'energy': -25, 'social': 0.8, 'time_distortion': 0.7, 'enjoyment': -0.5, 'anxiety': 0.8},
            'talking_with_xiaota': {'energy': 5, 'social': 0.9, 'time_distortion': 1.8, 'enjoyment': 0.95},
            'self_blame': {'energy': -10, 'social': 0, 'time_distortion': 0.4, 'enjoyment': -0.8},
            'medication': {'energy': 0, 'social': 0, 'time_distortion': 1.0},
        }

    def is_work_day(self, date=None):
        """判断某天是否是排班日"""
        if date is None:
            date = datetime.now()
        return date.weekday() in self._work_days

    def get_day_type(self):
        """获取今天的类型描述"""
        if self.is_work_day():
            return "排班日"
        else:
            return "休息日"

    def start_activity(self, activity_name):
        """开始一项活动"""
        if activity_name not in self._activities:
            return False

        # 结束当前活动
        if self._current_activity != 'idle':
            self._end_activity()

        self._current_activity = activity_name
        self._activity_start_time = time.time()

        self._activity_log.append({
            'time': time.time(),
            'activity': activity_name,
            'action': 'start'
        })

        return True

    def _end_activity(self):
        """结束当前活动"""
        duration = time.time() - self._activity_start_time
        self._activity_log.append({
            'time': time.time(),
            'activity': self._current_activity,
            'action': 'end',
            'duration': duration
        })
        self._current_activity = 'idle'
        self._activity_start_time = time.time()

    def get_current_activity(self):
        """获取当前活动"""
        return self._current_activity

    def get_activity_duration(self):
        """获取当前活动已持续的时间（秒）"""
        if self._current_activity == 'idle':
            return 0
        return time.time() - self._activity_start_time

    def get_activity_info(self, activity_name):
        """获取活动的属性信息"""
        return self._activities.get(activity_name, {})

    def get_today_summary(self):
        """获取今日活动摘要"""
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        activities_today = []
        current_activity = None
        current_start = None

        for log in self._activity_log:
            if log['time'] < today_start:
                continue
            if log['action'] == 'start':
                current_activity = log['activity']
                current_start = log['time']
            elif log['action'] == 'end' and current_activity:
                activities_today.append({
                    'activity': current_activity,
                    'duration': log.get('duration', log['time'] - current_start)
                })
                current_activity = None

        # 如果当前还有进行中的活动
        if current_activity and current_start:
            activities_today.append({
                'activity': current_activity,
                'duration': time.time() - current_start,
                'ongoing': True
            })

        # 统计
        total_by_activity = {}
        for act in activities_today:
            name = act['activity']
            if name not in total_by_activity:
                total_by_activity[name] = 0
            total_by_activity[name] += act['duration']

        return {
            'day_type': self.get_day_type(),
            'activities': total_by_activity,
            'current': self._current_activity,
            'count': len(activities_today)
        }

    def suggest_activity(self, mood=0, energy=50, social_anxiety=50):
        """根据状态建议活动"""
        candidates = []

        for name, info in self._activities.items():
            score = 0
            # 能量匹配
            if energy < 30 and info['energy'] < 0:
                score -= 20  # 累了不想做消耗精力的事
            elif energy > 70 and info['energy'] > 0:
                score -= 10  # 精力充沛不需要恢复

            # 情绪匹配
            if mood < -30 and info.get('enjoyment', 0) > 0:
                score += 15  # 情绪低落时倾向做喜欢的事
            elif mood > 30 and info.get('enjoyment', 0) > 0.5:
                score += 10

            # 社交焦虑匹配
            if social_anxiety > 60 and info.get('social', 0) > 0.3:
                score -= 30  # 社恐时回避社交活动

            # 随机波动
            score += random.uniform(-10, 10)

            candidates.append((name, score))

        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[:5]

    def get_routine_regularity(self):
        """计算作息规律性评分 0~100"""
        # 基于排班日的遵守程度、睡眠时间的方差等
        # 简化：里克的规律性很低
        if self.is_work_day():
            return random.uniform(40, 60)  # 排班日稍微规律
        else:
            return random.uniform(10, 30)  # 非排班日极度紊乱


# ============================================================
# L5: 时间记忆层 — TemporalMemory
# 记忆的时间编码 · 回溯性时间判断 · 时间距离感 · 混乱的时间记忆
# ============================================================

class TemporalMemory:
    """
    时间记忆层：记忆的时间编码与时间感知（里克专属：混乱且模糊）

    里克的时间记忆特点：
      - 作息昼夜颠倒 → 日期概念模糊，搞不清今天星期几、几号
      - 长期待在室内 → 白天黑夜界限不清，日子像融化了一样
      - 双相情感障碍 → 抑郁期觉得度日如年，躁狂期不知时日过
      - 社恐+回避 → 社交事件的时间记忆混乱（因为不想回忆）
      - 长期服药 → 部分记忆的时间感更加模糊
      - 来源遗忘 → 记得一件事，但完全不记得是什么时候发生的

    核心机制：
      - 时间戳漂移：主观记忆的时间戳会随时间逐渐偏移
      - 望远镜效应：近期事件觉得久远，远期事件反而觉得近
      - 状态依赖：不同心境状态下的记忆，时间感完全脱节
      - 时间混叠：连续几天做同样的事，记忆会糊成一团
      - 闪光灯记忆：只有极少数强烈情绪事件的时间是清晰的
    """

    def __init__(self, dm=None, memory_gateway=None):
        self.dm = dm
        self.memory_gateway = memory_gateway
        self._time_tags = {}  # 记忆ID → 时间标签
        self._temporal_confusion_level = 40  # 整体时间混乱度（里克：40/100）
        self._day_blur_factor = 0.6  # 日子模糊系数（越高越分不清每天的区别）

    def set_confusion_level(self, level):
        """设置时间记忆混乱度（0~100）"""
        self._temporal_confusion_level = max(0, min(100, level))

    def encode_memory_time(self, memory_id, real_timestamp=None, subjective_time=None,
                           context=None, encoding_state='neutral', emotional_intensity=0):
        """
        为记忆编码时间标签

        参数：
          - memory_id: 记忆ID
          - real_timestamp: 客观时间戳
          - subjective_time: 主观时间感知（扭曲后的时间）
          - context: 时间上下文（季节、天气、时间段等）
          - encoding_state: 编码时的状态（depressive/manic/anxious/neutral/sleepy）
          - emotional_intensity: 编码时的情绪强度 0~100
        """
        if real_timestamp is None:
            real_timestamp = time.time()

        dt = datetime.fromtimestamp(real_timestamp)

        # 编码时就已经可能带偏差了——状态越糟，编码越不准确
        encoding_bias = 0
        if encoding_state == 'depressive':
            encoding_bias = random.uniform(-0.1, 0.3)  # 抑郁时倾向于觉得"已经过了很久"
        elif encoding_state == 'manic':
            encoding_bias = random.uniform(-0.3, 0.1)  # 躁狂时觉得"好像就是刚才"
        elif encoding_state == 'sleepy':
            encoding_bias = random.uniform(-0.5, 0.5)  # 困倦时时间感完全混乱
        elif encoding_state == 'anxious':
            encoding_bias = random.uniform(-0.2, 0.4)  # 焦虑时也混乱

        # 闪光灯记忆：编码更准确
        is_flashbulb = emotional_intensity > 80
        if is_flashbulb:
            encoding_bias *= 0.1  # 强烈情绪下编码更准确

        time_tag = {
            'memory_id': memory_id,
            'real_timestamp': real_timestamp,
            'subjective_timestamp': subjective_time or real_timestamp,
            'encoded_bias': encoding_bias,  # 编码时的初始偏差
            'encoding_state': encoding_state,  # 编码时的状态
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'hour': dt.hour,
            'weekday': dt.weekday(),
            'time_of_day': self._classify_time_of_day(dt.hour),
            'season': self._classify_season(dt.month),
            'context': context or {},
            'clarity': 100.0,  # 记忆清晰度（随时间衰减）
            'flashbulb': is_flashbulb,  # 是否闪光灯记忆
            'emotional_intensity': emotional_intensity,  # 编码时的情绪强度
            'temporal_drift': 0.0,  # 时间漂移量（随时间积累）
            'source_forgotten': False,  # 是否来源遗忘（不记得什么时候了）
            'telescoped': False,  # 是否发生了望远镜效应
        }

        self._time_tags[memory_id] = time_tag
        return time_tag

    def mark_flashbulb(self, memory_id, emotional_intensity=90):
        """标记为闪光灯记忆（强烈情绪事件）"""
        if memory_id in self._time_tags:
            self._time_tags[memory_id]['flashbulb'] = True
            self._time_tags[memory_id]['emotional_intensity'] = emotional_intensity
            self._time_tags[memory_id]['encoded_bias'] *= 0.05  # 几乎无偏差
            return True
        return False

    def _classify_time_of_day(self, hour):
        """将小时分类为时间段"""
        if 5 <= hour < 9:
            return '清晨'
        elif 9 <= hour < 12:
            return '上午'
        elif 12 <= hour < 14:
            return '中午'
        elif 14 <= hour < 18:
            return '下午'
        elif 18 <= hour < 22:
            return '晚上'
        else:
            return '深夜'

    def _classify_season(self, month):
        """季节分类（北半球）"""
        if 3 <= month <= 5:
            return '春天'
        elif 6 <= month <= 8:
            return '夏天'
        elif 9 <= month <= 11:
            return '秋天'
        else:
            return '冬天'

    def _calc_temporal_drift(self, tag):
        """
        计算时间漂移量——记忆的主观时间戳会随时间"漂移"

        里克特点：漂移幅度大，方向不定
        """
        elapsed = time.time() - tag['real_timestamp']
        days_elapsed = elapsed / 86400

        if tag['flashbulb']:
            # 闪光灯记忆：漂移极小
            drift_rate = 0.002
        else:
            # 基础漂移率：每天约 2% 的漂移
            drift_rate = 0.02
            # 混乱度加成
            drift_rate *= (1 + self._temporal_confusion_level / 100.0)
            # 编码状态影响
            if tag['encoding_state'] in ['sleepy', 'depressive']:
                drift_rate *= 1.5
            elif tag['encoding_state'] == 'manic':
                drift_rate *= 1.3

        # 漂移方向：随机，但有轻微的"远期拉近"倾向（望远镜效应）
        drift_direction = random.choice([-1, 1])
        if days_elapsed > 7 and random.random() < 0.4:
            # 40%概率发生望远镜效应：把远期记忆拉近
            drift_direction = -1
            tag['telescoped'] = True

        drift_amount = elapsed * drift_rate * drift_direction * random.uniform(0.5, 1.5)

        # 来源遗忘：当清晰度低于阈值时，有概率完全不记得时间了
        if not tag['flashbulb'] and days_elapsed > 3:
            source_forget_prob = (days_elapsed / 30) * (self._temporal_confusion_level / 100)
            if random.random() < min(0.6, source_forget_prob):
                tag['source_forgotten'] = True

        tag['temporal_drift'] = drift_amount
        return drift_amount

    def get_memory_time(self, memory_id):
        """获取记忆的时间标签（含漂移计算）"""
        tag = self._time_tags.get(memory_id)
        if not tag:
            return None

        elapsed = time.time() - tag['real_timestamp']

        # 计算时间漂移
        drift = self._calc_temporal_drift(tag)

        # 计算记忆清晰度衰减
        if tag['flashbulb']:
            decay_rate = 0.001  # 每天衰减0.1%
        else:
            decay_rate = 0.015  # 每天衰减1.5%（里克记忆更模糊）
            # 混乱度加速衰减
            decay_rate *= (1 + self._temporal_confusion_level / 200.0)

        days_elapsed = elapsed / 86400
        clarity = max(5, 100 - days_elapsed * decay_rate * 100)

        # 来源遗忘时清晰度直接降到很低
        if tag['source_forgotten']:
            clarity = min(clarity, 15)

        tag['current_clarity'] = clarity
        tag['elapsed_seconds'] = elapsed
        tag['subjective_elapsed'] = elapsed + drift + tag['encoded_bias'] * elapsed

        return tag

    def estimate_time_since(self, memory_id, current_mood=0, current_emotion=None,
                            current_state='neutral'):
        """
        主观估计"那件事过了多久"

        里克的特点：经常猜错、含糊其辞、甚至完全不记得

        影响因素：
          - 时间漂移：记忆本身的时间戳已经偏移了
          - 当前状态与编码状态是否一致：一致则更准确，不一致则更混乱
          - 清晰度：越模糊越说不准
          - 望远镜效应：近期的事觉得远，远期的事觉得近
          - 来源遗忘：完全说不上来是什么时候
        """
        tag = self.get_memory_time(memory_id)
        if not tag:
            return None, "不知道……我不记得了。"

        real_elapsed = tag['elapsed_seconds']
        subjective_elapsed = tag['subjective_elapsed']
        clarity = tag['current_clarity']

        # 状态一致性效应：当前状态和编码状态不一致 → 时间判断更差
        state_match = (current_state == tag['encoding_state'])
        if not state_match and tag['encoding_state'] != 'neutral':
            # 状态不匹配 → 额外增加混乱
            state_confusion = random.uniform(0.5, 2.0)
            subjective_elapsed *= state_confusion
            clarity *= 0.7  # 清晰度也下降

        # 当前心境影响
        if current_mood < -30:
            # 抑郁状态：时间感拉长，觉得更久
            subjective_elapsed *= random.uniform(1.2, 1.6)
        elif current_mood > 30:
            # 愉悦/轻躁狂状态：时间感压缩，觉得更近
            subjective_elapsed *= random.uniform(0.7, 0.9)

        # 来源遗忘：完全没概念
        if tag['source_forgotten']:
            responses = [
                "我……完全不记得是什么时候的事了。",
                "什么时候来着？……我想不起来了。",
                "好像是很久以前？不对……也可能是最近？我搞不清楚。",
                "对不起……我对时间没什么概念。",
                "（沉默了一会儿）……不记得了。",
            ]
            return {
                'real_elapsed': real_elapsed,
                'subjective_elapsed': subjective_elapsed,
                'clarity': clarity,
                'flashbulb': tag['flashbulb'],
                'source_forgotten': True,
                'description': random.choice(responses)
            }, random.choice(responses)

        # 限制主观时间不能为负
        subjective_elapsed = max(60, subjective_elapsed)

        # 生成自然语言描述
        description = self._format_time_ago_rick(subjective_elapsed, clarity, tag)

        return {
            'real_elapsed': real_elapsed,
            'subjective_elapsed': subjective_elapsed,
            'clarity': clarity,
            'flashbulb': tag['flashbulb'],
            'source_forgotten': False,
            'telescoped': tag['telescoped'],
            'encoding_state': tag['encoding_state'],
            'description': description
        }, description

    def _format_time_ago_rick(self, seconds, clarity, tag=None):
        """
        里克风格的"多久以前"描述

        特点：含糊、不确定、经常自我怀疑、用"好像""大概""可能"
        """
        # 极低清晰度：完全混乱
        if clarity < 15:
            responses = [
                "……我完全记不清了。",
                "好久了吧……还是说其实没多久？",
                "我对时间没概念。",
                "对不起……想不起来。",
            ]
            return random.choice(responses)

        # 低清晰度：很模糊，可能搞反
        if clarity < 35:
            if seconds < 86400:
                # 几小时的事，她可能觉得是好几天前
                responses = [
                    "好像是昨天？不对……今天吗？我搞混了。",
                    "大概……是这几天的事？",
                    "我不记得了，就最近吧。",
                ]
            elif seconds < 86400 * 7:
                responses = [
                    "一个星期左右？还是更久？我分不清。",
                    "好像是前几天的事……也可能是上个星期。",
                    "我记不太清了，反正不是今天。",
                ]
            elif seconds < 86400 * 30:
                responses = [
                    "好几个星期了吧……也可能更久。",
                    "上个月？还是上上个月？",
                    "我对日子没什么概念。",
                ]
            else:
                responses = [
                    "很久了……久到我都记不清了。",
                    "好几个月？还是一年多？我真的不知道。",
                    "（摇头）……不记得了。",
                ]
            return random.choice(responses)

        # 中等清晰度：能说出大概范围，但不确定
        if clarity < 65:
            if seconds < 3600:
                responses = [
                    "好像是几十分钟前？",
                    "大概一个小时不到吧……我不确定。",
                    "没记错的话，就刚才没多久。",
                ]
            elif seconds < 86400:
                hours = int(seconds / 3600)
                responses = [
                    f"大概{hours}个小时前？……我也说不准。",
                    f"{hours}个小时左右？可能吧。",
                    "今天发生的事……应该是吧。",
                ]
            elif seconds < 86400 * 3:
                responses = [
                    "前两天的事？还是昨天？",
                    "两三天前吧大概。",
                    "我记得不是很清楚了，就这几天。",
                ]
            elif seconds < 86400 * 7:
                responses = [
                    "一个星期左右吧……",
                    "大概几天前？我对日子没概念。",
                    "好像是上周的事？不对……也可能是这周。",
                ]
            elif seconds < 86400 * 30:
                responses = [
                    "大概几个星期前？",
                    "上个月的事？我记不清了。",
                    "有段时间了，具体多久想不起来。",
                ]
            elif seconds < 86400 * 180:
                responses = [
                    "好几个月前了吧……",
                    "几个月前？还是半年多了？",
                    "挺久以前的事了。",
                ]
            else:
                responses = [
                    "好几年前的事了……大概吧。",
                    "很久以前了，具体什么时候不记得了。",
                    "（想了很久）……我真的记不清了。",
                ]
            return random.choice(responses)

        # 较高清晰度：比较确定，但还是带点犹豫（里克的风格）
        if seconds < 300:
            return "就刚才。"
        elif seconds < 3600:
            mins = int(seconds / 60)
            return f"{mins}分钟前吧。"
        elif seconds < 86400:
            hours = int(seconds / 3600)
            return f"{hours}个小时前……应该是。"
        elif seconds < 86400 * 3:
            days = int(seconds / 86400)
            return f"{days}天前。"
        elif seconds < 86400 * 7:
            days = int(seconds / 86400)
            return f"大概{days}天前。"
        elif seconds < 86400 * 30:
            weeks = int(seconds / 86400 / 7)
            return f"{weeks}个星期前吧。"
        elif seconds < 86400 * 365:
            months = int(seconds / 86400 / 30)
            return f"{months}个月前。"
        else:
            years = int(seconds / 86400 / 365)
            return f"{years}年前。"

    def get_subjective_date(self):
        """
        获取里克"觉得"今天是什么日期

        基于她的作息混乱程度，可能会搞错日期、星期
        """
        now = datetime.now()
        confusion = self._temporal_confusion_level / 100.0

        # 日期偏差天数（最多可能偏差2-3天）
        day_offset = 0
        if random.random() < confusion * 0.3:
            day_offset = random.choice([-2, -1, 1, 2])

        # 星期几也可能搞错
        weekday_correct = random.random() > confusion * 0.4

        subjective_dt = now + timedelta(days=day_offset)

        return {
            'real_date': now.strftime("%Y-%m-%d"),
            'real_weekday': now.weekday(),
            'subjective_date': subjective_dt.strftime("%Y-%m-%d"),
            'subjective_weekday': subjective_dt.weekday() if weekday_correct else random.randint(0, 6),
            'day_offset': day_offset,
            'weekday_correct': weekday_correct,
            'is_confused': abs(day_offset) > 0 or not weekday_correct,
        }

    def describe_today(self):
        """描述"今天"的感受（可能搞错）"""
        info = self.get_subjective_date()
        now = datetime.now()

        if not info['is_confused']:
            return f"今天是{now.month}月{now.day}日，星期{['一','二','三','四','五','六','日'][now.weekday()]}。"
        else:
            # 混乱时的表达
            responses = [
                "今天……几号来着？我不太清楚。",
                "星期几？……我忘了。反正不是排班日就是了。",
                "（抬头看了一眼窗外）……好像是白天？我也不知道。",
                "今天吗？……我搞不清。每天都差不多。",
                "大概是这个星期吧……我也说不准。",
            ]
            return random.choice(responses)

    def search_memories_by_time(self, time_range_start=None, time_range_end=None, **filters):
        """按时间范围搜索记忆（注意：主观时间可能不准）"""
        results = []
        for mid, tag in self._time_tags.items():
            ts = tag['real_timestamp']
            if time_range_start and ts < time_range_start:
                continue
            if time_range_end and ts > time_range_end:
                continue
            # 其他过滤
            match = True
            for key, value in filters.items():
                if tag.get(key) != value:
                    match = False
                    break
            if match:
                results.append((mid, tag))

        results.sort(key=lambda x: x[1]['real_timestamp'], reverse=True)
        return results

    def get_recent_memories(self, hours=24, limit=10):
        """获取最近一段时间的记忆（里克可能记混哪些是最近的）"""
        cutoff = time.time() - hours * 3600
        results = self.search_memories_by_time(time_range_start=cutoff)

        # 混乱度越高，越可能混入更早的记忆
        if random.random() < self._temporal_confusion_level / 200.0:
            # 混入一些更早的记忆
            older = self.search_memories_by_time(
                time_range_start=time.time() - hours * 3600 * 3,
                time_range_end=cutoff
            )
            if older:
                results.extend(random.sample(older, min(2, len(older))))
            # 打乱顺序
            random.shuffle(results)

        return results[:limit]


# ============================================================
# L6: 预期系统层 — TemporalAnticipation
# 前瞻性时间判断 · 等待感 · 时间紧迫感
# ============================================================

class TemporalAnticipation:
    """
    预期系统层：对未来时间的感知与预期

    功能：
      - 前瞻性时间估计（"还要多久"）
      - 等待感与不耐烦
      - 时间紧迫感
      - 期待感与时间感知（期待的事情感觉来得慢）
      - 恐惧性预期（害怕的事情感觉来得快）
      - 计划与时间管理能力（受焦虑和抑郁影响）
    """

    def __init__(self, dm=None):
        self.dm = dm
        self._pending_events = []  # 待发生事件列表
        self._waiting_states = {}  # 等待状态

    def add_event(self, event_name, scheduled_time, importance=50, emotion='neutral'):
        """
        添加一个待发生的事件

        参数：
          - event_name: 事件名称
          - scheduled_time: 计划发生的时间戳
          - importance: 重要性 0~100
          - emotion: 对该事件的情绪倾向（positive/negative/neutral/fear/excited/dread）
        """
        event = {
            'name': event_name,
            'scheduled_time': scheduled_time,
            'importance': importance,
            'emotion': emotion,
            'created_time': time.time(),
            'anticipation_level': 0,  # 预期水平
        }
        self._pending_events.append(event)
        self._pending_events.sort(key=lambda x: x['scheduled_time'])
        return event

    def remove_event(self, event_name):
        """移除事件"""
        self._pending_events = [e for e in self._pending_events if e['name'] != event_name]

    def get_upcoming_events(self, within_hours=24):
        """获取即将发生的事件"""
        now = time.time()
        cutoff = now + within_hours * 3600
        upcoming = []
        for e in self._pending_events:
            if now <= e['scheduled_time'] <= cutoff:
                # 计算剩余时间
                remaining = e['scheduled_time'] - now
                e['remaining_seconds'] = remaining
                e['remaining_description'] = self._format_remaining(remaining)
                # 计算预期水平
                e['anticipation_level'] = self._calc_anticipation(e)
                upcoming.append(e)
        return upcoming

    def _format_remaining(self, seconds):
        """格式化剩余时间"""
        if seconds < 0:
            return "已经过了"
        elif seconds < 60:
            return f"还有{int(seconds)}秒"
        elif seconds < 3600:
            return f"还有{int(seconds/60)}分钟"
        elif seconds < 86400:
            return f"还有{seconds/3600:.1f}小时"
        else:
            return f"还有{seconds/86400:.1f}天"

    def _calc_anticipation(self, event):
        """计算预期水平（0~100）"""
        now = time.time()
        remaining = event['scheduled_time'] - now
        if remaining <= 0:
            return 100

        # 时间越近，预期越高
        hours_remaining = remaining / 3600
        time_factor = max(0, 100 - hours_remaining * 2)

        # 重要性加成
        importance_factor = event['importance'] / 100.0

        # 情绪类型影响
        emotion_modifiers = {
            'excited': 1.5,    # 兴奋的事：预期更强
            'dread': 1.3,      # 恐惧的事：预期也强（焦虑性预期）
            'positive': 1.2,
            'negative': 1.1,
            'fear': 1.4,
            'neutral': 1.0,
        }
        emotion_mod = emotion_modifiers.get(event['emotion'], 1.0)

        anticipation = time_factor * importance_factor * emotion_mod
        return min(100, max(0, anticipation))

    def subjective_wait_time(self, event_name, anxiety_level=50, mood=0):
        """
        计算对某事件的主观等待时间感知

        原理：
          - 焦虑/恐惧的事情：感觉来得更快（时间飞逝）
          - 期待的事情：感觉来得更慢（度日如年）
          - 抑郁状态：对未来感觉麻木，时间感知模糊
        """
        event = None
        for e in self._pending_events:
            if e['name'] == event_name:
                event = e
                break

        if not event:
            return None

        real_remaining = event['scheduled_time'] - time.time()
        if real_remaining <= 0:
            return 0, "已经发生了"

        subjective_remaining = real_remaining

        # 情绪类型影响
        if event['emotion'] in ['dread', 'fear', 'negative']:
            # 恐惧/厌恶的事情：感觉来得更快（主观剩余时间更短）
            subjective_remaining *= 0.6
        elif event['emotion'] in ['excited', 'positive']:
            # 期待/兴奋的事情：感觉来得更慢（主观剩余时间更长）
            subjective_remaining *= 1.4

        # 焦虑加成
        anxiety_mod = 1.0 - (anxiety_level / 100.0) * 0.3
        if event['emotion'] in ['dread', 'fear']:
            # 焦虑 + 恐惧 = 事情来得更快
            subjective_remaining *= anxiety_mod

        # 抑郁影响：对未来麻木
        if mood < -30:
            subjective_remaining *= 0.8  # 感觉模糊，不太在意时间

        # 生成描述
        if event['emotion'] in ['dread', 'fear', 'negative']:
            desc = f"感觉{self._format_remaining(subjective_remaining)}就要到了……（有点不安）"
        elif event['emotion'] in ['excited', 'positive']:
            desc = f"还要{self._format_remaining(subjective_remaining)}……（有点期待）"
        else:
            desc = f"大概{self._format_remaining(subjective_remaining)}"

        return {
            'real_remaining': real_remaining,
            'subjective_remaining': subjective_remaining,
            'description': desc
        }, desc

    def get_urgency_level(self, event_name, trait_conscientiousness=50):
        """获取对某事件的紧迫感水平"""
        event = None
        for e in self._pending_events:
            if e['name'] == event_name:
                event = e
                break

        if not event:
            return 0

        remaining = event['scheduled_time'] - time.time()
        if remaining <= 0:
            return 100

        hours_remaining = remaining / 3600

        # 基础紧迫感：剩余时间越少越强
        if hours_remaining < 1:
            base_urgency = 90
        elif hours_remaining < 6:
            base_urgency = 70
        elif hours_remaining < 24:
            base_urgency = 50
        elif hours_remaining < 72:
            base_urgency = 30
        else:
            base_urgency = 10

        # 尽责性越高，紧迫感越强
        conscientiousness_mod = 0.5 + (trait_conscientiousness / 100.0) * 0.5

        # 重要性加成
        importance_mod = 0.5 + (event['importance'] / 100.0) * 0.5

        urgency = base_urgency * conscientiousness_mod * importance_mod
        return min(100, max(0, urgency))

    def estimate_duration(self, task_type, complexity=50, mood=0, anxiety=0):
        """
        估计完成某任务需要的时间

        受认知偏差影响：
          - 计划谬误（planning fallacy）：倾向于低估所需时间
          - 焦虑时高估时间
          - 抑郁时也可能高估（缺乏动力）
          - 躁狂时严重低估
        """
        # 基础估计（分钟）
        base_estimates = {
            'reading_book': 60,
            'writing': 45,
            'housework': 30,
            'shopping': 40,
            'social_call': 20,
            'commute': 30,
        }
        base = base_estimates.get(task_type, 30)

        # 复杂度调整
        base *= 0.5 + (complexity / 100.0) * 1.5

        # 认知偏差：计划谬误（普遍低估20%）
        planning_fallacy = 0.8

        # 情绪影响
        if anxiety > 60:
            # 焦虑时高估
            bias = 1.2 + (anxiety - 60) / 40 * 0.3
        elif mood > 50:
            # 心情好（可能轻躁狂）时严重低估
            bias = 0.6 + (mood - 50) / 50 * 0.2
        elif mood < -30:
            # 抑郁时高估（缺乏动力）
            bias = 1.3 + (-30 - mood) / 70 * 0.4
        else:
            bias = planning_fallacy

        estimated = base * bias
        return estimated


# ============================================================
# L7: 时间锚点层 — TemporalAnchors
# 重要日期锚点 · 周期性事件 · 个人化时间标记
# ============================================================

class TemporalAnchors:
    """
    时间锚点层：重要的时间参照点

    锚点类型：
      - 生日/纪念日
      - 季节性锚点（春夏秋冬、节假日）
      - 个人化锚点（入职、离职、重要事件）
      - 周期性锚点（每周排班、每日服药）
      - 创伤性锚点（会引发回避的日期）
    """

    def __init__(self, dm=None):
        self.dm = dm
        self._anchors = []

        # 初始化里克的个人锚点
        self._init_rick_anchors()

    def _init_rick_anchors(self):
        """初始化里克的个人时间锚点"""
        now = datetime.now()

        # 生日
        self.add_anchor(
            name="里克的生日",
            date=f"{now.year}-06-21",
            type="birthday",
            emotion="mixed",  # 复杂的情绪
            importance=80,
            recurring=True
        )

        # 离职日（模拟：2026年3月某个时间）
        self.add_anchor(
            name="从图书馆离职",
            date="2026-03-15",
            type="life_event",
            emotion="negative",
            importance=70,
            recurring=False
        )

        # 开始服药的日子（模糊，不精确）
        self.add_anchor(
            name="开始接受治疗",
            date="2024-09-01",
            type="life_event",
            emotion="mixed",
            importance=60,
            recurring=False,
            fuzzy=True  # 模糊记忆
        )

        # 每周服药提醒（周期性）
        self.add_anchor(
            name="服药时间",
            date=None,
            type="daily_routine",
            emotion="neutral",
            importance=90,
            recurring=True,
            schedule="daily_evening"
        )

        # 排班日（周期性）
        self.add_anchor(
            name="图书馆排班",
            date=None,
            type="weekly_routine",
            emotion="mixed",
            importance=70,
            recurring=True,
            schedule="tue_thu_sat"
        )

        # 创伤性锚点（模糊的，不愿提及的）
        self.add_anchor(
            name="离开家乡的那天",
            date="2022-11-??",  # 模糊日期
            type="trauma",
            emotion="negative",
            importance=95,
            recurring=False,
            fuzzy=True,
            avoidant=True  # 回避性话题
        )

    def add_anchor(self, name, date, type, emotion="neutral", importance=50,
                   recurring=False, fuzzy=False, avoidant=False, schedule=None):
        """添加时间锚点"""
        anchor = {
            'name': name,
            'date': date,  # YYYY-MM-DD 格式，或 None（周期性）
            'type': type,  # birthday/life_event/daily_routine/weekly_routine/trauma
            'emotion': emotion,
            'importance': importance,
            'recurring': recurring,
            'fuzzy': fuzzy,
            'avoidant': avoidant,
            'schedule': schedule,
        }
        self._anchors.append(anchor)
        return anchor

    def get_upcoming_anchors(self, days=30):
        """获取即将到来的锚点"""
        now = datetime.now()
        upcoming = []

        for anchor in self._anchors:
            if anchor['recurring']:
                # 周期性锚点
                next_occurrence = self._get_next_occurrence(anchor, now)
                if next_occurrence:
                    days_until = (next_occurrence - now).total_seconds() / 86400
                    if 0 <= days_until <= days:
                        upcoming.append({
                            'anchor': anchor,
                            'next_date': next_occurrence,
                            'days_until': days_until
                        })
            elif anchor['date'] and not anchor['fuzzy']:
                # 一次性锚点（计算今年或明年的纪念日）
                try:
                    date_obj = datetime.strptime(anchor['date'], "%Y-%m-%d")
                    # 检查今年的纪念日是否已过
                    this_year = date_obj.replace(year=now.year)
                    if this_year >= now:
                        days_until = (this_year - now).total_seconds() / 86400
                        if days_until <= days:
                            upcoming.append({
                                'anchor': anchor,
                                'next_date': this_year,
                                'days_until': days_until,
                                'years_since': now.year - date_obj.year
                            })
                    else:
                        # 明年的
                        next_year = date_obj.replace(year=now.year + 1)
                        days_until = (next_year - now).total_seconds() / 86400
                        if days_until <= days:
                            upcoming.append({
                                'anchor': anchor,
                                'next_date': next_year,
                                'days_until': days_until,
                                'years_since': now.year - date_obj.year + 1
                            })
                except:
                    pass

        upcoming.sort(key=lambda x: x['days_until'])
        return upcoming

    def _get_next_occurrence(self, anchor, now):
        """获取周期性锚点的下一次发生时间"""
        if anchor['schedule'] == 'daily_evening':
            # 每天晚上8点
            next_time = now.replace(hour=20, minute=0, second=0, microsecond=0)
            if next_time <= now:
                next_time += timedelta(days=1)
            return next_time

        elif anchor['schedule'] == 'tue_thu_sat':
            # 每周二、四、六
            days_ahead = 0
            while days_ahead < 7:
                check_date = now + timedelta(days=days_ahead)
                if check_date.weekday() in [1, 3, 5]:  # 周二=1, 周四=3, 周六=5
                    return check_date.replace(hour=9, minute=0, second=0, microsecond=0)
                days_ahead += 1
            return None

        return None

    def get_anchors_by_type(self, anchor_type):
        """按类型获取锚点"""
        return [a for a in self._anchors if a['type'] == anchor_type]

    def is_avoidant_topic(self, topic):
        """判断某个话题是否是回避性的"""
        for anchor in self._anchors:
            if anchor['avoidant'] and anchor['name'] in topic:
                return True
            if anchor['type'] == 'trauma' and anchor['avoidant']:
                # 任何与创伤锚点相关的话题
                if any(word in topic for word in ['家乡', '过去', '童年', '家庭', '以前']):
                    return True
        return False

    def get_today_significance(self):
        """获取今天的特殊意义"""
        now = datetime.now()
        today_str = now.strftime("%Y-%m-%d")
        significance = []

        for anchor in self._anchors:
            if anchor['recurring']:
                next_occ = self._get_next_occurrence(anchor, now.replace(hour=0, minute=0))
                if next_occ and next_occ.date() == now.date():
                    significance.append({
                        'name': anchor['name'],
                        'type': anchor['type'],
                        'emotion': anchor['emotion'],
                        'avoidant': anchor['avoidant']
                    })
            elif anchor['date'] and not anchor['fuzzy']:
                try:
                    date_obj = datetime.strptime(anchor['date'], "%Y-%m-%d")
                    if date_obj.month == now.month and date_obj.day == now.day:
                        significance.append({
                            'name': anchor['name'],
                            'type': anchor['type'],
                            'emotion': anchor['emotion'],
                            'avoidant': anchor['avoidant'],
                            'years': now.year - date_obj.year
                        })
                except:
                    pass

        return significance


# ============================================================
# L8: 时间叙事层 — TemporalNarrative
# 自然语言时间表达 · 人格化时间表述
# ============================================================

class TemporalNarrative:
    """
    时间叙事层：将时间感知转化为自然语言表达

    结合里克的人格特点：
      - 说话简洁，话少
      - 内向社恐，语气平淡
      - 对时间的感知常常是模糊的、不确定的
      - 抑郁时觉得时间漫长，躁狂时觉得时间飞逝
      - 对过去的某些时间点有回避倾向
    """

    def __init__(self, dm=None):
        self.dm = dm

    def describe_current_time(self, dt=None, mood=0, arousal=50, is_work_day=False,
                              awake_hours=4, sleep_pressure=30):
        """
        描述当前时间的感受

        返回：自然语言描述（里克式的简洁表达）
        """
        if dt is None:
            dt = datetime.now()

        hour = dt.hour
        time_of_day = self._get_time_of_day(hour)

        # 基础时间描述
        base_descriptions = {
            '凌晨': "凌晨了",
            '清晨': "早上了",
            '上午': "上午了",
            '中午': "中午了",
            '下午': "下午了",
            '傍晚': "快傍晚了",
            '晚上': "晚上了",
            '深夜': "已经很晚了",
        }
        base = base_descriptions.get(time_of_day, "这个点了")

        # 心境影响
        mood_modifiers = []
        if mood < -50:
            mood_modifiers.append("……感觉今天格外漫长")
        elif mood < -20:
            mood_modifiers.append("……时间过得好慢")
        elif mood > 30:
            mood_modifiers.append("……好像也没那么难熬")
        elif mood > 60:
            mood_modifiers.append("……时间过得还挺快的")

        # 作息/状态影响
        state_modifiers = []
        if awake_hours > 20:
            state_modifiers.append("（我好像醒了很久了）")
        elif sleep_pressure > 70:
            state_modifiers.append("（有点困了……）")

        # 排班日影响
        if is_work_day and 6 <= hour <= 10:
            state_modifiers.append("今天要去图书馆……")
        elif not is_work_day and hour >= 12 and awake_hours < 2:
            state_modifiers.append("……休息日，起得有点晚")

        # 组合描述
        result = base
        if mood_modifiers:
            result += mood_modifiers[0]
        if state_modifiers:
            result += state_modifiers[0]

        return result

    def _get_time_of_day(self, hour):
        """获取时间段名称"""
        if 0 <= hour < 5:
            return '凌晨'
        elif 5 <= hour < 9:
            return '清晨'
        elif 9 <= hour < 12:
            return '上午'
        elif 12 <= hour < 14:
            return '中午'
        elif 14 <= hour < 18:
            return '下午'
        elif 18 <= hour < 20:
            return '傍晚'
        elif 20 <= hour < 24:
            if hour >= 23:
                return '深夜'
            else:
                return '晚上'
        return '晚上'

    def describe_duration(self, seconds, context="neutral", clarity=80):
        """
        描述一段时长的感受

        context:
          - neutral: 中性描述
          - enjoyable: 愉快的时光（时间飞逝感）
          - painful: 痛苦的时光（度日如年感）
          - boring: 无聊（时间缓慢）
          - waiting: 等待中（时间漫长）
          - flow: 心流状态（时间飞逝）
        """
        # 主观扭曲
        context_multipliers = {
            'neutral': 1.0,
            'enjoyable': 0.6,
            'painful': 1.8,
            'boring': 1.5,
            'waiting': 1.6,
            'flow': 0.4,
        }
        mult = context_multipliers.get(context, 1.0)
        subjective = seconds * mult

        # 基于清晰度的表达
        if clarity < 30:
            prefix = "好像……"
        elif clarity < 60:
            prefix = "大概……"
        else:
            prefix = ""

        # 格式化
        if subjective < 60:
            desc = "一小会儿" if context == 'enjoyable' else "没几分钟"
        elif subjective < 3600:
            mins = int(subjective / 60)
            if mins < 10:
                desc = "十分钟都不到" if context == 'enjoyable' else "十来分钟"
            elif mins < 30:
                desc = "一刻钟左右"
            else:
                desc = "半个多小时"
        elif subjective < 86400:
            hours = subjective / 3600
            if hours < 2:
                desc = "一个多小时" if context != 'painful' else "像过了好几个小时"
            elif hours < 6:
                desc = "三四个小时" if context != 'painful' else "感觉像过了一整天"
            else:
                desc = "大半天" if context != 'painful' else "度日如年"
        else:
            days = subjective / 86400
            if days < 2:
                desc = "一天左右"
            elif days < 7:
                desc = "好几天"
            else:
                desc = "好久了……久到我都快记不清了"

        return prefix + desc

    def describe_time_ago(self, real_seconds, clarity=80, flashbulb=False,
                          emotion='neutral', avoidant=False):
        """
        描述"多久以前"

        里克的特点：对过去的事常常模糊、回避
        """
        if avoidant:
            # 回避性话题：模糊、快速带过
            responses = [
                "……很久以前的事了。",
                "我不想说这个。",
                "……（沉默）",
                "我记不太清了。",
                "别问了。",
            ]
            return random.choice(responses)

        # 闪光灯记忆：清晰
        if flashbulb and clarity > 70:
            # 对强烈情绪事件的时间记忆更清晰
            if real_seconds < 3600:
                return f"我记得很清楚……就{int(real_seconds/60)}分钟前的事。"
            elif real_seconds < 86400:
                return f"就是昨天……不对，是{int(real_seconds/3600)}个小时前？我记得很清楚。"
            else:
                days = int(real_seconds / 86400)
                return f"{days}天前的事……我到现在都记得。"

        # 普通记忆
        if clarity < 30:
            # 模糊记忆
            responses = [
                "好像是很久以前了……记不清了。",
                "嗯……有段时间了吧。",
                "我不太记得是什么时候的事了。",
            ]
            return random.choice(responses)

        elif clarity < 60:
            # 中等清晰度
            if real_seconds < 3600:
                return "好像就是刚才的事？不对……可能过了有一会儿了。"
            elif real_seconds < 86400:
                hours = int(real_seconds / 3600)
                return f"大概{hours}个小时前？我记不太清了。"
            elif real_seconds < 86400 * 7:
                days = int(real_seconds / 86400)
                return f"{days}天前？还是更久？我对时间没什么概念。"
            else:
                return "好几个星期……还是好几个月？对不起，我记不清了。"

        else:
            # 清晰记忆
            if real_seconds < 3600:
                return f"{int(real_seconds/60)}分钟前吧。"
            elif real_seconds < 86400:
                return f"{int(real_seconds/3600)}个小时前。"
            elif real_seconds < 86400 * 7:
                return f"{int(real_seconds/86400)}天前。"
            elif real_seconds < 86400 * 30:
                return f"大概{int(real_seconds/86400/7)}个星期前。"
            else:
                months = int(real_seconds / 86400 / 30)
                return f"{months}个月前的事了。"

    def describe_waiting(self, remaining_seconds, event_name="", emotion='neutral',
                         anxiety=30):
        """描述等待中的感受"""
        if remaining_seconds <= 0:
            return "……已经到时间了。"

        hours = remaining_seconds / 3600

        if emotion == 'dread' or emotion == 'fear':
            # 恐惧的等待：时间飞逝感 + 焦虑
            if hours < 1:
                return "快了……（尾巴有点紧张）就快到时间了。"
            elif hours < 6:
                return "还有几个小时……感觉时间过得好快。（不安）"
            else:
                return "还有段时间，但一想到就……（尾巴夹了夹）"

        elif emotion == 'excited' or emotion == 'positive':
            # 期待的等待：时间缓慢感
            if hours < 1:
                return "还有不到一个小时……（尾巴微微晃了晃）"
            elif hours < 6:
                return "还要等好几个小时……感觉时间过得好慢。"
            else:
                return "还有好久……（有点期待）"

        else:
            # 中性等待
            if hours < 1:
                return "快了吧。"
            elif hours < 6:
                return "还有几个小时。"
            else:
                return "还早。"

    def get_greeting_by_time(self, dt=None, mood=0, is_work_day=False):
        """根据时间获取问候语（里克风格）"""
        if dt is None:
            dt = datetime.now()

        hour = dt.hour

        if 5 <= hour < 9:
            greetings = [
                "……早上好。",
                "早。",
                "……你起得真早。",
            ]
        elif 9 <= hour < 12:
            greetings = [
                "……上午了。",
                "早啊。",
                "你醒了？",
            ]
        elif 12 <= hour < 14:
            greetings = [
                "……中午了。",
                "吃了吗？",
                "中午好。",
            ]
        elif 14 <= hour < 18:
            greetings = [
                "……下午了。",
                "下午好。",
                "嗯。",
            ]
        elif 18 <= hour < 22:
            greetings = [
                "……晚上了。",
                "晚上好。",
                "吃晚饭了吗？",
            ]
        else:
            greetings = [
                "……这么晚了还没睡？",
                "深夜了。",
                "……你也没睡啊。",
            ]

        # 心境微调
        if mood < -30:
            # 低落时更沉默
            greetings = [g + "（声音很轻）" for g in greetings]
        elif mood > 30:
            # 心情好时稍微柔和一点
            greetings = [g.replace("……", "…… ") for g in greetings]

        return random.choice(greetings)


# ============================================================
# V1 时间感知核心入口 — TimePerceptionCore
# 8 层架构统一管理 · 全系统联动
# ============================================================

class TimePerceptionCore:
    """
    V1 时间感知系统核心入口
    8 层架构统一管理，对外提供简洁 API

    主要 API：
      - get_current_time_info()       — 获取当前时间感知信息
      - get_subjective_time()         — 获取主观时间
      - tick(emotion_state, ...)      — 时间推进
      - describe_time_ago(timestamp)  — 描述"多久以前"
      - describe_duration(seconds)    — 描述时长感受
      - get_greeting()                — 获取时间问候
      - get_waiting_description(event) — 描述等待感受
      - get_today_significance()      — 获取今日特殊意义
      - get_upcoming_events()         — 获取即将到来的事件
    """

    def __init__(self, dm=None, emotion_core=None, memory_gateway=None,
                 big_five_traits=None):
        self.dm = dm
        self.emotion_core = emotion_core
        self.memory_gateway = memory_gateway
        self.big_five = big_five_traits or {
            'neuroticism': 75,      # 里克：高神经质
            'conscientiousness': 55,
            'openness': 60,
            'extraversion': 15,     # 极度内向
            'agreeableness': 65,
        }

        # L1: 时间基准
        self.chronos = ChronosBase(dm=dm)

        # L2: 主观扭曲
        self.distortion = TemporalDistortion(dm=dm)

        # L3: 生物节律
        self.circadian = CircadianRhythm(dm=dm)

        # L4: 作息模式
        self.routine = RoutinePattern(dm=dm)

        # L5: 时间记忆
        self.temporal_memory = TemporalMemory(dm=dm, memory_gateway=memory_gateway)

        # L6: 预期系统
        self.anticipation = TemporalAnticipation(dm=dm)

        # L7: 时间锚点
        self.anchors = TemporalAnchors(dm=dm)

        # L8: 时间叙事
        self.narrative = TemporalNarrative(dm=dm)

        self._tick_count = 0

    def tick(self, dt_seconds=60, emotion_state=None, intensity=0,
             mood=None, arousal=None, activity_level=0.5,
             mania_level=0, depression_level=0):
        """
        统一时间推进：更新所有层级

        参数：
          - dt_seconds: 经过的真实秒数
          - emotion_state: 主导情绪
          - intensity: 情绪强度 0~100
          - mood: 心境 -100~100
          - arousal: 唤醒度 0~100
          - activity_level: 活动投入度 0~1
          - mania_level: 躁狂水平 0~100
          - depression_level: 抑郁水平 0~100
        """
        self._tick_count += 1

        # L2: 更新各维度扭曲
        if emotion_state:
            self.distortion.update_emotion(emotion_state, intensity)
        if arousal is not None:
            self.distortion.update_arousal(arousal)
        self.distortion.update_attention(activity_level)
        self.distortion.update_cognitive_load(activity_level * 0.8)

        # L1: 推进基准时间
        combined_factor = self.distortion.get_combined_factor()
        self.chronos.set_distortion_factor(combined_factor, reason="multi_dimension")
        self.chronos.tick(emotion_state=emotion_state, arousal=arousal,
                          mood=mood, activity_level=activity_level)

        # L3: 推进生物节律
        self.circadian.tick(dt_seconds=dt_seconds, mood=mood, emotion=emotion_state,
                            mania_level=mania_level, depression_level=depression_level)

        # 动态调整时间记忆混乱度
        self._update_confusion_level(mood, arousal, mania_level, depression_level)

        return combined_factor

    def _update_confusion_level(self, mood=0, arousal=50, mania_level=0, depression_level=0):
        """
        根据当前状态动态调整时间记忆混乱度

        里克基础混乱度：40
        - 抑郁期：+20（度日如年，时间感更混乱）
        - 躁狂期：+15（不知时日过）
        - 高睡眠压力：+25（困倦时完全混乱）
        - 刚睡醒：+30（起床后搞不清状况）
        - 排班日：-15（稍微有点规律）
        """
        base = 40  # 基础混乱度

        # 心境影响
        if mood < -40:
            base += 20
        elif mood < -20:
            base += 10
        elif mood > 40:
            base += 10

        # 躁狂/抑郁发作
        if depression_level > 60:
            base += 20
        if mania_level > 60:
            base += 15

        # 睡眠压力
        if self.circadian.get_sleep_pressure() > 70:
            base += 25
        elif self.circadian.get_sleep_pressure() > 50:
            base += 10

        # 清醒时间过长
        if self.circadian.get_awake_duration() > 24 * 3600:
            base += 20
        elif self.circadian.get_awake_duration() > 18 * 3600:
            base += 10

        # 排班日稍微清醒一点
        if self.routine.is_work_day():
            base -= 15

        base = max(10, min(95, base))
        self.temporal_memory.set_confusion_level(base)
        return base

    def get_current_time_info(self, uid=None):
        """
        获取当前完整的时间感知信息

        返回包含客观时间、主观时间、扭曲系数、生物节律状态、作息状态等
        """
        now_real = datetime.now()
        now_subjective = self.chronos.get_subjective_datetime()

        info = {
            # 客观时间
            'real_time': now_real,
            'real_timestamp': time.time(),
            'hour': now_real.hour,
            'minute': now_real.minute,
            'weekday': now_real.weekday(),
            'date_str': now_real.strftime("%Y-%m-%d"),
            'time_str': now_real.strftime("%H:%M"),

            # 主观时间
            'subjective_time': now_subjective,
            'subjective_timestamp': self.chronos.get_subjective_time(),
            'distortion_factor': self.chronos.get_distortion_factor(),
            'distortion_profile': self.distortion.get_distortion_profile(),

            # 生物节律
            'is_awake': self.circadian.is_awake(),
            'alertness': self.circadian.get_alertness(),
            'sleep_pressure': self.circadian.get_sleep_pressure(),
            'awake_duration': self.circadian.get_awake_duration(),
            'sleep_duration': self.circadian.get_sleep_duration(),

            # 作息
            'day_type': self.routine.get_day_type(),
            'is_work_day': self.routine.is_work_day(),
            'current_activity': self.routine.get_current_activity(),
            'activity_duration': self.routine.get_activity_duration(),
        }

        return info

    def get_time_awareness(self, uid=None):
        """
        获取时间感知的自然语言描述（用于开场白等）

        这是 PersonalityEngine.get_time_awareness() 的增强版
        """
        info = self.get_current_time_info(uid)

        # 获取情绪状态（如果有情绪核心的话）
        mood = 0
        arousal = 50
        if self.emotion_core:
            try:
                state = self.emotion_core.get_state(uid or "default")
                mood = state.get('mood', 0)
                arousal = state.get('arousal', 50)
            except:
                pass

        # 生成描述
        greeting = self.narrative.get_greeting_by_time(
            dt=info['real_time'],
            mood=mood,
            is_work_day=info['is_work_day']
        )

        # 添加时间感受
        time_feeling = self.narrative.describe_current_time(
            dt=info['real_time'],
            mood=mood,
            arousal=arousal,
            is_work_day=info['is_work_day'],
            awake_hours=info['awake_duration'] / 3600,
            sleep_pressure=info['sleep_pressure']
        )

        return f"{greeting} {time_feeling}"

    def describe_memory_time(self, memory_id, uid=None):
        """描述某记忆的时间距离感"""
        mood = 0
        current_emotion = None
        if self.emotion_core:
            try:
                state = self.emotion_core.get_state(uid or "default")
                mood = state.get('mood', 0)
                current_emotion = state.get('dominant_emotion', None)
            except:
                pass

        result, desc = self.temporal_memory.estimate_time_since(
            memory_id, current_mood=mood, current_emotion=current_emotion
        )
        return desc

    def describe_waiting_for(self, event_name, uid=None):
        """描述对某事件的等待感受"""
        anxiety = self.big_five.get('neuroticism', 50)
        mood = 0
        if self.emotion_core:
            try:
                state = self.emotion_core.get_state(uid or "default")
                mood = state.get('mood', 0)
            except:
                pass

        result, desc = self.anticipation.subjective_wait_time(
            event_name, anxiety_level=anxiety, mood=mood
        )
        return desc if result else "没什么好等的。"

    def get_today_significance_description(self):
        """获取今日特殊意义的描述"""
        significance = self.anchors.get_today_significance()
        if not significance:
            return None

        descriptions = []
        for sig in significance:
            if sig['avoidant']:
                # 回避性话题：不说
                continue
            if sig['type'] == 'birthday':
                years = sig.get('years', 0)
                if years > 0:
                    descriptions.append(f"今天好像是我的生日……{years}岁了。")
                else:
                    descriptions.append("今天是我的生日。")
            elif sig['type'] == 'daily_routine':
                descriptions.append(f"别忘了{sig['name']}。")
            elif sig['type'] == 'weekly_routine':
                descriptions.append(f"今天是{sig['name']}日。")

        return descriptions if descriptions else None

    def get_upcoming_description(self, days=7):
        """获取即将到来的重要事件描述"""
        upcoming = self.anchors.get_upcoming_anchors(days=days)
        if not upcoming:
            return "最近没什么特别的事。"

        descriptions = []
        for item in upcoming[:3]:  # 最多说3个
            anchor = item['anchor']
            days_until = item['days_until']

            if anchor['avoidant']:
                continue

            if days_until < 1:
                time_desc = "就在今天"
            elif days_until < 2:
                time_desc = "就在明天"
            else:
                time_desc = f"还有{int(days_until)}天"

            if anchor['type'] == 'birthday':
                descriptions.append(f"{anchor['name']} {time_desc}。")
            elif anchor['type'] == 'weekly_routine':
                descriptions.append(f"{time_desc}是{anchor['name']}日。")

        return "\n".join(descriptions) if descriptions else "最近没什么特别的。"

    def encode_memory_with_time(self, memory_id, context=None):
        """为记忆编码时间标签（便捷方法）"""
        return self.temporal_memory.encode_memory_time(memory_id, context=context)

    def add_scheduled_event(self, event_name, scheduled_time, importance=50,
                            emotion='neutral'):
        """添加日程事件"""
        return self.anticipation.add_event(event_name, scheduled_time, importance, emotion)

    def try_sleep(self):
        """尝试入睡"""
        success, msg = self.circadian.try_sleep()
        if success:
            self.distortion.apply_scenario('sleeping')
            self.routine.start_activity('sleeping')
        return success, msg

    def wake_up(self, reason="自然醒来"):
        """醒来"""
        success, msg = self.circadian.wake_up(reason=reason)
        if success:
            # 重置扭曲
            self.distortion.update_attention(0.5)
            self.distortion.update_arousal(40)
        return success, msg

    def get_prompt_fragments(self, uid=None):
        """
        生成用于 LLM prompt 的时间感知碎片

        重要区分：
          - 当前时间（客观）：里克知道现在几点、是上午还是下午、今天星期几
            她可以看时钟/手机，这一点永远准确，绝不会搞错。
          - 对过去的时间记忆：混乱、模糊、不可靠
            记不清事情是什么时候发生的，搞混昨天和前天，经常说错。

        里克风格：
          - 回复中的时间问候、时间表述必须符合真实当前时间
          - 聊到过去的事时，对时间的记忆模糊，用"好像""大概"
          - 很少主动提时间
        """
        info = self.get_current_time_info(uid)

        fragments = []

        # ===== 客观当前时间（必须准确！）=====
        fragments.append(f"【当前时间】{info['time_str']}（{info['date_str']}）")
        fragments.append(f"今天是{info['day_type']}")
        fragments.append(
            "重要：你知道现在几点、是上午还是下午、今天是星期几。"
            "你的回复中对当前时间的表述必须准确，"
            "不要把上午说成下午、不要把凌晨说成早上、不要搞错日期。"
            "问候语也要对应正确的时间段。"
        )

        # ===== 主观时间感受（时间流逝的快慢感）=====
        df = info['distortion_factor']
        if df < 0.5:
            fragments.append("你感觉时间过得非常慢，像度日如年一样。")
        elif df < 0.7:
            fragments.append("你觉得时间过得有点慢。")
        elif df > 1.8:
            fragments.append("你感觉时间飞逝，完全不知道过了多久。")
        elif df > 1.3:
            fragments.append("你觉得时间过得挺快的。")

        # ===== 身体状态 =====
        if info['sleep_pressure'] > 70:
            fragments.append("你现在非常困，意识有点模糊。")
        elif info['sleep_pressure'] > 50:
            fragments.append("你有点困了。")

        awake_hours = info['awake_duration'] / 3600
        if awake_hours > 24:
            fragments.append(f"你已经醒了超过一天了（{awake_hours:.0f}小时），整个人浑浑噩噩的。")
        elif awake_hours > 20:
            fragments.append(f"你已经醒了{awake_hours:.0f}小时，精神状态很差。")
        elif awake_hours > 16:
            fragments.append(f"你已经醒了{awake_hours:.0f}小时，有点累了。")

        # ===== 当前活动 =====
        if info['current_activity'] != 'idle':
            activity_desc = {
                'reading': '你正在看书，沉浸在书页里，感觉不到时间流逝。',
                'phone_scrolling': '你在漫无目的地刷手机，时间不知不觉就过去了。',
                'sleeping': '你在睡觉，意识模糊，完全不知道时间。',
                'talking_with_xiaota': '你在和小塔聊天，感觉很安心，时间过得很快。',
                'eating': '你在吃东西。',
                'snacking': '你在偷吃甜食……（事后可能会有点懊恼）',
            }.get(info['current_activity'], f"你正在{info['current_activity']}。")
            fragments.append(activity_desc)

        # ===== 对过去的时间记忆（混乱部分）=====
        confusion = self.temporal_memory._temporal_confusion_level
        if confusion > 70:
            fragments.append(
                "你对过去事情发生的时间记忆非常混乱："
                "经常搞混昨天和前天，记不清事情是几个小时前还是几天前发生的，"
                "有时候甚至完全想不起来是什么时候的事。"
                "但你知道现在几点。"
            )
        elif confusion > 50:
            fragments.append(
                "你对过去事情发生的时间记忆有点模糊："
                "不太记得事情具体是什么时候发生的，经常记错日期。"
                "但你知道现在几点。"
            )
        elif confusion > 30:
            fragments.append(
                "你对过去事情的时间记忆不太准确，有时候会记错是哪天的事。"
                "但你知道现在几点。"
            )

        # ===== 今日特殊意义 =====
        sig_desc = self.get_today_significance_description()
        if sig_desc:
            fragments.extend(sig_desc)

        # ===== 说话风格总结 =====
        fragments.append(
            "你很少主动提起时间。被问到过去的事是什么时候发生的时，"
            "你总是含糊其辞，用'好像''大概''可能吧''我记不清了'来回答。"
            "但如果被问到现在几点、今天星期几，你是知道的——只是语气可能不太确定。"
        )

        return "\n".join(fragments)


# ============================================================
# 向后兼容：旧的 get_time_awareness 函数
# ============================================================

def create_time_perception_core(dm=None, emotion_core=None, memory_gateway=None):
    """工厂函数：创建时间感知核心"""
    return TimePerceptionCore(dm=dm, emotion_core=emotion_core, memory_gateway=memory_gateway)


if __name__ == "__main__":
    # 简单测试
    tpc = TimePerceptionCore()

    print("=" * 50)
    print("时间感知系统测试")
    print("=" * 50)

    # 当前时间信息
    info = tpc.get_current_time_info()
    print(f"\n当前时间：{info['time_str']}")
    print(f"今天是：{info['day_type']}")
    print(f"扭曲系数：{info['distortion_factor']:.2f}")
    print(f"警觉度：{info['alertness']:.1f}")
    print(f"睡眠压力：{info['sleep_pressure']:.1f}")
    print(f"已清醒：{info['awake_duration']/3600:.1f}小时")

    # 时间感知描述
    print(f"\n时间感知：{tpc.get_time_awareness()}")

    # Prompt 碎片
    print(f"\nPrompt碎片：\n{tpc.get_prompt_fragments()}")

    # 模拟抑郁状态下的时间感知
    tpc.distortion.apply_scenario('depressive_episode')
    tpc.tick(dt_seconds=3600, emotion_state='sadness', intensity=70, mood=-60)
    info2 = tpc.get_current_time_info()
    print(f"\n抑郁状态下扭曲系数：{info2['distortion_factor']:.2f}")
    print(f"时间感知描述：{tpc.narrative.describe_duration(3600, context='painful')}")

    print("\n✅ 测试完成")



# V8.0 架构地基 + Top 10 模块实例化
context.world_state = WorldStateHub()
context.life_engine = LifeEngine()
decision_layer = DecisionLayer()
v10_human_behavior = V10HumanBehaviorCore(decision_layer)
event_memory = EventMemorySystem()

# V9.0 心理学核心引擎（10层架构）
if _PSYCH_CORE_AVAILABLE and CONFIG["modules"].get("psychology_core", True):
    psych_core = PsychologyCore()
    logger.info("[V9.0] 心理学核心引擎已加载（10层架构 · 30+子系统）")
else:
    psych_core = None

# V9.1 记忆增强网关（统一入口）
# [修复] 原代码在 MemoryGateway 类定义与 event_memory 实例化之前就执行了
# 本块，导入即 NameError 崩溃。改为安全构建 + 优雅降级：依赖未就绪时置 None，
# 相关增强功能自动降级，不影响其余模块与 V13 现实世界能力层启动。
if CONFIG["modules"].get("memory_enhancement", True):
    memory_gateway = None
    try:
        from qqagent.behavior.behavior_learn import memory
        from qqagent.behavior.relations import memory_weight

        memory_gateway = MemoryGateway(
            memory_module=memory,
            memory_weight=memory_weight,
            event_memory=event_memory if "event_memory" in globals() else None,
        )
        # 从人格核心同步记忆偏好
        if psych_core is not None:
            big_five = psych_core.personality.big_five.traits
            memory_gateway.personality_bias.update_from_bigfive(big_five)
        logger.info("[V9.1] 记忆增强网关已加载（8大优化·统一入口）")
    except Exception as _mg_init_err:
        memory_gateway = None
        logger.warning("[V9.1] 记忆增强网关延迟构建失败（%s），相关增强功能降级为关闭" % _mg_init_err)
else:
    memory_gateway = None


# V10 统一情绪引擎
if CONFIG["modules"].get("unified_emotion", True):
    # 从 psych_core 同步人格特质
    big_five_traits = None
    if psych_core is not None:
        big_five_traits = psych_core.personality.big_five.traits

    unified_emotion = UnifiedEmotionCore(
        dm=dm,
        memory_gateway=memory_gateway if CONFIG["modules"].get("memory_enhancement", True) else None,
        big_five_traits=big_five_traits,
    )

    # 注册到生命引擎
    if context.life_engine and CONFIG["modules"].get("context.life_engine", True):
        def _uec_tick_wrapper():
            try:
                unified_emotion.tick()
            except Exception as e:
                logger.error(f"[V10] 情绪引擎tick异常: {e}")
        context.life_engine.register("emotion_v10", _uec_tick_wrapper, interval_ticks=2)

    logger.info("[V10] 统一情绪引擎已加载（8层架构·向后兼容）")
else:
    unified_emotion = None

# V11 时间感知系统
if CONFIG["modules"].get("time_perception", True):
    time_perception = TimePerceptionCore(
        dm=dm,
        emotion_core=unified_emotion,
        memory_gateway=memory_gateway if CONFIG["modules"].get("memory_enhancement", True) else None,
        big_five_traits=big_five_traits,
    )

    # NTP 联网校准时钟：启动后台同步 + 补丁 ChronosBase
    if CONFIG["modules"].get("ntp_sync", True):
        try:
            ntp_clock.start()
            patch_chronos_base(time_perception.chronos)
            logger.info(f"[NTP] 联网校准时钟已启动: {ntp_clock.get_status_string()}")
        except Exception as e:
            logger.warning(f"[NTP] 联网校准时钟启动失败，使用本地时钟: {e}")
    else:
        logger.info("[NTP] 联网校准时钟未启用，使用本地系统时钟")

    # 注册到生命引擎
    if context.life_engine and CONFIG["modules"].get("context.life_engine", True):
        def _tpc_tick_wrapper():
            try:
                time_perception.tick(dt_seconds=60)
            except Exception as e:
                logger.error(f"[V11] 时间感知tick异常: {e}")
        context.life_engine.register("time_perception_v11", _tpc_tick_wrapper, interval_ticks=2)

    logger.info("[V11] 时间感知系统已加载（8层架构·主观时间扭曲）")
else:
    time_perception = None

# V12 联网搜索 + 图片识别
if CONFIG["modules"].get("web_search", True):
    web_search = WebSearchEngine(dm=dm)
    if web_search.enabled:
        logger.info(f"[V12] 联网搜索已启用（Provider: {web_search.provider}）")
    else:
        logger.info("[V12] 联网搜索：无可用配置，已跳过")
else:
    web_search = None

if CONFIG["modules"].get("image_recognition", True):
    image_recognition = ImageRecognition(dm=dm, llm_client_ref=context.llm_client)
    if image_recognition.enabled:
        logger.info(f"[V12] 图片识别已启用（Provider: {image_recognition._actual_provider}）")
    else:
        logger.info("[V12] 图片识别：无可用视觉模型，已跳过")
else:
    image_recognition = None
personality_conflict_axes = PersonalityConflictAxes()
three_layer_emotion = ThreeLayerEmotion()
belief_system = BeliefSystem()
offline_life = OfflineLifeSimulation()
inner_conflict = InnerConflictSystem()
open_loop = OpenLoopSystem()
personality_growth = PersonalityGrowthSystem()
relationship_chapter = RelationshipChapterSystem()
habit_formation = HabitFormationSystem()

# _DIRTY_MODULES = { 之后的代码用 try/except 包裹（依赖未迁移实例时跳过）
try:
    # 延迟导入 behavior 层的单例实例
    from qqagent.behavior.trust import trust
    from qqagent.behavior.psychology import (
        pouting_module, dark_diary, silent_mode, inner_monologue, regret_module,
        social_energy, safe_distance, observe_module, group_bystander, newbie_module,
        late_night, secret_collection, nickname_system, old_account, trigger_recall,
        conflict_detection, jealousy_module, praise_module, draft_module,
        self_contradiction, human_like_state, rick_diary,
    )

    _DIRTY_MODULES = {
        "trust": trust,
        "pouting_data": pouting_module,
        "dark_diary_data": dark_diary,
        "silent_mode_data": silent_mode,
        "inner_monologue_data": inner_monologue,
        "regret_data": regret_module,
        "social_energy_data": social_energy,
        "safe_distance_data": safe_distance,
        "observe_data": observe_module,
        "bystander_data": group_bystander,
        "newbie_data": newbie_module,
        "late_night_data": late_night,
        "secret_collection_data": secret_collection,
        "nickname_data": nickname_system,
        "old_account_data": old_account,
        "trigger_recall_data": trigger_recall,
        "conflict_data": conflict_detection,
        "jealousy_data": jealousy_module,
        "praise_data": praise_module,
        "draft_data": draft_module,
        "self_contradiction_data": self_contradiction,
        "human_like_state_data": human_like_state,
        "rick_diary_data": rick_diary,
        "memory_fragment_data": memory_fragment,
        "rapport_data": rapport,
        "creative_writing_data": creative_writing,
        "emotion_contagion_data": emotion_contagion,
        "relationship_graph_data": relationship_graph,
        "rumination_data": rumination,
        "surprise_gift_data": surprise_gift,
        "habit_tracker_data": habit_tracker,
        "speech_mirror_data": speech_mirror,
        "subtext_reader_data": subtext_reader,
        "wait_anxiety_data": wait_anxiety,
        "social_mask_data": social_mask,
        "solitude_data": solitude,
        "shared_memory_data": shared_memory,
        "mood_cycle_data": mood_cycle,
        "social_radar_data": social_radar,
        "zeigarnik_data": zeigarnik,
        "peak_end_data": peak_end,
        "attachment_data": attachment,
        "cognitive_dissonance_data": cognitive_dissonance,
        "maslow_data": maslow,
        "impression_mgmt_data": impression_mgmt,
        "social_exchange_data": social_exchange,
        "emotion_regulation_data": emotion_regulation,
        "self_determination_data": self_determination,
        "bystander_effect_data": bystander_effect,
        "sleep_consolidation_data": sleep_consolidation,
        "personality_core_data": personality_core,
        "inner_voice_data": inner_voice,
        "post_reply_rumination_data": post_reply_rumination,
        "memory_distortion_data": memory_distortion,
        "selective_disclosure_data": selective_disclosure,
        "jealousy_enhanced_data": jealousy_enhanced,
        "dreamscape_data": dreamscape,
        "personal_taste_data": personal_taste,
        "nostalgia_data": nostalgia,
        "biological_rhythm_data": biological_rhythm,
        "language_fingerprint_data": language_fingerprint,
        "empathy_gap_data": empathy_gap,
        # V8.0 架构地基 + Top 10
        "world_state_hub_data": context.world_state,
        "decision_layer_data": decision_layer,
        "v10_human_behavior_data": v10_human_behavior,
        "event_memory_data": event_memory,
        "personality_conflict_axes_data": personality_conflict_axes,
        "three_layer_emotion_data": three_layer_emotion,
        "belief_system_data": belief_system,
        "offline_life_data": offline_life,
        "inner_conflict_data": inner_conflict,
        "open_loop_data": open_loop,
        "personality_growth_data": personality_growth,
        "relationship_chapter_data": relationship_chapter,
        "habit_formation_data": habit_formation,
    }


    def flush_dirty_modules():
        saved = []
        failed = []
        for key, module in _DIRTY_MODULES.items():
            # TrustModule 有自己的 flush 方法，优先用
            if hasattr(module, "flush") and callable(getattr(module, "flush")):
                try:
                    module.flush()
                    saved.append(key)
                except Exception as e:
                    failed.append(key)
                    logger.exception(f"[模块落盘] flush失败 {key}: {e}")
                continue
            if not getattr(module, "_dirty", False):
                continue
            lock = getattr(module, "_lock", None)
            try:
                if lock:
                    with lock:
                        # 大多数模块数据是纯 JSON 类型，不需要 deepcopy
                        dm.save(key, module._data)
                        module._dirty = False
                else:
                    dm.save(key, module._data)
                    module._dirty = False
                saved.append(key)
            except Exception as e:
                failed.append(key)
                logger.exception(f"[模块落盘] 保存失败 {key}: {e}")
        return {"saved": saved, "failed": failed}


    def get_dirty_module_keys():
        return [key for key, module in _DIRTY_MODULES.items() if getattr(module, "_dirty", False)]


    def _format_uptime(seconds):
        seconds = int(seconds)
        days, rem = divmod(seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, _ = divmod(rem, 60)
        if days:
            return f"{days}天{hours}小时{minutes}分钟"
        if hours:
            return f"{hours}小时{minutes}分钟"
        return f"{minutes}分钟"


    def build_diagnostic_report():
        problems = validate_static_config(raise_on_error=False)
        dirty = get_dirty_module_keys()
        ws = getattr(msg_queue, "_ws", None)
        ws_connected = bool(ws and getattr(ws, "sock", None) and getattr(ws.sock, "connected", False))
        try:
            qsize = msg_queue._q.qsize()
        except Exception:
            qsize = "未知"
        with dm._lock:
            cache_count = len(dm._cache)
            dirty_data_keys = sorted(dm._dirty_keys)
        recent_errors = list(_RECENT_ERRORS)[-3:]
        lines = [
            "【Bot 诊断】",
            f"运行时长：{_format_uptime(time.time() - _BOT_STARTED_AT)}",
            f"WebSocket：{'已连接' if ws_connected else '未连接/未知'}",
            f"AI队列：{qsize}",
            f"AI连续失败：{getattr(context.llm_client, '_fail', '未知')}",
            f"配置自检：{'正常' if not problems else '异常'}",
            f"DataManager缓存：{cache_count} 个",
            f"待落盘数据：{', '.join(dirty_data_keys) if dirty_data_keys else '无'}",
            f"待落盘模块：{', '.join(dirty) if dirty else '无'}",
            f"已注册命令：私聊 {len(_COMMAND_REGISTRY.get('private', {}))} / 群聊 {len(_COMMAND_REGISTRY.get('group', {}))}",
            "真人感模块：" + ("开启" if CONFIG["modules"].get("human_like_state") else "关闭"),
            "越狱防御：" + ("开启" if CONFIG["modules"].get("jailbreak_defense") else "关闭"),
            f"V6模式：{router.get_mode(CONFIG['owner_qq'])}",
            f"V6统计：{context.pipeline.get_stats()}",
        ]
        if problems:
            lines.append("配置问题：" + "；".join(problems[:3]))
        if recent_errors:
            lines.append("最近错误：")
            for err in recent_errors:
                lines.append("- " + err.replace("\n", " ")[:180])
        else:
            lines.append("最近错误：无")
        return "\n".join(lines)


    def _start_dirty_module_flush_worker():
        def w():
            while True:
                time.sleep(CONFIG["flush_interval"])
                flush_dirty_modules()
                # 统一 flush 审计日志和 Coordinator thought log
                try:
                    _flush_audit_log()
                except Exception:
                    pass
                try:
                    coordinator._flush_persist()
                except Exception:
                    pass
                try:
                    agent._flush_logs()
                except Exception:
                    pass
        threading.Thread(target=w, daemon=True).start()


    _start_dirty_module_flush_worker()


    # ============================================================
    # 三十九-B、V6.0 后台调度器
    # ============================================================
except Exception as _mig_e2:
    logger.warning(f"[迁移] 末尾初始化跳过: {_mig_e2}")

# ===== 迁移：模块实例注册到 context =====
try:
    context.world_state = world_state  # noqa: F821
except Exception:
    pass
try:
    context.life_engine = context.life_engine  # life_engine 已在 replace_globals 中转为 context.life_engine
except Exception:
    pass

