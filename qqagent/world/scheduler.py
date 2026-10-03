#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""后台调度器（从 src_l_scheduler.py 迁移）。"""
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
from datetime import datetime, timedelta
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed

from qqagent.core import CONFIG, logger, dm
from qqagent.core import context

class BackgroundScheduler:
    """V6.0 后台调度器——定时执行 Agent 任务、主动行为、日记、梦境等"""

    def __init__(self):
        self._running = False
        self._threads = []

    def start(self):
        if self._running:
            return
        self._running = True

        if not CONFIG["modules"].get("v6_background_scheduler", True):
            logger.info("[后台调度器] 未启用")
            return

        # 后台 Agent 定时任务
        t1 = threading.Thread(target=self._agent_loop, daemon=True)
        t1.start()
        self._threads.append(t1)

        # 主动行为检查
        if CONFIG.get("v6", {}).get("enable_proactive", True) and CONFIG["modules"].get("v6_proactive", True):
            t2 = threading.Thread(target=self._proactive_loop, daemon=True)
            t2.start()
            self._threads.append(t2)

        # 每日日记
        if CONFIG.get("v6", {}).get("enable_auto_diary", True):
            t3 = threading.Thread(target=self._diary_loop, daemon=True)
            t3.start()
            self._threads.append(t3)

        # 梦境生成
        if CONFIG.get("v6", {}).get("enable_dream", False):
            t4 = threading.Thread(target=self._dream_loop, daemon=True)
            t4.start()
            self._threads.append(t4)

        # V6.5: 深夜创意写作 + 反刍思维
        if CONFIG["modules"].get("creative_writing") or CONFIG["modules"].get("rumination"):
            t5 = threading.Thread(target=self._deep_night_loop, daemon=True)
            t5.start()
            self._threads.append(t5)

        # V6.5: 全局情绪衰减
        if CONFIG["modules"].get("emotion_contagion"):
            t6 = threading.Thread(target=self._emotion_decay_loop, daemon=True)
            t6.start()
            self._threads.append(t6)

        # V6.5: 惊喜礼物
        if CONFIG["modules"].get("surprise_gift"):
            t7 = threading.Thread(target=self._surprise_gift_loop, daemon=True)
            t7.start()
            self._threads.append(t7)

        # V6.6: 情绪周期 + 遗忘曲线 + 独处时光
        if CONFIG["modules"].get("mood_cycle"):
            t8 = threading.Thread(target=self._mood_cycle_loop, daemon=True)
            t8.start()
            self._threads.append(t8)
        if CONFIG["modules"].get("forgetting_curve"):
            t9 = threading.Thread(target=self._forgetting_curve_loop, daemon=True)
            t9.start()
            self._threads.append(t9)
        if CONFIG["modules"].get("solitude"):
            t10 = threading.Thread(target=self._solitude_loop, daemon=True)
            t10.start()
            self._threads.append(t10)

        # V6.7: 马斯洛需求衰减 + 睡眠巩固
        if CONFIG["modules"].get("maslow"):
            t11 = threading.Thread(target=self._maslow_decay_loop, daemon=True)
            t11.start()
            self._threads.append(t11)
        if CONFIG["modules"].get("sleep_consolidation"):
            t12 = threading.Thread(target=self._sleep_consolidation_loop, daemon=True)
            t12.start()
            self._threads.append(t12)

        # V6.8: 人格核心月度审视
        if CONFIG["modules"].get("personality_core", True):
            t13 = threading.Thread(target=self._personality_review_loop, daemon=True)
            t13.start()
            self._threads.append(t13)

        # V6.9: 人性化增强后台线程
        if CONFIG["modules"].get("inner_voice"):
            t14 = threading.Thread(target=self._inner_voice_loop, daemon=True)
            t14.start()
            self._threads.append(t14)
        if CONFIG["modules"].get("biological_rhythm"):
            t15 = threading.Thread(target=self._biological_rhythm_loop, daemon=True)
            t15.start()
            self._threads.append(t15)
        if CONFIG["modules"].get("jealousy_enhanced"):
            t16 = threading.Thread(target=self._jealousy_decay_loop, daemon=True)
            t16.start()
            self._threads.append(t16)
        if CONFIG["modules"].get("dreamscape"):
            t17 = threading.Thread(target=self._dreamscape_loop, daemon=True)
            t17.start()
            self._threads.append(t17)
        if CONFIG["modules"].get("post_reply_rumination"):
            t18 = threading.Thread(target=self._waiting_anxiety_loop, daemon=True)
            t18.start()
            self._threads.append(t18)

        logger.info(f"[后台调度器] 已启动 {len(self._threads)} 个后台线程")

        # V7.0: 注册模块到生命引擎
        if CONFIG["modules"].get("context.life_engine"):
            context.life_engine.register("context.world_state", context.world_state.update_phase, 2)          # 每60秒
            context.life_engine.register("offline_life", offline_life.update_activity, 4)     # 每120秒
            context.life_engine.register("event_expiry", event_memory.check_expiry, 120)      # 每小时
            context.life_engine.register("open_loop_check", open_loop.check_due, 60)          # 每30分钟
            context.life_engine.register("social_pressure_decay", lambda: context.world_state.add_social_pressure(-0.05), 10)
            context.life_engine.register("state_fluctuation", lambda: (
                human_like_state.fluctuate_all(),
                context.get("emotion_isolated").fluctuate_all(),
                personality.fluctuate_all()
            ), 20)  # 每10分钟随机波动一次
            # V9.0: 心理学核心引擎时间推进（每60秒，情绪自然衰减+内省）
            if psych_core is not None and CONFIG["modules"].get("psychology_core", True):
                context.life_engine.register("psych_core_tick", psych_core.tick, 2)
                logger.info("[V9.0] 心理学核心引擎已注册到生命引擎")
            context.life_engine.start()
            logger.info("[生命引擎] 已启动并注册模块")
            human_like_state.fluctuate_all()
            context.get("emotion_isolated").fluctuate_all()
            personality.fluctuate_all()
            def _startup_dream():
                time.sleep(8)
                try:
                    logger.info("[启动] 开始生成梦境...")
                    dream = dreamscape.generate_dream()
                    if dream:
                        logger.info(f"[启动] 梦境生成成功: {dream.get('emotion', '?')} - {dream.get('content', '')[:50]}")
                    else:
                        logger.info("[启动] 梦境生成跳过（今日已生成）")
                except Exception as e:
                    logger.error(f"[启动] 梦境生成失败: {e}", exc_info=True)
            threading.Thread(target=_startup_dream, daemon=True).start()

    def _agent_loop(self):
        """后台 Agent 定时执行"""
        interval = CONFIG.get("v6", {}).get("background_agent_interval", 300)
        while self._running:
            time.sleep(interval)
            try:
                # 记忆整理
                agent.submit("cleanup_memory")
                # 世界状态刷新
                agent.submit("world_refresh")
                logger.debug("[后台调度器] Agent定时任务已提交")
            except Exception as e:
                logger.error(f"[后台调度器] Agent循环异常: {e}")

    def _proactive_loop(self):
        """主动行为检查循环——随机间隔触发"""
        v6 = CONFIG.get("v6", {})
        min_int = v6.get("proactive_min_interval", 120)
        max_int = v6.get("proactive_max_interval", 480)
        first = True
        while self._running:
            if first:
                wait = 300  # 启动后5分钟首次检查，避免重启即发消息
                first = False
            else:
                wait = random.uniform(min_int, max_int)
            time.sleep(wait)
            try:
                agent.submit("proactive_check")
                logger.debug(f"[后台调度器] 主动行为检查已提交（下次随机等待{wait:.0f}秒）")
            except Exception as e:
                logger.error(f"[后台调度器] 主动行为循环异常: {e}")

    def _diary_loop(self):
        """每日日记生成"""
        diary_time = CONFIG.get("v6", {}).get("diary_time", "23:00")
        while self._running:
            try:
                now = datetime.now()
                target_hour, target_min = map(int, diary_time.split(":"))
                # 计算到下一个目标时间点的秒数
                target = now.replace(hour=target_hour, minute=target_min, second=0, microsecond=0)
                if target <= now:
                    target += timedelta(days=1)
                wait_seconds = (target - now).total_seconds()
                time.sleep(min(wait_seconds, 3600))  # 最多等1小时再检查

                if datetime.now().hour == target_hour and datetime.now().minute >= target_min:
                    agent.submit("daily_diary")
                    logger.info("[后台调度器] 每日日记已提交")
                    time.sleep(60)  # 避免重复触发
            except Exception as e:
                logger.error(f"[后台调度器] 日记循环异常: {e}")
                time.sleep(3600)

    def _dream_loop(self):
        """梦境生成循环"""
        interval = CONFIG.get("v6", {}).get("dream_interval", 86400)
        while self._running:
            time.sleep(interval)
            try:
                agent.submit("dream")
                logger.info("[后台调度器] 梦境生成已提交")
            except Exception as e:
                logger.error(f"[后台调度器] 梦境循环异常: {e}")

    def _deep_night_loop(self):
        """深夜专属循环——创意写作 + 反刍思维（凌晨 1~4 点）"""
        while self._running:
            try:
                time.sleep(1800)  # 每 30 分钟检查一次
                now = datetime.now()
                if not (1 <= now.hour <= 4):
                    continue
                # 创意写作
                if CONFIG["modules"].get("creative_writing"):
                    work = creative_writing.maybe_write()
                    if work:
                        logger.info(f"[后台调度器] 创意写作完成: {work.get('date')}")
                        # 随机选一个高信任用户分享
                        for uid_str, _ in list(user_profiles._data.items()):
                            try:
                                uid = int(uid_str)
                            except (ValueError, TypeError):
                                continue
                            if trust.get(uid) >= 500 and random.random() < 0.3:
                                msg = f"......睡不着，写了点东西。\n\n「{work['content']}」"
                                msg = security_shield.sanitize_output(msg, uid=uid)
                                if context.wsm and context.wsm._ws:
                                    send_private(context.wsm._ws, uid, msg)
                                    logger.info(f"[创意写作] 分享给 {uid}")
                                break
                # 反刍思维
                if CONFIG["modules"].get("rumination"):
                    result = rumination.ruminate()
                    if result:
                        # 通过 Agent 主动发送给对应用户
                        agent.submit("rumination_send", result)
                        logger.info(f"[后台调度器] 反刍思维完成，目标: {result.get('uid')}")
            except Exception as e:
                logger.error(f"[后台调度器] 深夜循环异常: {e}")
                time.sleep(3600)

    def _emotion_decay_loop(self):
        """全局情绪衰减循环——每 10 分钟衰减一次"""
        while self._running:
            time.sleep(600)
            try:
                if CONFIG["modules"].get("emotion_contagion"):
                    emotion_contagion.decay()
            except Exception as e:
                logger.error(f"[后台调度器] 情绪衰减循环异常: {e}")

    def _surprise_gift_loop(self):
        """惊喜礼物检查循环——随机间隔触发"""
        while self._running:
            wait = random.uniform(3600, 10800)  # 1~3 小时检查一次
            time.sleep(wait)
            try:
                if CONFIG["modules"].get("surprise_gift"):
                    agent.submit("surprise_gift_check")
                    logger.debug("[后台调度器] 惊喜礼物检查已提交")
            except Exception as e:
                logger.error(f"[后台调度器] 惊喜礼物循环异常: {e}")

    def _mood_cycle_loop(self):
        """情绪周期更新——每 30 分钟推进一次"""
        while self._running:
            time.sleep(1800)
            try:
                if CONFIG["modules"].get("mood_cycle"):
                    mood_cycle.update()
                    logger.debug("[后台调度器] 情绪周期已更新")
            except Exception as e:
                logger.error(f"[后台调度器] 情绪周期循环异常: {e}")

    def _forgetting_curve_loop(self):
        """遗忘曲线衰减——每小时对所有用户的记忆应用衰减"""
        while self._running:
            time.sleep(3600)
            try:
                if CONFIG["modules"].get("forgetting_curve"):
                    for uid_str in list(user_profiles._data.keys()):
                        try:
                            uid = int(uid_str)
                        except (ValueError, TypeError):
                            continue
                        faded = forgetting_curve.decay_memories(uid)
                        if faded > 0:
                            logger.debug(f"[后台调度器] 遗忘曲线: {uid} 有 {faded} 条记忆变模糊")
            except Exception as e:
                logger.error(f"[后台调度器] 遗忘曲线循环异常: {e}")

    def _solitude_loop(self):
        """独处时光——检测无人对话时生成独处活动"""
        while self._running:
            time.sleep(1800)  # 每 30 分钟检查一次
            try:
                if CONFIG["modules"].get("solitude"):
                    # 检查最近 1 小时内是否有人找 Rick
                    now = time.time()
                    has_recent_interaction = False
                    for uid_str, data in user_profiles._data.items():
                        last = data.get("last_seen", "")
                        if last:
                            try:
                                last_dt = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
                                if (now - last_dt.timestamp()) < 3600:
                                    has_recent_interaction = True
                                    break
                            except Exception:
                                continue
                    if has_recent_interaction:
                        solitude.update_last_alone_time(0)  # 重置独处时间
                    else:
                        # 无人对话，更新独处开始时间并可能生成活动
                        alone_duration = solitude.check_solitude()
                        if alone_duration == 0:
                            solitude.update_last_alone_time()
                        elif alone_duration > 3600:  # 独处超过 1 小时
                            activity = solitude.generate_activity()
                            if activity:
                                logger.info(f"[后台调度器] 独处时光: {activity['type']}")
            except Exception as e:
                logger.error(f"[后台调度器] 独处时光循环异常: {e}")

    def _maslow_decay_loop(self):
        """马斯洛需求衰减——每 2 小时衰减一次"""
        while self._running:
            time.sleep(7200)
            try:
                if CONFIG["modules"].get("maslow"):
                    maslow.decay()
                    logger.debug("[后台调度器] 马斯洛需求已衰减")
            except Exception as e:
                logger.error(f"[后台调度器] 马斯洛衰减循环异常: {e}")

    def _sleep_consolidation_loop(self):
        """睡眠记忆巩固——每 30 分钟检查一次，凌晨 2~5 点触发"""
        while self._running:
            time.sleep(1800)
            try:
                if CONFIG["modules"].get("sleep_consolidation"):
                    result = sleep_consolidation.consolidate()
                    if result:
                        logger.info(f"[后台调度器] 睡眠巩固完成: {len(result)} 个用户")
            except Exception as e:
                logger.error(f"[后台调度器] 睡眠巩固循环异常: {e}")

    def _personality_review_loop(self):
        """人格核心月度审视——每天检查一次，每月最多执行一次审视"""
        while self._running:
            time.sleep(86400)  # 每 24 小时检查一次
            try:
                if CONFIG["modules"].get("personality_core", True):
                    result = personality_core.monthly_review()
                    if result:
                        logger.info("[后台调度器] 人格核心月度审视已完成")
            except Exception as e:
                logger.error(f"[后台调度器] 人格审视循环异常: {e}")

    def _inner_voice_loop(self):
        """内心独白生成循环——每 5~15 分钟生成一段"""
        while self._running:
            wait = random.uniform(300, 900)
            time.sleep(wait)
            try:
                if CONFIG["modules"].get("inner_voice"):
                    # 50%概率生成全局独白，50%针对最近互动的用户
                    if random.random() < 0.5:
                        inner_voice.generate(None)
                    else:
                        # 找最近互动的用户
                        try:
                            recent_uids = list(user_profiles._data.keys())[-5:]
                            if recent_uids:
                                uid = int(random.choice(recent_uids))
                                inner_voice.generate(uid)
                        except Exception:
                            inner_voice.generate(None)
            except Exception as e:
                logger.error(f"[后台调度器] 内心独白循环异常: {e}")

    def _biological_rhythm_loop(self):
        """生理节律更新——每 15 分钟更新一次"""
        while self._running:
            time.sleep(900)
            try:
                if CONFIG["modules"].get("biological_rhythm"):
                    biological_rhythm.update()
            except Exception as e:
                logger.error(f"[后台调度器] 生理节律循环异常: {e}")

    def _jealousy_decay_loop(self):
        """嫉妒衰减循环——每 30 分钟衰减一次"""
        while self._running:
            time.sleep(1800)
            try:
                if CONFIG["modules"].get("jealousy_enhanced"):
                    jealousy_enhanced.decay()
            except Exception as e:
                logger.error(f"[后台调度器] 嫉妒衰减循环异常: {e}")

    def _dreamscape_loop(self):
        """梦境生成循环——每天凌晨触发一次"""
        while self._running:
            try:
                now = datetime.now()
                # 凌晨 2~4 点之间触发
                if 2 <= now.hour <= 4:
                    if CONFIG["modules"].get("dreamscape"):
                        dream = dreamscape.generate_dream()
                        if dream:
                            logger.info(f"[后台调度器] 梦境生成完成: {dream['emotion']}")
                    time.sleep(3600)  # 触发后等1小时
                else:
                    time.sleep(1800)  # 非凌晨每30分钟检查一次
            except Exception as e:
                logger.error(f"[后台调度器] 梦境循环异常: {e}")
                time.sleep(3600)

    def _waiting_anxiety_loop(self):
        """等待焦虑检查——每 10 分钟检查一次"""
        while self._running:
            time.sleep(600)
            try:
                if CONFIG["modules"].get("post_reply_rumination"):
                    post_reply_rumination.check_waiting_anxiety()
            except Exception as e:
                logger.error(f"[后台调度器] 等待焦虑循环异常: {e}")


    def stop(self):
        self._running = False


context.bg_scheduler = BackgroundScheduler()

