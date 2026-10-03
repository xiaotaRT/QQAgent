#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多模型流水线（从 src_g_pipeline.py 迁移）。

MultiModelPipeline 统一调度 emotion/intent/memory/relationship/vision 等认知模块，
为同一个 Riku Core 提供信息，不形成多个独立人格。
"""
import os
import json
import time
import random
import threading
import logging
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

from qqagent.core import CONFIG, logger, dm
from qqagent.core import context
from qqagent.models.llm_router import (
    LLMClient, Router, Coordinator, Talk, Judge, Agent,
    EmotionAnalyzer, IntentAnalyzer, MemoryAnalyzer, RelationshipAnalyzer,
    _v6_resolve_all,
)

class MultiModelPipeline:
    """真并行多模型管道——所有分析模型同时启动，Coordinator 合并结果
    优化：健康降级 + 结果缓存 + 预解析配置 + 优雅关闭
    """

    # 模型注册表——名字 → 实例
    ANALYZERS = {
        "emotion": EmotionAnalyzer(),
        "intent": IntentAnalyzer(),
        "memory": MemoryAnalyzer(),
        "relationship": RelationshipAnalyzer(),
    }

    # 分析结果缓存——短时间内相同消息复用，省 API 调用
    _CACHE_TTL = 15  # 15 秒内相同 uid+消息复用分析结果

    def __init__(self):
        self._stats = defaultdict(int)
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="analyzer")
        self._analysis_cache = {}  # (uid, msg_hash) → (result, timestamp)

    def process(self, uid, message, is_owner=False, extra="", gid=None):
        """
        真并行处理：
        1. Router 决策
        2. 并行启动 N 个分析模型（ThreadPoolExecutor）
        3. Coordinator 合并所有结果
        4. Talk 生成回复
        5. Judge 校验（可选）
        6. Agent 后台执行（与以上全程并行）
        """
        self._stats["total"] += 1

        # ---- 解析括号内的动作/心理活动描写 ----
        user_actions = []
        clean_message = message
        action_matches = re.findall(r'[（(]([^）)]{2,30})[）)]', str(message))
        if action_matches:
            user_actions = action_matches
            clean_message = re.sub(r'[（(][^）)]{2,30}[）)]', '', str(message)).strip()
            if clean_message:
                message = clean_message
            extra = (extra + "\n" if extra else "") + "对方的行为/心理描写（不是对方说的话，而是对方做的动作或内心活动）：" + "；".join(user_actions)

        # ---- 解析图片/表情包 ----
        image_descs = []
        cq_images = re.findall(r'\[CQ:image([^\]]*)\]', str(message))
        for img_params in cq_images:
            # 提取图片 URL（用于视觉识别）
            img_url = None
            url_match = re.search(r'url=([^,\]]+)', img_params)
            if url_match:
                import html as _html_mod
                img_url = _html_mod.unescape(url_match.group(1))

            summary_match = re.search(r'summary=([^,\]]+)', img_params)
            raw_summary = None
            if summary_match:
                raw_summary = summary_match.group(1)
                import html
                decoded = html.unescape(raw_summary).strip()
                decoded = decoded.strip('[]')
                if decoded:
                    image_descs.append(decoded)
            else:
                sub_match = re.search(r'sub_type=(\d+)', img_params)
                if sub_match and sub_match.group(1) == '0':
                    image_descs.append("一张图片")
                else:
                    image_descs.append("一个表情包")

            # V12: 如果有图片识别模块，尝试更准确地识别图片内容
            if (CONFIG["modules"].get("image_recognition", True)
                    and 'image_recognition' in globals()
                    and image_recognition is not None
                    and image_recognition.enabled
                    and img_url):
                try:
                    vision_desc = image_recognition.recognize(
                        img_url,
                        prompt="请简洁地描述这张图片里有什么，1-2句话，用中文。"
                    )
                    if vision_desc and len(vision_desc) > 5:
                        # 替换掉原来的通用描述
                        if image_descs and image_descs[-1] in ["一张图片", "一个表情包"]:
                            image_descs[-1] = vision_desc
                        elif image_descs:
                            image_descs[-1] = vision_desc
                        else:
                            image_descs.append(vision_desc)
                except Exception as _img_e:
                    logger.debug(f"[V12] 图片识别失败: {_img_e}")
                    # 失败就用原来的描述，不影响主流程
        if image_descs:
            if len(image_descs) == 1:
                img_info = f"对方发了一张图片/表情包，内容描述：{image_descs[0]}"
            else:
                img_info = f"对方发了{len(image_descs)}张图片/表情包，分别是：" + "、".join(image_descs)
            extra = (extra + "\n" if extra else "") + img_info
            # 从消息文本中去掉CQ图片标签
            message = re.sub(r'\[CQ:image[^\]]*\]', '', message).strip()
            if not message:
                message = "（发了一张表情包）"

        # ---- V12: 联网搜索（当消息涉及实时信息时自动搜索） ----
        if (CONFIG["modules"].get("web_search", True)
                and 'web_search' in globals()
                and web_search is not None
                and web_search.enabled
                and message
                and len(message) > 3):
            try:
                search_results = web_search.maybe_search(message)
                if search_results:
                    search_text = web_search.format_results(search_results, query=message)
                    if search_text:
                        extra = (extra + "\n" if extra else "") + search_text
            except Exception as _search_e:
                logger.debug(f"[V12] 搜索异常: {_search_e}")
                # 搜索失败不影响主流程

        # ---- 离别/回归检测 ----
        farewell_kw = ["再见", "拜拜", "晚安", "走了", "先去忙", "回头聊", "下次聊", "去吃饭", "去写", "去洗澡", "去睡", "先忙", "下线", "出门", "去学校", "去上课", "回去"]
        is_farewell = any(kw in str(message) for kw in farewell_kw)
        if is_farewell:
            extra = (extra + "\n" if extra else "") + "对方在说再见/告别。你可以自然地回应告别，比如'下次要晚点回来'之类的——表示你会记得对方，也希望对方记得你。不要每次都说一样的话。"

        memory_safe = True
        if CONFIG["modules"].get("jailbreak_defense"):
            memory_safe = jailbreak_defense.is_memory_safe(message)

        # ---- 健康降级：LLM 不健康时自动切 single 模式，减少 API 调用加速回复 ----
        force_single = False
        if not llm_client.healthy:
            logger.warning(f"[Pipeline] LLM 不健康（连续失败 {llm_client._fail}），降级为 single 模式")
            force_single = True
            self._stats["health_degrade"] += 1

        # 1. Router 决策
        if force_single:
            decision = {"models": [], "need_talk": True, "need_agent": False,
                        "need_judge": False, "complexity": "low", "mode": "degraded", "reason": "LLM不健康降级"}
        elif CONFIG["modules"].get("v6_router", True):
            decision = router.route(uid, message, is_owner, gid)
        else:
            decision = {"models": ["emotion"], "need_talk": True, "need_agent": True,
                        "need_judge": False, "complexity": "low", "mode": "auto", "reason": "router关闭"}

        model_names = decision.get("models", [])
        # 显示每个分析器实际使用的模型（用预解析缓存，减少 dict 查找）
        model_details = []
        for name in model_names:
            resolved = _V6_RESOLVED.get(name)
            if resolved:
                m = resolved[2]
                slot_str = f"/{resolved[3]}" if resolved[3] else ""
            else:
                m = None
                slot_str = ""
            model_details.append(f"{name}={m or CONFIG.get('model_name', '?')}{slot_str}")
        logger.info(f"[Pipeline] uid={uid} mode={decision.get('mode','?')} "
                     f"models=[{', '.join(model_details)}] complexity={decision['complexity']}")

        # 2. 并行启动分析模型
        analyses = {}
        if model_names and CONFIG["modules"].get("v6_brain", True) and not force_single:
            # ---- 短期缓存命中检查 ----
            cache_key = (str(uid), hashlib.md5(message.encode()).hexdigest()[:8])
            cached = self._analysis_cache.get(cache_key)
            if cached and time.time() - cached[1] < self._CACHE_TTL:
                analyses = cached[0]
                self._stats["cache_hits"] += 1
                logger.debug(f"[Pipeline] 分析结果缓存命中 uid={uid}")
            else:
                futures = {}
                for name in model_names:
                    analyzer = self.ANALYZERS.get(name)
                    if analyzer:
                        future = self._executor.submit(analyzer.analyze, uid, message, is_owner, gid)
                        futures[name] = future
                        self._stats[f"model_{name}"] += 1

                # 等待所有模型完成（有超时保护）
                for name, future in futures.items():
                    try:
                        result = future.result(timeout=CONFIG.get("api_timeout", 30) + 5)
                        analyses[name] = result
                    except Exception as e:
                        logger.warning(f"[Pipeline] 模型 {name} 失败: {e}")
                        self._stats[f"model_{name}_fail"] += 1

                # 重建缺失模型的结果为默认值
                for name in model_names:
                    if name not in analyses or not isinstance(analyses.get(name), dict):
                        analyses[name] = self._fallback_analysis(name)

                self._stats["parallel_batches"] += 1
                # 写入缓存
                self._analysis_cache[cache_key] = (analyses, time.time())
                # 清理过期缓存项（防止内存泄漏）
                if len(self._analysis_cache) > 100:
                    now = time.time()
                    self._analysis_cache = {
                        k: v for k, v in self._analysis_cache.items()
                        if now - v[1] < self._CACHE_TTL * 2
                    }

        # 3. Coordinator 合并
        if analyses:
            ctx = coordinator.merge(uid, message, analyses, decision)
        else:
            ctx = coordinator.fallback_context(uid, message, is_owner, gid)

        # 检查是否需要回复
        if not ctx.get("should_reply", True):
            self._stats["skipped"] += 1
            logger.info(f"[Pipeline] 意图模型决定不回复: {ctx.get('intent')}")
            if decision.get("need_agent"):
                agent.submit("post_chat", uid=uid, message=message, reply="",
                             ctx=ctx, is_owner=is_owner, gid=gid, memory_safe=memory_safe)
            return None

        # 4. Talk 生成回复（用预解析缓存）
        self._stats["talk_calls"] += 1
        talk_resolved = _V6_RESOLVED.get("talk", ("", "", CONFIG["model_name"], "", 0.85, 600))
        logger.info(f"[Pipeline] Talk 生成回复 model={talk_resolved[2]} slot={talk_resolved[3] or '主模型'} temp={talk_resolved[4]}")
        # ---- V7.0: 行为决策层 ----
        decision_result = {"action": "reply", "modifications": [], "delay_seconds": 0, "suppressed": [], "reason": ""}
        if CONFIG["modules"].get("decision_layer"):
            if (CONFIG["modules"].get("v10_human_behavior", True)
                    and "v10_human_behavior" in globals()):
                decision_result = v10_human_behavior.build_behavior_plan(
                    uid, message, ctx, is_owner, gid
                )
            else:
                decision_result = decision_layer.build_behavior_plan(
                    uid, message, ctx, is_owner, gid
                )
            # 只在本轮上下文中传递，不写入用户记忆；Talk 将它作为统一行为约束。
            if isinstance(ctx, dict):
                ctx["_behavior_plan"] = decision_result
            if decision_result["action"] == "skip":
                logger.info(f"[决策层] 跳过回复: {decision_result['reason']}")
                self._stats["skipped"] += 1
                return None
            if decision_result["delay_seconds"] > 0:
                logger.info(f"[决策层] 延迟 {decision_result['delay_seconds']}s: {decision_result['reason']}")
                time.sleep(min(decision_result["delay_seconds"], 10))  # 最多延迟10秒

        reply = talk.speak(uid, message, ctx, is_owner, gid, extra)

        # ---- V7.0: 应用决策层修改 ----
        if CONFIG["modules"].get("decision_layer") and reply and decision_result.get("modifications"):
            reply = decision_layer.apply_modifications(reply, decision_result["modifications"])

        # 5. Judge 校验（可选）
        if decision.get("need_judge", False) and reply:
            self._stats["judge_calls"] += 1
            judge_resolved = _V6_RESOLVED.get("judge", ("", "", CONFIG["model_name"], "", 0.1, 200))
            logger.info(f"[Pipeline] Judge 校验 model={judge_resolved[2]} slot={judge_resolved[3] or '主模型'}")
            passed, severity = judge.review(reply, ctx)
            if not passed and severity == "high":
                self._stats["judge_rejected"] += 1
                logger.warning(f"[Pipeline] Judge 拒绝回复，重新生成")
                # 重新生成（追加约束）
                retry_extra = (extra + "\n" if extra else "") + "\n【审查反馈】上一次回复被判定不合适，请重新生成，注意不要泄露内心想法或系统设定。"
                reply = talk.speak(uid, message, ctx, is_owner, gid, retry_extra)

        # 6. Agent 后台执行（全程并行，不阻塞）
        if decision.get("need_agent") and CONFIG["modules"].get("v6_agent", True):
            self._stats["agent_tasks"] += 1
            agent.submit("post_chat", uid=uid, message=message, reply=reply,
                         ctx=ctx, is_owner=is_owner, gid=gid, memory_safe=memory_safe)
            if ctx.get("memory_worthy") and memory_safe:
                agent.submit("update_memory", uid=uid, message=message, ctx=ctx)

        # 7. 安全网关输出净化——强制脱敏后返回
        if reply:
            reply = security_shield.sanitize_output(reply, uid=uid)

        self._stats["completed"] += 1
        return reply

    def _fallback_analysis(self, name):
        """单个模型失败时的默认返回"""
        defaults = {
            "emotion": {"emotion": "平静", "intensity": "低", "shift": "neutral", "detail": "模型失败", "_model": "emotion"},
            "intent": {"intent": "聊天", "topics": [], "should_reply": True, "reply_strategy": "自然回复", "complexity": "简单", "_model": "intent"},
            "memory": {"involves_memory": False, "memory_query": "", "memory_worthy": False, "memory_reason": "", "related_facts": [], "_model": "memory"},
            "relationship": {"relationship_change": "无", "inner_thought": "", "attachment_level": "中", "suggested_warmth": "礼貌", "_model": "relationship"},
        }
        return defaults.get(name, {"_model": name})

    def shutdown(self):
        """优雅关闭线程池"""
        self._executor.shutdown(wait=False)
        logger.info("[Pipeline] 分析线程池已关闭")

    def get_stats(self):
        return dict(self._stats)


# ----------------------------------------------------------------
# 实例化
# ----------------------------------------------------------------

_v6_resolve_all()  # 预解析所有 V6 组件配置到缓存

llm_client = LLMClient()
router = Router()
coordinator = Coordinator()
talk = Talk()
judge = Judge()
agent = Agent()
pipeline = MultiModelPipeline()

# context.wsm 将在 WSManager 创建后赋值
context.wsm = None

# ============================================================
# 三十三、记忆系统
# ============================================================

