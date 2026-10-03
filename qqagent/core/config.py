#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""配置（从 src_a_utils.py 迁移，凭据已脱敏为环境变量读取）。"""
import os

CONFIG = {
    # 凭据不再写入源码：可设置完整 NAPCAT_WS_URL，或单独设置 NAPCAT_ACCESS_TOKEN
    "napcat_ws_url": os.environ.get(
        "NAPCAT_WS_URL",
        "ws://127.0.0.1:3001/onebot/v11/ws",
    ),
    "napcat_access_token": os.environ.get("NAPCAT_ACCESS_TOKEN", ""),
    "bot_qq": os.environ.get("BOT_QQ", ""),
    "owner_qq": os.environ.get("OWNER_QQ", ""),

    "api_key": os.environ.get("DEEPSEEK_API_KEY", ""),
    "api_url": "https://api.deepseek.com/v1/chat/completions",
    "model_name": "deepseek-chat",
    "api_timeout": 30,
    "ai_workers": 3,

    # ===== 三模型 API 接口配置 =====
    # 三个槽位，配几个用几个，没配的自动回退到主模型
    # 只需填入 api_url + api_key + model_name
    "model_slots": {
        "model_1": {   # 模型1——分析型（快、便宜）
            "api_url": "",      # 填入 OpenAI 兼容 API 地址，空=用 CONFIG["api_url"]
            "api_key": "",      # 填入对应密钥，空=用 CONFIG["api_key"]
            "model_name": "",   # 填入模型名，空=用 CONFIG["model_name"]
        },
        "model_2": {   # 模型2——关系/执行型
            "api_url": "",
            "api_key": "",
            "model_name": "",
        },
        "model_3": {   # 模型3——核心表达型（最强）
            "api_url": "",
            "api_key": "",
            "model_name": "",
        },
        "model_4": {   # 模型4——DeepSeek V4 Flash（快速响应）
            "api_url": "https://api.deepseek.com/v1/chat/completions",
            "api_key": os.environ.get("MODEL_4_API_KEY", ""),
            "model_name": "deepseek-v4-flash",
        },
    },

    # ===== 取餐码/设备状态通知接入（V22：手机通知 → webhook → 主动提醒）=====
    # 手机端用 SMS Forwarder 把通知 POST 到本服务：
    #   POST /notify  Header: X-Notify-Secret   Body: {"title","text","package"}
    # 未配置 secret 时只监听本机回环地址；配置后监听 0.0.0.0 供局域网/公网访问。
    "notify_webhook": {
        "port": os.environ.get("NOTIFY_PORT", "8765"),
        "secret": os.environ.get("NOTIFY_SECRET", ""),
    },

    # ===== 联网搜索配置（无API key自动跳过） =====
    "web_search_config": {
        "provider": "auto",   # auto/tavily/serpapi/bing/duckduckgo
        "api_key": "",        # 搜索API密钥，空=自动检测/跳过
        "api_url": "",        # 自定义搜索API地址
        "max_results": 3,     # 最多返回几条结果
        "timeout": 10,        # 搜索超时时间（秒）
    },

    # ===== 图片识别配置（无API key自动跳过） =====
    "image_recognition_config": {
        "provider": "auto",   # auto/qwen_vl/chat_completion_vision/none
        "api_key": "",        # 视觉模型API密钥，空=尝试用主模型/跳过
        "api_url": "",        # 自定义视觉模型API地址
        "model_name": "",     # 视觉模型名，空=自动探测
        "detail": "low",      # 图片细节程度 low/high
        "timeout": 15,        # 识别超时时间（秒）
    },

    # ===== V13 现实世界能力层配置（Riku Device / Tool Gateway）=====
    # 依据【里克现实世界能力建设 TODO】核心架构（一）/ 设备统一管理（十六）/
    # Riku Node（十七）/ 安全（十八）/ 人格层整合（十九）/ 现实世界闭环（十五）
    "device_gateway_config": {
        "enabled": True,                 # 总开关：False 则整个现实世界能力层不加载
        "register_local_node": True,     # 是否注册本机为只读 Local Node
        "offline_timeout": 90,           # 设备多久无心跳视为离线（秒）
        "heartbeat_interval": 30,        # 设备心跳周期（秒）
        "confirmation_timeout": 120,     # 高风险操作等待二次确认超时（秒）
        "task_timeout": 30,              # 任务默认超时（秒）
        "task_max_retries": 2,           # 任务默认重试次数
        "task_workers": 3,               # 任务分发线程数
        "require_confirm_high_risk": True,   # 高风险操作强制二次确认
        "agent_auto_allow_low": True,        # Agent 低风险操作自动放行
    },

    # ===== Riku Node 接入配置（十七）：留空=该类型节点未接入，仅注册能力清单 =====
    "riku_node_config": {
        "token": os.environ.get("RIKU_NODE_TOKEN", "rik-node-default-token-change-me"),
        "windows": {"endpoint": ""},     # 例: http://127.0.0.1:9210 （Windows-MCP）
        "android": {"endpoint": ""},     # 例: http://192.168.1.x:9211 （Aster/mobile-mcp）
        "vision": {"endpoint": ""},      # 例: http://127.0.0.1:9212 （go2rtc/视觉模型）
        "homeassistant": {"endpoint": ""},  # 例: http://192.168.1.x:8123
    },

    # ===== V14 Windows 现实交互配置 =====
    "windows_config": {
        "mcp_endpoint": "",              # 可选 Windows-MCP 端点（留空=本地能力）
        "mcp_token": "",
        "allowed_dirs": [],              # 受限文件操作允许目录（留空=仅用户目录）
        "process_whitelist": [],         # 白名单进程
        "ps_whitelist": ["Get-Process", "Get-Service", "Get-ComputerInfo", "Get-NetIPConfiguration"],
    },

    # ===== V15 智能家居配置 =====
    "homeassistant_config": {
        "base_url": "",                  # 例: http://192.168.1.x:8123
        "token": "",
        "rooms": {},                     # 房间映射: {"客厅": ["light.living"], ...}
        "entity_names": {},              # 名称映射: {"light.living_1": "客厅主灯"}
        "anomaly_rules": [],             # 自定义异常规则（缺省有电量/温度/湿度规则）
        "anomaly_interval": 120,         # 异常监控间隔（秒）
    },

    # ===== V16 视觉系统配置 =====
    "vision_config": {
        "cameras": [],                   # [{id,name,type,url/rtsp/snapshot,device,enabled}]
        "model": {"provider": "none", "base_url": "", "model": "", "api_key": ""},
        "omniparser": {"endpoint": ""},  # OmniParser-MCP（GUI 视觉结构化）
        "capture_interval": 10,          # 轮询间隔（秒）
        "event_cooldown": 30,            # 事件节流（秒）
        "change_threshold": 0.08,        # 画面变化判定阈值
        "save_raw": False,               # 是否保存原始帧
        "raw_cap": 100,                  # 原始帧上限
    },


    # ===== V21 无线探测配置（WiFi / 蓝牙：寻找指定设备） =====
    "wireless_probe_config": {
        "wifi_source": "none",           # none / mock / command：未配置返回 wifi_not_configured
        "bt_source": "none",             # none / mock / command：未配置返回 bt_not_configured
        "wifi_command": "",              # 例: netsh wlan show networks / nmcli dev wifi list
        "bt_command": "",                # 例: bluetoothctl scan on / hcitool lescan
        "beacon_map": {                  # 蓝牙广播 → 关联控制目标（找到后经网关控制）
            # "RikuTag": "ha_light.living",
        },
    },

    # ===== V18 闲置设备 Worker 池配置 =====
    "workers_config": {
        "workers": [],                   # [{id,name,endpoint,token,kind}] 静态注册
        "idle_threshold": 300,           # 低负载持续多久判定为空闲（秒）
        "load_limit": 0.6,               # 高于此负载移出可调度池
        "poll_interval": 30,             # 心跳/空闲检测间隔（秒）
    },

    # ===== V19 人格层整合 + 安全补齐配置 =====
    "persona_config": {
        "scan_interval": 120,            # 主动行为/行为后记录巡检间隔（秒）
        "high_power_limit_watts": 1500,  # 高功率电器限制阈值
        "high_power_entities": ["switch.heater", "switch.oven", "switch.water_heater", "switch.air_conditioner"],
    },

    # ===== V20 主动行为系统配置 =====
    "proactive_config": {
        "scan_interval": 30,            # 引擎巡检间隔（秒）
        "max_auto_risk": "MEDIUM",      # 自主执行的最高风险（LOW/MEDIUM/HIGH）
        "min_confidence": 0.55,         # 置信度门槛：低于则只观察/忽略
        "default_cooldown": 600,        # 动作默认冷却（秒）
        "same_target_cooldown": 600,    # 同目标冷却（防循环，秒）
        "event_dedup_window": 30,       # 事件去重窗口（秒）
        "hysteresis_ticks": 2,          # 状态变化滞回：连续稳定 N 个 tick 才视为变化
        "max_actions_per_window": 10,   # 窗口内最大行动次数
        "window_seconds": 600,          # 频率窗口（秒）
        "ask_on_high_risk": True,       # 超出自主上限：询问 owner（否则忽略）
        "notify_on_speak": True,        # SPEAK 是否走通知出口
        "verify_timeout": 5,            # 结果验证超时（秒）
        "verify_retries": 3,            # 结果验证重试次数
        "report_to_owner": True,      # 主动行为结果是否真实报告给主人（QQ）
        "report_min_gap": 60,          # 报告最小间隔（秒）
        "report_max_daily": 100        # 报告每日上限
    },

    "memory_short_max": 15,
    "memory_short_days": 7,
    "memory_important_days": 30,
    "memory_weight_decay": 0.95,
    "memory_forgetting_threshold": 0.1,

    "flush_interval": 300,
    "heartbeat_interval": 30,
    "retry_max": 5,
    "retry_base_delay": 5,
    "retry_max_delay": 60,
    "enable_double_backup": True,

    "rate_limit_window": 10,
    "rate_limit_max": 5,

    "trust_max": 1000,
    "trust_owner": 1000,
    "trust_initial": 0,
    "trust_decay_per_day": 0.3,
    "trust_decay_min": 50,
    "trust_negative_penalty": 5,

    "relationship_stages": {
        "stranger": {"name": "陌生人", "threshold": 0, "desc": "保持距离，话很少"},
        "acquaintance": {"name": "认识", "threshold": 50, "desc": "稍微熟悉一点"},
        "familiar": {"name": "熟悉", "threshold": 150, "desc": "可以说一些日常话"},
        "friend": {"name": "朋友", "threshold": 350, "desc": "信任，话会多一些"},
        "close_friend": {"name": "亲密朋友", "threshold": 600, "desc": "很信任，偶尔主动"},
        "trusted": {"name": "信任的人", "threshold": 850, "desc": "几乎不设防"},
        "soulmate": {"name": "灵魂伴侣", "threshold": 950, "desc": "完全信任，特殊的"},
    },

    "modules": {
        "emotion": True,
        "trust": True,
        "memory": True,
        "group_memory": True,
        "profile": True,
        "blacklist": True,
        "defense": True,
        "egg": True,
        "birthday": True,
        "gift": True,
        "diary": True,
        "collection": True,
        "mutter": True,
        "guardian": True,
        "personality": True,
        "relationship": True,
        "world_state": True,
        "self_summary": True,
        "inner_monologue": True,
        "social_energy": True,
        "secret_collection": True,
        "nickname_system": True,
        "jealousy": True,
        "conflict_detection": True,
        "newbie_adaptation": True,
        "regret_mechanism": True,
        "self_contradiction": True,
        "silent_mode": True,
        "dark_diary": True,
        "pouting": True,
        "safe_distance": True,
        "late_night_mode": True,
        "observe_mode": True,
        "group_bystander": True,       # 群聊旁观插嘴
        "trigger_recall": True,
        "old_account": True,
        "rick_diary": True,
        "praise": True,
        "human_like_state": True,
        "jailbreak_defense": True,

        # ===== 新增角色深度模块 =====
        "memory_fragment": True,       # 回忆碎片
        "rapport": True,               # 默契度
        "season_awareness": True,      # 季节/节日感知
        "creative_writing": True,      # 创意写作
        "emotion_contagion": True,     # 情绪传染
        "relationship_graph": True,    # 用户关系图谱
        "rumination": True,            # 反刍思维
        "surprise_gift": True,         # 惊喜礼物
        "tone_enhancer": True,         # 语气词系统增强

        # ===== V6.6 角色深度增强模块 =====
        "habit_tracker": True,         # 习惯追踪
        "forgetting_curve": True,      # 遗忘曲线
        "speech_mirror": True,         # 镜像模仿
        "subtext_reader": True,        # 潜台词解读
        "wait_anxiety": True,          # 等待焦虑
        "social_mask": True,           # 社交面具
        "solitude": True,              # 独处时光
        "shared_memory": True,         # 共同记忆深化
        "mood_cycle": True,            # 情绪周期
        "social_radar": True,          # 社交雷达

        # ===== V6.7 心理科学模块 =====
        "zeigarnik": True,             # 蔡格尼克效应（未完成对话追踪）
        "peak_end": True,             # 峰终定律（对话记忆由峰值+结尾决定）
        "attachment": True,           # 依恋理论（不同用户不同依恋模式）
        "cognitive_dissonance": True,  # 认知失调（行为与内心矛盾）
        "maslow": True,               # 马斯洛需求层次
        "impression_mgmt": True,      # 印象管理（针对个人的面具）
        "social_exchange": True,      # 社会交换理论（情感收支）
        "emotion_regulation": True,   # 情绪调节策略
        "self_determination": True,   # 自我决定理论
        "bystander_effect": True,     # 旁观者效应
        "sleep_consolidation": True,  # 睡眠记忆巩固

        # ===== V6.8 人格核心 =====
        "personality_core": True,     # 人格核心（决策大脑）
        "inner_voice": True,            # 内心独白（增强版）
        "post_reply_rumination": True,  # 事后反刍/自我怀疑
        "memory_distortion": True,      # 记忆扭曲
        "selective_disclosure": True,   # 选择性保留/秘密
        "jealousy_enhanced": True,      # 增强版嫉妒
        "dreamscape": True,             # 梦境
        "personal_taste": True,         # 个人品味
        "nostalgia": True,              # 怀旧
        "biological_rhythm": True,      # 生理节律
        "language_fingerprint": True,   # 语言指纹演化
        "empathy_gap": True,            # 共情偏差

        # ===== V9.0 心理学核心引擎（10层架构）=====
        "psychology_core": True,         # 心理学核心引擎总开关
        "psych_cognitive": True,         # L1 认知系统（双系统思维/认知偏差/归因）
        "psych_emotion_deep": True,      # L2 深层情绪系统（情绪轮/情绪调节/共情/情感预测）
        "psych_personality": True,       # L3 人格与自我（大五/自我概念/防御机制/自卑优越）
        "psych_social": True,            # L4 社会影响（影响力原则/从众/说服/偏见）
        "psych_motivation": True,        # L5 动机与奖赏（多巴胺/SDT/驱力系统）
        "psych_clinical": True,          # L6 临床与存在（存在议题/应对策略/异常维度）
        "psych_positive": True,          # L7 积极心理（心流/品格优势/幸福感/意义建构）
        "psych_evolutionary": True,      # L8 进化心理（择偶/利他/错误管理）
        "psych_developmental": True,     # L9 发展心理（埃里克森/道德发展/成年初显期）
        "psych_inject_prompt": True,     # 是否将心理深度注入 LLM prompt
        "psych_sync_emotion": True,      # 是否与现有情绪模块双向同步

        "memory_enhancement": True,       # V9.1 记忆增强总开关（语义检索/情绪联动/统一网关等）
        "memory_semantic_search": True,   # 语义化记忆检索
        "memory_emotion_link": True,      # 情绪-记忆双向联动
        "memory_personality_bias": True,  # 人格→记忆偏好

        "unified_emotion": True,         # V10 统一情绪引擎总开关（8层架构）
        "time_perception": True,         # V11 时间感知系统总开关（8层架构）
        "ntp_sync": True,                # NTP 联网校准时钟（需要 ntp_sync_clock.py）
        "web_search": True,              # V12 联网搜索（无API key自动跳过）
        "image_recognition": True,       # V12 图片识别（无API key自动跳过）        # ===== V8.0 架构地基 + Top 10 =====
        # ===== V13 现实世界能力层（依据【里克现实世界能力建设 TODO】）=====
        # 核心架构（一）：解耦 QQ Bot 与现实世界设备能力，建立
        # Agent → Tool Gateway → Device 标准调用链，LLM 不直接持有设备权限
        "device_manager": True,          # 统一 Device Manager（十六：设备统一管理）
        "tool_gateway": True,            # 统一 Tool Gateway（一/十八：安全网关）
        "riku_node": True,               # Riku Node 节点规范与接入（十七）
        "environment_awareness": True,   # 环境感知中枢——人格层整合（十九）
        "perception_loop": True,         # 看→理解→决定→执行→再看 闭环骨架（十五）
        # ===== V14~V19 现实世界能力扩展 =====
        "windows_ops": True,             # V14 Windows 现实交互（系统/进程/窗口/屏幕/文件/PS/注册表）
        "smart_home": True,              # V15 智能家居（Home Assistant 统一控制）
        "vision_system": True,           # V16 视觉系统（摄像头/图像理解/GUI视觉/视觉记忆）
        "workers": True,                 # V18 闲置设备 Worker 池（Tiny Container）
        "persona_integration": True,     # V19 人格层整合 + 安全补齐
        "proactive_actions": True,       # V20 主动行为系统（受安全约束的自主行动闭环）
        "wireless_probe": True,          # V21 无线探测（WiFi / 蓝牙 寻找指定设备）
        "notify_webhook": True,          # V22 取餐/设备状态通知接入（手机通知 → 主动提醒）
        "task_system": True,             # V23-B 完整任务系统（目标→拆分→执行→验证→重试/调整）
        "world_state_hub": True,        # 世界状态中心
        "life_engine": True,            # 生命引擎
        "decision_layer": True,         # 行为决策层
        "v10_human_behavior": True,     # V10 统一真人行为核心
        "event_memory": True,           # 事件记忆系统
        "personality_conflict_axes": True,  # 人格矛盾系统
        "three_layer_emotion": True,    # 三层情绪结构
        "belief_system": True,          # 信念系统
        "offline_life": True,           # 离线生命模拟
        "inner_conflict": True,         # 内心冲突系统
        "open_loop": True,              # 开放循环系统
        "personality_growth": True,     # 人格成长系统
        "relationship_chapter": True,   # 关系章节系统
        "habit_formation": True,        # 习惯形成系统
        # ===== V6.0 多模型架构模块 =====
        "v6_router": True,
        "v6_brain": True,
        "v6_talk": True,
        "v6_agent": True,
        "v6_proactive": True,
        "v6_background_scheduler": True,
    },

    "data_dir": "bot_data",
    "log_dir": "logs",
    "log_max_size": 10 * 1024 * 1024,
    "log_backup_count": 5,

    "files": {
        "memory_short": "memory_short.json",
        "memory_weight": "memory_weight.json",
        "memory_profile": "memory_profile.json",
        "memory_event": "memory_event.json",
        "group_memory": "group_memory.json",
        "weather_data": "weather_data.json",
        "blacklist": "bot_blacklist.json",
        "admin": "bot_admin.json",
        "emotion": "bot_emotion.json",
        "trust": "bot_trust.json",
        "birthday": "bot_birthday.json",
        "skill": "bot_skill.json",
        "egg": "bot_egg.json",
        "greeting": "bot_greeting.txt",
        "last_active": "bot_last_active.json",
        "permanent_blacklist": "permanent_blacklist.json",
        "user_profiles": "user_profiles.json",
        "emotion_isolated": "emotion_isolated.json",
        "gift_v5_data": "gift_v5_data.json",
        "daily_diary": "daily_diary.json",
        "collection_box": "collection_box.json",
        "mutter_data": "mutter_data.json",
        "guardian_log": "guardian_log.json",
        "personality_data": "personality_data.json",
        "relationship_data": "relationship_data.json",
        "world_state_data": "world_state_data.json",
        "device_registry": "device_registry.json",
        "environment_awareness_data": "environment_awareness_data.json",
        "v10_emotion_states": "v10_emotion_states.json",       # 修复遗留：V10 引擎动态键
        "v10_emotion_regulation": "v10_emotion_regulation.json",
        "v10_empathy": "v10_empathy.json",
        "v10_mood": "v10_mood.json",
        "v10_foresight": "v10_foresight.json",
        "smart_home_state": "smart_home_state.json",          # V15 智能家居状态记忆
        "visual_memory": "visual_memory.json",                # V16 视觉记忆
        "worker_registry": "worker_registry.json",            # V18 Worker 注册表
        "proactive_actions_data": "proactive_actions_data.json",  # V20 主动行为历史/决策
        "self_summary_data": "self_summary_data.json",
        "topic_tracker_data": "topic_tracker_data.json",
        "tag_data": "tag_data.json",
        "milestone_data": "milestone_data.json",
        "gift_daily_counts": "gift_daily_counts.json",
        "gift_last_reset": "gift_last_reset.json",
        "cmd_registry": "cmd_registry.json",
        "inner_monologue_data": "inner_monologue_data.json",
        "social_energy_data": "social_energy_data.json",
        "secret_collection_data": "secret_collection_data.json",
        "nickname_data": "nickname_data.json",
        "jealousy_data": "jealousy_data.json",
        "conflict_data": "conflict_data.json",
        "newbie_data": "newbie_data.json",
        "regret_data": "regret_data.json",
        "self_contradiction_data": "self_contradiction_data.json",
        "silent_mode_data": "silent_mode_data.json",
        "dark_diary_data": "dark_diary_data.json",
        "pouting_data": "pouting_data.json",
        "safe_distance_data": "safe_distance_data.json",
        "late_night_data": "late_night_data.json",
        "observe_data": "observe_data.json",
        "bystander_data": "bystander_data.json",
        "trigger_recall_data": "trigger_recall_data.json",
        "old_account_data": "old_account_data.json",
        "rick_diary_data": "rick_diary_data.json",
        "praise_data": "praise_data.json",
        "draft_data": "draft_data.json",
        "human_like_state_data": "human_like_state_data.json",
        "jailbreak_log": "jailbreak_log.json",
        "notify_log_data": "notify_log_data.json",
        "agent_state_data": "agent_state_data.json",
        "task_manager_data": "task_manager_data.json",

        # ===== V6.0 新增数据文件 =====
        "v6_thought_log": "v6_thought_log.json",
        "v6_router_log": "v6_router_log.json",
        "v6_agent_log": "v6_agent_log.json",
        "v6_mode_settings": "v6_mode_settings.json",
        "v6_proactive_log": "v6_proactive_log.json",
        "v6_dream_data": "v6_dream_data.json",
        "v6_goals_data": "v6_goals_data.json",
        "v6_style_data": "v6_style_data.json",

        # ===== 新增模块数据文件 =====
        "memory_fragment_data": "memory_fragment_data.json",
        "rapport_data": "rapport_data.json",
        "creative_writing_data": "creative_writing_data.json",
        "emotion_contagion_data": "emotion_contagion_data.json",
        "relationship_graph_data": "relationship_graph_data.json",
        "rumination_data": "rumination_data.json",
        "surprise_gift_data": "surprise_gift_data.json",

        # ===== V6.6 新增模块数据文件 =====
        "habit_tracker_data": "habit_tracker_data.json",
        "forgetting_curve_data": "forgetting_curve_data.json",
        "speech_mirror_data": "speech_mirror_data.json",
        "subtext_reader_data": "subtext_reader_data.json",
        "wait_anxiety_data": "wait_anxiety_data.json",
        "social_mask_data": "social_mask_data.json",
        "solitude_data": "solitude_data.json",
        "shared_memory_data": "shared_memory_data.json",
        "mood_cycle_data": "mood_cycle_data.json",
        "social_radar_data": "social_radar_data.json",

        # ===== V6.7 心理科学模块数据文件 =====
        "zeigarnik_data": "zeigarnik_data.json",
        "peak_end_data": "peak_end_data.json",
        "attachment_data": "attachment_data.json",
        "cognitive_dissonance_data": "cognitive_dissonance_data.json",
        "maslow_data": "maslow_data.json",
        "impression_mgmt_data": "impression_mgmt_data.json",
        "social_exchange_data": "social_exchange_data.json",
        "emotion_regulation_data": "emotion_regulation_data.json",
        "self_determination_data": "self_determination_data.json",
        "bystander_effect_data": "bystander_effect_data.json",
        "sleep_consolidation_data": "sleep_consolidation_data.json",

        # ===== V6.8 人格核心数据文件 =====
        "personality_core_data": "personality_core_data.json",
        "inner_voice_data": "inner_voice_data.json",
        "post_reply_rumination_data": "post_reply_rumination_data.json",
        "memory_distortion_data": "memory_distortion_data.json",
        "selective_disclosure_data": "selective_disclosure_data.json",
        "jealousy_enhanced_data": "jealousy_enhanced_data.json",
        "dreamscape_data": "dreamscape_data.json",
        "personal_taste_data": "personal_taste_data.json",
        "nostalgia_data": "nostalgia_data.json",
        "biological_rhythm_data": "biological_rhythm_data.json",
        "language_fingerprint_data": "language_fingerprint_data.json",
        "empathy_gap_data": "empathy_gap_data.json",

        # ===== V8.0 架构地基 + Top 10 数据文件 =====
        "world_state_hub": "world_state_hub_data.json",
        "world_state_hub_data": "world_state_hub_data.json",
        "personality_conflict_axes": "personality_conflict_axes_data.json",
        "three_layer_emotion": "three_layer_emotion_data.json",
        "decision_layer_data": "decision_layer_data.json",
        "v10_human_behavior_data": "v10_human_behavior_data.json",
        "event_memory_data": "event_memory_data.json",
        "personality_conflict_axes_data": "personality_conflict_axes_data.json",
        "three_layer_emotion_data": "three_layer_emotion_data.json",
        "belief_system_data": "belief_system_data.json",
        "offline_life_data": "offline_life_data.json",
        "inner_conflict_data": "inner_conflict_data.json",
        "open_loop_data": "open_loop_data.json",
        "personality_growth_data": "personality_growth_data.json",
        "relationship_chapter_data": "relationship_chapter_data.json",
        "habit_formation_data": "habit_formation_data.json",
    },

    "guardian": {
        "enabled": True,
        "window_seconds": 60,
        "max_ops_per_window": 3,
        "risk_threshold_low_trust": 2,
        "risk_threshold_high_trust": 5,
        "trust_low_threshold": 200,
        "trust_high_threshold": 500,
        "auto_revoke_admin": True,
        "auto_blacklist": True,
        "notify_owner": True,
        "exempt_owner": True,
    },

    "permanent_blacklist": {
        "enabled": True,
        "auto_add_guardian_bans": True,
        "auto_add_defense_bans": False,
        "notify_owner_on_add": True,
    },

    "owner_rate_window": 60,
    "owner_warn_threshold": 10,
    "owner_angry_threshold": 20,
    "owner_angry_cooldown": 120,

    "group_rate_window": 5,
    "group_rate_max": 5,

    "emotion_isolation": {
        "enabled": True,
        "strict_mode": True,
        "debug_logging": True,
        "owner_relaxed": True,
        "test_mode_enabled": False,
    },

    "gift_system": {
        "enabled": True,
        "daily_limit_reset_hour": 0,
        "good_decay_enabled": True,
        "repeat_decay_enabled": True,
        "min_effectiveness": 0.05,
        "daily_gift_limit": 5,
        "global_cooldown": 86400,
    },

    "personality": {
        "enabled": True,
        "fatigue_decay": 0.02,
        "safety_decay": 0.01,
        "social_desire_decay": 0.015,
        "focus_decay": 0.01,
        "time_effects": True,
    },

    "social_energy": {
        "max_energy": 100,
        "consume_per_mention": 10,
        "recover_per_minute": 0.5,
    },

    "silent_mode": {
        "probability": 0.05,
        "duration_hours": 24,
    },

    "late_night": {
        "hours": [0, 1, 2, 3, 4],
        "reply_length_multiplier": 1.5,
        "openness_multiplier": 1.3,
    },

    "pouting": {
        "unmentioned_threshold": 5,
        "duration_minutes": 30,
    },

    "conflict": {
        "threshold": 5,
        "silence_minutes": 10,
    },

    "newbie": {
        "adaptation_days": 7,
        "reply_length_factor": 0.5,
    },

    "jealousy": {
        "threshold": 3,
        "cooldown_hours": 2,
    },

    "praise": {
        "owner_bonus": 5,
        "stranger_penalty": -3,
    },

    "diary": {
        "generate_hour": 22,
        "max_keywords": 5,
    },

    "human_like": {
        "enabled": True,
        "state_decay_per_hour": 0.15,
        "max_thread_messages": 8,
        "active_desire_threshold": 65,
        "active_desire_late_night_bonus": 18,
        "active_desire_private_bonus": 8,
        "active_desire_group_penalty": 18,
        "grievance_recover_per_hour": 0.8,
    },

    "jailbreak_defense": {
        "enabled": True,
        "block_threshold": 6,
        "watch_threshold": 3,
        "critical_threshold": 9,
        "owner_bypass_input": True,
        "record_owner_tests": True,
        "penalize_trust": True,
        "max_log_items": 300,
        "max_reason_preview": 80,
    },

    "security_shield": {
        "enabled": True,
        "auto_ban_threshold": 5,         # 累积违规次数 → 自动永久拉黑
        "progressive_window": 3600,      # 渐进式注入检测窗口（秒）
        "progressive_block_count": 3,    # 渐进式注入触发拦截的次数
        "max_message_length": 4000,      # 消息最大长度（超过此值才拦截）
        "long_message_warning": 2000,    # 超过此值但低于max时，需配合可疑关键词才拦截
        "sanitize_output": True,         # 是否启用输出净化
    },

    "send_greeting_on_start": True,
    "data_version": 10,
    "runtime_version": "V10",

    # ===== V6.0 多模型架构配置 =====
    "v6": {
        "default_mode": "auto",          # auto / single / dual / triple
        "router_use_llm": False,          # Router 是否使用 LLM 进行路由判断

        # ===== 三模型协同分工 =====
        # slot 指向 CONFIG["model_slots"] 的槽位，配几个用几个
        # slot 留空 = 用主模型（CONFIG 的 api_url/api_key/model_name）
        # 只配了1个槽位 → 所有组件回退到那个槽位或主模型，照常工作
        "models": {
            # 核心管道——8个组件
            "router":       {"slot": "model_4", "temperature": 0.2, "max_tokens": 400},
            "emotion":      {"slot": "model_4", "temperature": 0.2, "max_tokens": 300},
            "intent":       {"slot": "model_4", "temperature": 0.3, "max_tokens": 400},
            "memory":       {"slot": "model_4", "temperature": 0.2, "max_tokens": 400},
            "relationship": {"slot": "model_2", "temperature": 0.4, "max_tokens": 400},
            "talk":         {"slot": "model_3", "temperature": 0.85, "max_tokens": 600},
            "judge":        {"slot": "model_4", "temperature": 0.1, "max_tokens": 200},
            "agent":        {"slot": "model_2", "temperature": 0.2, "max_tokens": 800},
            # 全局功能——5个组件
            "diary":        {"slot": "model_3", "temperature": 0.7, "max_tokens": 500},
            "gift":         {"slot": "model_2", "temperature": 0.8, "max_tokens": 300},
            "personality":  {"slot": "model_4", "temperature": 0.3, "max_tokens": 400},
            "summary":      {"slot": "model_3", "temperature": 0.6, "max_tokens": 500},
            "milestone":    {"slot": "model_2", "temperature": 0.4, "max_tokens": 300},
        },
        "enable_proactive": True,         # 主动行为
        "enable_judge": True,            # Judge审查模型
        "proactive_min_interval": 120,    # 主动行为检查最小间隔（秒，2分钟）
        "proactive_max_interval": 480,    # 主动行为检查最大间隔（秒，8分钟）
        "proactive_max_daily": 140,        # 每人每天最多主动消息次数
        "proactive_min_gap": 120,         # 两次主动消息最小间隔（秒，2分钟）
        "proactive_offline_min": 2,       # 离线多久后可能主动联系（小时，下限）
        "proactive_offline_max": 8,       # 离线多久后可能主动联系（小时，上限）
        "proactive_random_prob": 0.65,    # 每次检查的随机触发概率（主人65%）
        "proactive_trust_threshold": 600, # 主动行为扩展到该信任值以上的用户（亲密朋友）
        "proactive_non_owner_max_daily": 1, # 非主人每天最多主动消息次数
        "proactive_non_owner_prob": 0.15, # 非主人每次检查触发概率（比主人低）
        "proactive_group_prob": 0.45,     # 群聊主动发言概率
        "proactive_group_max_daily": 40,  # 每群每天最多主动发言次数
        "enable_auto_diary": True,        # 自动日记
        "diary_time": "23:00",            # 日记生成时间
        "enable_dream": False,            # 梦境生成
        "dream_interval": 86400,          # 梦境间隔（秒）
        "background_agent_interval": 300, # 后台Agent执行间隔（秒）
        "long_text_threshold": 100,       # 长文本阈值
        "memory_update_threshold": 15,    # 触发记忆更新的最小消息长度
        "agent_batch_size": 5,            # Agent批量处理消息数
        "complexity_keywords": {
            "memory": ["记得", "上次", "之前", "你说过", "忘记", "想起来", "还记得", "那次"],
            "emotion": ["开心", "难过", "生气", "害怕", "担心", "委屈", "喜欢", "讨厌",
                        "想哭", "心疼", "不舒服", "烦", "累", "无聊", "兴奋", "感动"],
            "agent": ["记住", "别忘了", "记下来", "帮我记", "帮我保存"],
            "complex": ["为什么", "怎么办", "你觉得", "你认为", "帮我分析", "解释一下",
                        "什么意思", "区别", "对比", "计划", "建议"],
            "proactive": ["在吗", "你在吗", "起床", "晚安", "早安", "睡了"],
        },
    },
}


_REQUIRED_MODULE_KEYS = {
    "emotion", "trust", "memory", "group_memory", "profile", "blacklist", "defense",
    "egg", "birthday", "gift", "diary", "collection", "mutter", "guardian",
    "personality", "relationship", "world_state", "self_summary", "inner_monologue",
    "social_energy", "secret_collection", "nickname_system", "jealousy",
    "conflict_detection", "newbie_adaptation", "regret_mechanism",
    "self_contradiction", "silent_mode", "dark_diary", "pouting", "safe_distance",
    "late_night_mode", "observe_mode", "trigger_recall", "old_account",
    "rick_diary", "praise", "human_like_state", "jailbreak_defense",
    "v6_router", "v6_brain", "v6_talk", "v6_agent", "v6_proactive", "v6_background_scheduler",
    "memory_fragment", "rapport", "season_awareness", "creative_writing",
    "emotion_contagion", "relationship_graph", "rumination", "surprise_gift", "tone_enhancer",
    "habit_tracker", "forgetting_curve", "speech_mirror", "subtext_reader",
    "wait_anxiety", "social_mask", "solitude", "shared_memory", "mood_cycle", "social_radar",
    "zeigarnik", "peak_end", "attachment", "cognitive_dissonance", "maslow",
    "impression_mgmt", "social_exchange", "emotion_regulation",
    "self_determination", "bystander_effect", "sleep_consolidation",
    "personality_core",
}

_REQUIRED_FILE_KEYS = {
    "memory_short", "memory_weight", "memory_profile", "memory_event", "group_memory",
    "weather_data",
    "blacklist", "admin", "emotion", "trust", "birthday", "skill", "egg",
    "greeting", "last_active", "permanent_blacklist", "user_profiles",
    "emotion_isolated", "gift_v5_data", "daily_diary", "collection_box",
    "mutter_data", "guardian_log", "personality_data", "relationship_data",
    "world_state_data", "self_summary_data", "topic_tracker_data", "tag_data",
    "milestone_data", "gift_daily_counts", "gift_last_reset", "cmd_registry",
    "inner_monologue_data", "social_energy_data", "secret_collection_data",
    "nickname_data", "jealousy_data", "conflict_data", "newbie_data", "regret_data",
    "self_contradiction_data", "silent_mode_data", "dark_diary_data", "pouting_data",
    "safe_distance_data", "late_night_data", "observe_data", "bystander_data", "trigger_recall_data",
    "old_account_data", "rick_diary_data", "praise_data", "draft_data",
    "human_like_state_data", "jailbreak_log",
    "v6_thought_log", "v6_router_log", "v6_agent_log", "v6_mode_settings",
    "v6_proactive_log", "v6_dream_data", "v6_goals_data", "v6_style_data",
    "memory_fragment_data", "rapport_data", "creative_writing_data",
    "emotion_contagion_data", "relationship_graph_data", "rumination_data", "surprise_gift_data",
    "habit_tracker_data", "forgetting_curve_data", "speech_mirror_data", "subtext_reader_data",
    "wait_anxiety_data", "social_mask_data", "solitude_data", "shared_memory_data",
    "mood_cycle_data", "social_radar_data",
    "zeigarnik_data", "peak_end_data", "attachment_data", "cognitive_dissonance_data",
    "maslow_data", "impression_mgmt_data", "social_exchange_data", "emotion_regulation_data",
    "self_determination_data", "bystander_effect_data", "sleep_consolidation_data",
    "personality_core_data",
}


def validate_static_config(raise_on_error=True):
    missing_modules = sorted(_REQUIRED_MODULE_KEYS - set(CONFIG.get("modules", {})))
    missing_files = sorted(_REQUIRED_FILE_KEYS - set(CONFIG.get("files", {})))
    invalid_files = sorted(
        k for k, v in CONFIG.get("files", {}).items()
        if not isinstance(v, str) or not v.strip()
    )
    problems = []
    if missing_modules:
        problems.append("缺少模块开关: " + ", ".join(missing_modules))
    if missing_files:
        problems.append("缺少数据文件配置: " + ", ".join(missing_files))
    if invalid_files:
        problems.append("数据文件名无效: " + ", ".join(invalid_files))
    if problems and raise_on_error:
        raise RuntimeError("配置自检失败：" + "；".join(problems))
    return problems


validate_static_config()

# ============================================================
# 四、DataManager（增量写入版）
# ============================================================

