#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""qq_bot 兼容层（shim）。

旧测试文件通过 `import qq_bot as bot` 访问扁平命名空间。
本模块从 qqagent 包各层重新导出所有公共名称，保持向后兼容。
测试文件中改为 `import qq_bot_shim as bot`。
"""
# 触发各层初始化
from qqagent.core import *  # noqa: F401,F403
from qqagent.core import context, CONFIG, logger, dm  # noqa: F401
from qqagent.core.utils import *  # noqa: F401,F403
from qqagent.core.commands import *  # noqa: F401,F403
from qqagent.models.llm_router import *  # noqa: F401,F403
from qqagent.models.pipeline import *  # noqa: F401,F403
from qqagent.memory.life_v10 import *  # noqa: F401,F403
from qqagent.memory.memory_time import *  # noqa: F401,F403
from qqagent.world.scheduler import *  # noqa: F401,F403
from qqagent.world.environment import *  # noqa: F401,F403
from qqagent.tools.reality import *  # noqa: F401,F403
from qqagent.behavior.trust import *  # noqa: F401,F403
from qqagent.behavior.relations import *  # noqa: F401,F403
from qqagent.behavior.personality import *  # noqa: F401,F403
from qqagent.behavior.behavior_learn import *  # noqa: F401,F403
from qqagent.behavior.psychology import *  # noqa: F401,F403
from qqagent.behavior.proactive import *  # noqa: F401,F403
from qqagent.behavior.agent_state import *  # noqa: F401,F403
from qqagent.behavior.task import *  # noqa: F401,F403
from qqagent.communication.ws_manager import *  # noqa: F401,F403
import qqagent.communication.commands  # noqa: F401  触发命令注册
import qqagent.communication.windows_ops  # noqa: F401  V14 Windows 现实交互
import qqagent.communication.smart_home  # noqa: F401  V15 智能家居
import qqagent.communication.vision  # noqa: F401  V16 视觉系统
import qqagent.nodes.worker_pool  # noqa: F401  V18 WorkerPool
import qqagent.behavior.persona_context  # noqa: F401  V19 PersonaContext
try:
    from qqagent.communication.windows_ops import _HAS_WINDOWS_OPS  # noqa: F401
except ImportError:
    _HAS_WINDOWS_OPS = False  # noqa: F405
try:
    from qqagent.communication.smart_home import _smart_home_agent, _ha_client, _HA_DOMAIN_INFO  # noqa: F401
except ImportError:
    _smart_home_agent = None  # noqa: F405
    _ha_client = None  # noqa: F405
    _HA_DOMAIN_INFO = {}  # noqa: F405

# 关键单例（通过 context 访问，同时导出为模块级变量）
world_state = context.world_state  # noqa: F405
life_engine = context.life_engine  # noqa: F405
tool_gateway = context.tool_gateway  # noqa: F405
device_manager = context.device_manager  # noqa: F405
bg_scheduler = context.bg_scheduler  # noqa: F405
agent_state = context.agent_state  # noqa: F405
task_manager = context.task_manager  # noqa: F405
action_engine = context.action_engine  # noqa: F405
llm_client = context.llm_client  # noqa: F405
pipeline = context.pipeline  # noqa: F405
wsm = context.wsm  # noqa: F405
environment_hub = context.environment_hub  # noqa: F405
perception_loop = context.perception_loop  # noqa: F405

# ===== 下划线名称导出（旧测试依赖）=====
# 从 proactive 导入决策常量和配置
try:
    from qqagent.behavior.proactive import (
        _ACT, _SPEAK, _OBSERVE, _WAIT, _IGNORE, _ASK,
        _PAC_CFG, _proactive_bus,
    )
except ImportError:
    _ACT = "ACT"; _SPEAK = "SPEAK"; _OBSERVE = "OBSERVE"
    _WAIT = "WAIT"; _IGNORE = "IGNORE"; _ASK = "ASK"
    _PAC_CFG = {}; _proactive_bus = None

# 调用 boot_sequence 初始化 action_engine 等（测试期望 import 后即可用）
try:
    from qqagent.behavior.proactive import boot_sequence as _boot
    _boot()
except Exception as _boot_e:
    logger.warning("[shim] boot_sequence 跳过: %s", _boot_e)

_action_engine = context.action_engine  # noqa: F405

# 工具函数
from qqagent.core.utils import _now_str  # noqa: F401

# Agent State
_agent_state = context.agent_state  # noqa: F405

try:
    from qqagent.behavior.proactive import _NotifyHTTPServer, start_notify_server, stop_notify_server  # noqa: F401
except ImportError:
    _NotifyHTTPServer = None  # noqa: F405
    def start_notify_server(): pass
    def stop_notify_server(): pass
try:
    from qqagent.behavior.persona_context import _persona  # noqa: F401
except ImportError:
    _persona = None  # noqa: F405
try:
    from qqagent.communication.vision import _vis_router, _vis_events, _visual_memory, _vision_stop_watcher  # noqa: F401
except ImportError:
    _vis_router = None  # noqa: F405
    _vis_events = None  # noqa: F405
    _visual_memory = None  # noqa: F405
    _vision_stop_watcher = None  # noqa: F405
try:
    from qqagent.nodes.worker_pool import _worker_pool  # noqa: F401
except ImportError:
    _worker_pool = None  # noqa: F405

# 图像哈希函数（从 memory_time 导入）
try:
    from qqagent.communication.vision import _img_dhash, _dhash_distance  # noqa: F401
except ImportError:
    _img_dhash = None  # noqa: F405
    _dhash_distance = None  # noqa: F405

# ===== _DIRTY_MODULES（所有变量赋值后定义）=====
_DIRTY_MODULES = {  # noqa: F405
    "device_registry": context.device_manager,
    "environment_awareness_data": context.environment_hub,
    "gateway_audit": context.tool_gateway,
    "smart_home_state": _smart_home_agent,
    "visual_memory": _visual_memory,
    "worker_registry": _worker_pool,
}
