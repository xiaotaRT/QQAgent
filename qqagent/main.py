#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QQAgent 主入口（模块化版本）。

运行方式：
    python -m qqagent.main
    python QQAgent_v23.py  （内联打包单文件）
"""
from __future__ import annotations

import os
import sys
import signal

# ===== 触发各层模块初始化（import 即执行模块级初始化代码）=====
from qqagent.core import CONFIG, logger, dm, context
from qqagent.models.pipeline import pipeline, llm_client
import qqagent.memory.life_v10
import qqagent.memory.memory_time
import qqagent.world.scheduler
import qqagent.world.environment
import qqagent.tools.reality
import qqagent.nodes
import qqagent.behavior.trust
import qqagent.behavior.relations
import qqagent.behavior.personality
import qqagent.behavior.behavior_learn
import qqagent.behavior.psychology
import qqagent.behavior.proactive
import qqagent.behavior.agent_state
import qqagent.behavior.task
from qqagent.communication import WSManager
from qqagent import __version__

# ===== 从各层导入关键类/函数 =====
from qqagent.behavior.proactive import boot_sequence
from qqagent.behavior.task import TaskManager

# ===== 未迁移函数的占位（后续迁移到 communication.notify / core.persistence 后替换）=====
def start_notify_server():
    logger.warning("[main] 通知服务未迁移，跳过")

def stop_notify_server():
    pass

def validate_static_config():
    pass

def flush_dirty_modules():
    pass


def boot() -> None:
    """按层初始化基础设施并注册到 context。"""
    context.config = CONFIG
    context.logger = logger
    context.dm = dm
    context.llm_client = llm_client
    context.pipeline = pipeline
    logger.info("QQAgent 启动 version=%s", __version__)


def main() -> int:
    """主入口：启动序列 → 信号处理 → 任务系统 → WebSocket。"""
    boot()
    boot_sequence()

    os.makedirs(CONFIG["data_dir"], exist_ok=True)
    os.makedirs(CONFIG["log_dir"], exist_ok=True)

    def _shutdown(sig, frame):
        logger.info("\n收到退出信号，正在优雅关闭...")
        for _attr, _method in [
            ("life_engine", "stop"),
            ("bg_scheduler", "stop"),
            ("pipeline", "shutdown"),
            ("llm_client", "close"),
            ("tool_gateway", "shutdown"),
            ("device_manager", "shutdown"),
            ("_smart_home_agent", "stop_watcher"),
            ("_worker_pool", "stop"),
            ("_action_engine", "shutdown"),
            ("task_manager", "shutdown"),
        ]:
            _obj = getattr(context, _attr, None)
            if _obj is not None:
                try:
                    getattr(_obj, _method)()
                except Exception:
                    pass
        try:
            stop_notify_server()
        except Exception:
            pass
        try:
            validate_static_config()
            flush_dirty_modules()
        except Exception:
            pass
        try:
            dm.graceful_shutdown()
        except Exception:
            pass
        logger.info("再见～（尾巴轻轻晃了晃）")
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # 通知服务（取餐码/设备状态）
    if CONFIG["modules"].get("notify_webhook", True):
        try:
            start_notify_server()
        except Exception as e:
            logger.warning("取餐/设备状态通知服务启动失败: %s", e)

    # 任务系统（V23）
    if CONFIG["modules"].get("task_system", True):
        try:
            _tm = TaskManager()
            _tm.start()
            context.task_manager = _tm
            logger.info("[V23-B] 任务系统已启动（队列上限 %s，同目标冷却 %ss）",
                        _tm._queue_limit, int(_tm._cooldown_seconds))
        except Exception as e:
            logger.warning("任务系统启动失败: %s", e)

    # WebSocket 连接（NapCat）
    wsm = WSManager()
    context.wsm = wsm
    wsm.start()
    return 0


if __name__ == "__main__":
    sys.exit(main())
