"""核心基础设施层。

对外暴露：
- context: 运行时单例注册中心
- CONFIG: 全局配置字典
- DataManager / dm: 持久化
- logger: 统一日志
- utils: 通用工具函数
- audit: 审计日志
"""
from qqagent.core.context import Context, context
from qqagent.core.config import CONFIG
from qqagent.core.persistence import DataManager
from qqagent.core.logging import logger
from qqagent.core import utils
from qqagent.core import audit

# 持久化单例（import 时即初始化，DataManager 从 CONFIG 读取 data_dir）
dm = DataManager()

__all__ = [
    "Context", "context", "CONFIG", "DataManager", "dm",
    "logger", "utils", "audit",
]
