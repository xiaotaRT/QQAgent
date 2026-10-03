"""运行时上下文 / 单例注册中心。

所有运行时单例集中在此对象管理。其他模块通过：
    from qqagent.core import context
    context.tool_gateway.execute(...)
来访问，替代旧的 globals().get("tool_gateway") 全局捞取模式。

设计理由：
- 避免模块间循环 import（context 不依赖任何业务模块）
- 单例生命周期集中可见，便于 shutdown / 测试替换
- 内联打包成单文件时，context 仍是全局唯一实例
"""
from __future__ import annotations

from typing import Any, Optional


class Context:
    """运行时单例注册中心。

    字段在各模块 boot 阶段填充；未填充时为 None。
    新增单例只需在此加字段 + 在对应模块 boot 时 context.xxx = instance。
    """

    # ---- 基础设施 ----
    config: Optional[Any] = None
    dm: Optional[Any] = None          # DataManager（持久化）
    logger: Optional[Any] = None

    # ---- 模型层 ----
    llm_client: Optional[Any] = None
    pipeline: Optional[Any] = None    # MultiModelPipeline

    # ---- 记忆层 ----
    life_engine: Optional[Any] = None
    memory_store: Optional[Any] = None
    visual_memory: Optional[Any] = None

    # ---- 世界状态 ----
    world_state: Optional[Any] = None
    environment_hub: Optional[Any] = None
    bg_scheduler: Optional[Any] = None

    # ---- 工具/网关 ----
    tool_gateway: Optional[Any] = None
    device_manager: Optional[Any] = None

    # ---- 节点 ----
    worker_pool: Optional[Any] = None

    # ---- 行为层 ----
    action_engine: Optional[Any] = None
    agent_state: Optional[Any] = None
    task_manager: Optional[Any] = None
    notify_instance: Optional[Any] = None

    # ---- 通信 ----
    wsm: Optional[Any] = None          # WSManager（QQ 连接）

    # ---- 运行标志 ----
    persona_thread_alive: bool = False
    emergency_stop: bool = False

    def __init__(self) -> None:
        # 允许运行时动态挂载额外单例（Adapter 等）
        self._extra: dict[str, Any] = {}

    def set(self, name: str, value: Any) -> None:
        """动态设置额外单例（标准字段请直接赋值 context.xxx = ...）。"""
        self._extra[name] = value

    def get(self, name: str, default: Any = None) -> Any:
        """获取字段或额外单例。"""
        if hasattr(self, name):
            val = getattr(self, name)
            if val is not None:
                return val
        return self._extra.get(name, default)

    def reset(self) -> None:
        """测试用：清空所有单例引用。"""
        for field in list(self.__class__.__dict__):
            if field.startswith("_") or callable(getattr(self.__class__, field, None)):
                continue
            if isinstance(getattr(self.__class__, field, None), property):
                continue
            setattr(self, field, None if field != "persona_thread_alive" else False)
        self.emergency_stop = False
        self._extra.clear()


# 全局唯一上下文实例
context = Context()
