# QQAgent_v20 —— 多文件源码工程

> Riku（里克）QQ Agent 单文件版已按功能域拆分为 15 个源码分片。
> **维护在多文件里改，交付/运行时用 `build_single.py` 拼回单文件**（`QQAgent_v20.py`），行为零变化。

## 目录结构

```
项目根目录/
├── qqagent_v20/               # 源码分片（维护入口）
│   ├── src_a_utils.py         # 行 1~1216    基础工具 / 审计日志 / 配置校验
│   ├── src_b_data_defense.py  # 行 1217~2154 DataManager / 命令注册 / 防御模块
│   ├── src_c_trust.py         # 行 2155~3086 黑名单 / 权限 / 信任 / 用户画像 / 情绪隔离
│   ├── src_d_relations.py     # 行 3087~4486 守护 / 关系 / 记忆权重 / 世界状态 / 礼物 / 日记
│   ├── src_e_personality.py   # 行 4487~5332 人格引擎 / 人格核心
│   ├── src_f_llm_router.py    # 行 5333~7632 LLM 客户端 / 路由 / 分析器 / 协调器 / Talk / Agent
│   ├── src_g_pipeline.py      # 行 7633~7955 MultiModelPipeline（多模型认知管道）
│   ├── src_h_behavior.py      # 行 7956~10101 记忆模块 / 行为模块群
│   ├── src_i_psych.py         # 行 10102~14419 心理科学模块群（V6.6~V6.8）
│   ├── src_j_life_v10.py      # 行 14420~19995 世界状态中心 / 生命引擎 / 决策层 / V10 真人行为核心
│   ├── src_k_memory_time.py   # 行 19996~26885 心理学核心 / 记忆网关 / 情绪引擎 / 时间感知
│   ├── src_l_scheduler.py     # 行 26886~28914 后台调度器（定时任务 / 主动检查 / 日记 / 梦境）
│   ├── src_m_reality.py       # 行 28915~32503 现实世界能力层 V13~V19（设备/网关/HA/视觉）
│   ├── src_n_proactive.py     # 行 32504~33845 V20 主动行为系统（ActionEngine/BehaviorLearner）
│   └── src_o_main.py          # 行 33846~33927 main() / __main__ 启动入口
├── build_single.py            # 多文件 → 单文件构建器（含清单与自检）
├── QQAgent_v20.py             # 构建产物（单文件，运行/测试用）
├── test_proactive.py          # 主动行为 + 偏好学习测试（62 项）
└── test_full.py               # 全功能回归（100 项）
```

## 常用修改定位（Ctrl+F 速查）

| 想改什么 | 去哪个文件 |
| --- | --- |
| 主动行为决策 / 偏好学习（BehaviorLearner / ActionEngine / ProactiveAction） | `src_n_proactive.py` |
| 设备 / 网关 / 安全链（ToolGateway / RikuDevice / RikuNode） | `src_m_reality.py` |
| 人格（PersonalityEngine / PersonalityCore / V10 行为核心 / DecisionLayer） | `src_e_personality.py`、`src_j_life_v10.py` |
| 多模型认知管道（MultiModelPipeline / LLMClient / Router / Coordinator） | `src_f_llm_router.py`、`src_g_pipeline.py` |
| 记忆 / 关系 / 情绪（MemoryModule / RelationshipManager / WorldState / 时间感知） | `src_d_relations.py`、`src_h_behavior.py`、`src_k_memory_time.py` |
| 命令注册 / 权限 / 防御（register_command / PermissionModule / DefenseModule） | `src_b_data_defense.py`、`src_c_trust.py` |
| 后台定时任务 / 调度（BackgroundScheduler / 主动检查 / 日记 / 梦境） | `src_l_scheduler.py` |
| 启动 / 优雅关闭（main / _shutdown / 信号处理） | `src_o_main.py` |

## 维护流程

1. **改代码**：用上面的表格定位到对应 `src_*.py` 修改（文件内行号对应原文连续区间，可直接 Ctrl+F 找类名/函数名）。
2. **重新生成单文件**：
   ```bash
   python3 build_single.py          # 拼回 QQAgent_v20.py（产物与现有单文件一致时会有提示）
   python3 build_single.py --check  # 校验分片能否完整还原当前单文件
   ```
3. **回归测试**：
   ```bash
   python3 test_proactive.py        # 期望 62/62 通过
   python3 test_full.py             # 期望 100/100 通过
   ```
4. **交付/部署**：把 `QQAgent_v20.py`（或整个 zip）发出去即可，单文件照常运行。

## 约定与注意

- 分片是**原文件的连续区间**，不包含 import 语句，互相之间不独立可运行——它们拼起来才是完整程序。
- `src_*.py` 的头部没有模块名注释（保持与原文件字节一致），本 README 是权威索引。
- 若手动调整了 `build_single.py` 的 `MANIFEST`（如新增分片），记得同步本 README。
- 新增代码时，**写进对应功能域的分片末尾**，不要新建"临时文件"；改完必须 `build_single.py` + 跑测试。

## 版本基线

- V20 主动行为系统：决策 ACT/SPEAK/OBSERVE/WAIT/IGNORE/ASK；唯一执行通道 ToolGateway；
  执行后真实状态验证（VERIFIED/NOT_VERIFIED/FAILED/TIMED_OUT）。
- V21 无线探测：WiFi / 蓝牙广播寻找指定设备（`wifi_scan` / `wifi_lookup` / `bt_scan` / `bt_lookup`），
  设备 `wifi-probe` / `bt-probe`；模式 none（默认 not_configured）/ mock / command；
  `beacon_map` 配置蓝牙标签 → 关联控制目标，找到设备后经 ToolGateway 控制（owner 命令「无线状态」）。
- V20+ 偏好学习 / 自主学习：`BehaviorLearner`（显式反馈 / 结果信号 / 用户干预隐式信号，
  指数时间衰减，偏好随 `proactive_actions_data.json` 持久化），owner 命令「行为偏好」。
- V22 通知接入：手机通知（SMS Forwarder）→ webhook → 主动 QQ 提醒。
   · 取餐：关键词匹配 + 取餐码提取 + 同码去重 + 频控；命令「取餐提醒」
   · 设备状态：低电量 / 充电完成 / 存储不足识别 + 每类 30 分钟去重；命令「设备状态」
   · 手机端配置见 `TAKEOUT_SETUP.md`
- V23-A 统一 Agent State：当前任务/关注目标/环境摘要/行为状态/近期事件，
  各模块共享同一份状态（防状态割裂），崩溃恢复 + 落盘。
- V23-B 完整任务系统：目标 → LLM 拆步骤 → 按优先级执行 → 逐步验证 →
  失败重试 → 替代方案 → 需小塔介入时 ASK（回复「任务 继续 <id>」放行）。
  含：同目标冷却、队列上限、单步超时、任务 deadline、暂停/恢复/取消/重试、
  紧急停止联动阻断、结果写入记忆+AgentState。
  命令：「任务」系列（开始/列表/详情/暂停/恢复/取消/继续/重试）。
- 主动行为报告联动：`_report_to_owner()`（SPEAK/ASK/ACT 结果经 QQ 私聊上报，含频控）。
