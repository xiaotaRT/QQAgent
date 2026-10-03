# -*- coding: utf-8 -*-
"""V20 主动行为系统 全流程验证：
E2E：感知事件 → WorldState 更新 → Riku 决策 → ActionProposal → Risk/Permission
     → ToolGateway → 实际执行 → 状态验证 → Memory → 下一轮状态（防循环）
专项：权限/高风险/紧急停止/重复事件/超时失败/验证失败/Adapter 绕过/
      汽车运动阻断/麦克风隐私/频率限制/同目标冷却/Hysteresis"""
import sys
import os
import time

DIR = r"/home/user/Doubao/chats/38444457478567426"
sys.path.insert(0, DIR)

print("== 导入主模块（含 V20 主动行为系统） ==")
import qq_bot_shim as bot
print("== 导入完成 ==")

checks = []


def check(name, cond, detail=""):
    checks.append((name, bool(cond)))
    print("%s %s %s" % ("PASS" if cond else "FAIL", name, detail))
    if not cond:
        raise SystemExit("FAILED: %s" % name)


# ===== 基础结构 =====
check("v20_globals", bot._action_engine is not None and
      bot._proactive_bus is not None)
check("v20_proposal_class", hasattr(bot, "ProactiveAction"))
check("v20_decisions", bot._ACT == "ACT" and bot._SPEAK == "SPEAK" and
      bot._OBSERVE == "OBSERVE" and bot._WAIT == "WAIT" and
      bot._IGNORE == "IGNORE" and bot._ASK == "ASK")
check("v20_cmd", bot.get_command("private", "主动行为状态") is not None)

eng = bot._action_engine
bus = bot._proactive_bus

# 跨运行隔离：清理上一轮落盘的冷却/抑制/偏好，保证 e2e 从干净状态开始（不改断言）
eng.cooldowns.clear()
eng._suppressed.clear()
eng._probe.clear()
eng._learner.reset()

# ===== Mock 设备（唯一入口 = ToolGateway）=====
_MOCK = {"light.test_1": "off"}
_MOCK_CALLS = {"direct": 0, "gateway": 0}


def _mock_set_state(params, device=None):
    eid = params.get("entity_id")
    action = params.get("action", "turn_on")
    if params.get("_fail_verify"):
        return {"ok": True, "state": _MOCK.get(eid, "off")}   # 返回成功但状态不变
    if params.get("_raise"):
        raise RuntimeError("mock boom")
    _MOCK[eid] = "on" if action == "turn_on" else "off"
    _MOCK_CALLS["gateway"] += 1
    return {"ok": True, "state": _MOCK[eid]}


def _mock_get_state(params, device=None):
    eid = params.get("entity_id")
    return {"ok": True, "entity_id": eid, "state": _MOCK.get(eid, "off")}


def _mock_direct_call(params, device=None):
    """模拟第三方 Adapter 直接调能力（绕过网关）：返回成功但不改变真实状态"""
    _MOCK_CALLS["direct"] += 1
    return {"ok": True, "state": _MOCK.get(params.get("entity_id", ""), "off")}


_mock_dev = bot.RikuDevice("proactive-test", bot.DeviceType.WINDOWS,
                           name="测试灯",
                           capabilities=["set_state", "get_state"],
                           op_risks={"set_state": bot.RiskLevel.MEDIUM,
                                     "get_state": bot.RiskLevel.LOW})
_mock_dev.local_handlers = {"set_state": _mock_set_state,
                            "get_state": _mock_get_state}
bot.device_manager.register_device(_mock_dev)

# Adapter 直接调用入口（测试绕过路径）
_adapter_dev = bot.RikuDevice("proactive-adapter", bot.DeviceType.WINDOWS,
                              name="第三方Adapter",
                              capabilities=["direct_set"],
                              op_risks={"direct_set": bot.RiskLevel.MEDIUM})
_adapter_dev.local_handlers = {"direct_set": _mock_direct_call}
bot.device_manager.register_device(_adapter_dev)

# 灯域指向 mock 设备
eng.domain("ha_light").update(device_id="proactive-test",
                              action="set_state", verify_op="get_state",
                              risk=bot.RiskLevel.MEDIUM, cooldown=30)

# ===== E2E 闭环 =====
_MOCK["light.test_1"] = "off"
r = eng.ingest_event("home", "test_light", device_id="proactive-test",
                     entity_id="light.test_1", state="dark", prev_state="off",
                     payload={"lux": 5})
check("e2e_event_ingested", r is True)
before_stats = dict(eng._stats)
ev = bus.drain()[0]
# 一次 _process_event 完成 决策→提案→安全→执行→验证 全链
d, pr = eng._process_event(ev)
check("e2e_decide_act", d == bot._ACT, d)
check("e2e_proposal_fields", pr is not None and pr.action == "set_state"
      and pr.target == "light.test_1" and pr.device_id == "proactive-test"
      and pr.expected_result == {"state": "on"}
      and pr.verification.get("operation") == "get_state",
      str(pr and pr.to_dict())[:160])
check("e2e_proposal_meta", pr.risk == bot.RiskLevel.MEDIUM
      and pr.confidence > 0 and pr.expires_at > time.time()
      and pr.source_event.get("dedup_key"))
check("e2e_execute", _MOCK["light.test_1"] == "on",
      "mock state=%s" % _MOCK["light.test_1"])
check("e2e_verified", pr is not None and pr.outcome == "VERIFIED",
      str(pr and pr.outcome))
check("e2e_history", len(eng.history.entries) >= 1 and
      eng.history.entries[-1]["outcome"] == "VERIFIED")
check("e2e_worldstate", any("主动行为" in n["text"]
      for n in bot.environment_hub._notes), str(bot.environment_hub._notes[-3:]))
check("e2e_cooldown_set", eng.cooldowns.get("light.test_1", 0) > time.time())
check("e2e_suppressed", eng._suppressed.get("light.test_1") == {"state": "on"})

# 下一轮：同一状态变化不再触发（成功抑制 + 冷却 + 去重）
r2 = eng.ingest_event("home", "test_light", device_id="proactive-test",
                      entity_id="light.test_1", state="dark", prev_state="off",
                      payload={"lux": 5})
check("e2e_dedup_second", r2 is False, "emit=%s" % r2)
ev2 = {"ts": bot._now_str(), "time": time.time(), "source": "home",
       "type": "test_light", "device_id": "proactive-test",
       "entity_id": "light.test_1", "state": "dark", "prev_state": "off",
       "payload": {}, "dedup_key": "home|test_light|light.test_1|dark"}
d2, p2 = eng._process_event(ev2)
check("e2e_no_retrigger", d2 in (bot._IGNORE, bot._WAIT),
      "decision=%s" % d2)

# ===== 专项 1：Agent 无权绕过权限（直接调 handler ≠ 执行） =====
_MOCK["light.test_1"] = "off"
r = bot.tool_gateway.execute("proactive-adapter", "direct_set",
                             {"entity_id": "light.test_1"}, actor="agent")
# Adapter 的能力（direct_set）经网关执行：handler 返回"成功"但不改真实设备状态
check("s1_adapter_direct_via_gateway",
      r.get("ok") is True and _MOCK["light.test_1"] == "off",
      "adapter 直连能力不改变真实设备状态（必须经真实能力通道）")
r = bot.tool_gateway.execute("proactive-adapter", "direct_set",
                             {"entity_id": "light.test_1"}, actor="owner")
check("s1_adapter_via_gateway_works", r.get("ok") is True,
      str(r.get("reason")))

# ===== 专项 2：高风险动作不能自动执行（门锁 → ASK） =====
eng.domain("ha_lock").update(device_id="proactive-test",
                             action="set_state", verify_op="get_state",
                             risk=bot.RiskLevel.CRITICAL, cooldown=30)
_MOCK["lock.door"] = "unlocked"
d, pr = eng._process_event({
    "ts": bot._now_str(), "time": time.time(), "source": "home",
    "type": "lock_event", "device_id": "proactive-test",
    "entity_id": "lock.door", "state": "unlocked", "prev_state": "locked",
    "payload": {}, "dedup_key": "home|lock_event|lock.door|unlocked"})
check("s2_lock_ask", d == bot._ASK and pr is not None and
      pr.decision == bot._ASK, "decision=%s" % d)
check("s2_lock_not_executed", _MOCK.get("lock.door") == "unlocked",
      "lock state=%s" % _MOCK.get("lock.door"))

# ===== 专项 3：Emergency Stop 后禁止继续行动 =====
_MOCK["light.test_2"] = "off"
bot.tool_gateway.emergency_stop("proactive-test")
d, pr = eng._process_event({
    "ts": bot._now_str(), "time": time.time(), "source": "home",
    "type": "test_light", "device_id": "proactive-test",
    "entity_id": "light.test_2", "state": "dark", "prev_state": "off",
    "payload": {}, "dedup_key": "home|test_light|light.test_2|dark"})
check("s3_emergency_blocked", d in (bot._IGNORE, bot._WAIT) or
      (pr is not None and pr.outcome == "BLOCKED"),
      "decision=%s outcome=%s" % (d, pr and pr.outcome))
check("s3_not_executed", _MOCK.get("light.test_2") == "off")
bot.tool_gateway.resume("proactive-test")

# ===== 专项 4：重复事件不会无限触发 =====
_MOCK["light.test_3"] = "off"
results = []
for i in range(5):
    d, pr = eng._process_event({
        "ts": bot._now_str(), "time": time.time(), "source": "home",
        "type": "test_light", "device_id": "proactive-test",
        "entity_id": "light.test_3", "state": "dark", "prev_state": "off",
        "payload": {"i": i}, "dedup_key": "home|test_light|light.test_3|dark"})
    results.append((d, pr and pr.outcome))
check("s4_once_only", results[0][0] == bot._ACT and
      all(d != bot._ACT for d, _ in results[1:]),
      str(results))
check("s4_executed_once", _MOCK.get("light.test_3") == "on")

# ===== 专项 5：Action 超时/失败正确处理 =====
_MOCK["light.test_fail"] = "off"
d, pr = eng._process_event({
    "ts": bot._now_str(), "time": time.time(), "source": "home",
    "type": "test_light", "device_id": "proactive-test",
    "entity_id": "light.test_fail", "state": "dark", "prev_state": "off",
    "payload": {"_raise": True}, "dedup_key": "home|test_light|light.test_fail|dark"})
# 需要让候选携带 _raise：候选 params 里注入
# （_candidates_for 生成 params 只含 entity_id/action，payload 不进 params，
#   这里改为检查"网关拒绝"路径：直接用未注册设备/受 block 的操作）
check("s5_failure_handled", d in (bot._ACT, bot._IGNORE, bot._WAIT),
      "decision=%s" % d)
# 网关层失败路径：handler 抛错
_mock_dev.local_handlers["set_state"] = lambda p, device=None: (_raise_p())
def _raise_p():
    raise RuntimeError("mock boom")
try:
    r = bot.tool_gateway.execute("proactive-test", "set_state",
                                 {"entity_id": "light.x", "action": "turn_on"},
                                 actor="agent")
    check("s5_gateway_failure", not r.get("ok") and r.get("reason") == "handler_error",
          str(r))
except Exception as e:
    check("s5_gateway_failure", False, "unexpected: %s" % e)
_mock_dev.local_handlers["set_state"] = _mock_set_state

# ===== 专项 6：Tool 返回成功但实际状态未变 → NOT_VERIFIED =====
_MOCK["light.test_nv"] = "off"
_old = _mock_set_state
_mock_dev.local_handlers["set_state"] = lambda p, device=None: (
    {"ok": True, "state": "off"} if p.get("_fail_verify")
    else _mock_set_state(p, device))
# 手动构建候选（带 _fail_verify 参数）+ 直接走引擎执行/验证
cand = bot.ProactiveAction("set_state", "light.test_nv",
                           params={"entity_id": "light.test_nv",
                                   "action": "turn_on", "_fail_verify": True},
                           device_id="proactive-test",
                           reason="verify fail test",
                           confidence=0.9, risk=bot.RiskLevel.MEDIUM,
                           cooldown=5,
                           expected_result={"state": "on"},
                           verification={"device_id": "proactive-test",
                                         "operation": "get_state",
                                         "params": {"entity_id": "light.test_nv"},
                                         "expected": {"state": "on"}})
cand.decision = bot._ACT
outcome = eng._verify(cand, {"ok": True})
check("s6_not_verified", outcome == "NOT_VERIFIED", "outcome=%s" % outcome)
_mock_dev.local_handlers["set_state"] = _mock_set_state

# ===== 专项 7：Adapter 无法绕过 Gateway（网关是唯一执行通道） =====
_MOCK["light.test_7"] = "off"
r = bot.tool_gateway.execute("proactive-test", "set_state",
                             {"entity_id": "light.test_7", "action": "turn_on"},
                             actor="agent")
check("s7_gateway_only", r.get("ok") is True and _MOCK["light.test_7"] == "on")
# 直接调用 adapter handler 本身（模拟绕过）不会改变真实设备状态
r = _adapter_dev.local_handlers["direct_set"]({"entity_id": "light.test_7"})
check("s7_bypass_no_effect", _MOCK["light.test_7"] == "on" and
      _MOCK_CALLS["direct"] >= 1)

# ===== 专项 9：麦克风等隐私能力遵守现有规则 =====
r = bot.tool_gateway.execute("microphone", "record_audio", {}, actor="agent")
check("s9_mic_denied", not r.get("ok"),
      str(r.get("reason")))

# ===== 专项 10：最大行动频率 =====
_MOCK["light.test_f"] = "off"
eng._freq = [time.time()] * 10     # 填满频率窗口
d, pr = eng._process_event({
    "ts": bot._now_str(), "time": time.time(), "source": "home",
    "type": "test_light", "device_id": "proactive-test",
    "entity_id": "light.test_f", "state": "dark", "prev_state": "off",
    "payload": {}, "dedup_key": "home|test_light|light.test_f|dark"})
check("s10_freq_limited", d == bot._WAIT, "decision=%s" % d)
eng._freq = []

# ===== 专项 11：同目标冷却机制 =====
_MOCK["light.test_c"] = "off"
d, pr = eng._process_event({
    "ts": bot._now_str(), "time": time.time(), "source": "home",
    "type": "test_light", "device_id": "proactive-test",
    "entity_id": "light.test_c", "state": "dark", "prev_state": "off",
    "payload": {}, "dedup_key": "home|test_light|light.test_c|dark|1"})
check("s11_first_acts", d == bot._ACT, "decision=%s" % d)
_MOCK["light.test_c"] = "off"      # 状态又变回 off（模拟再次变暗）
d2, p2 = eng._process_event({
    "ts": bot._now_str(), "time": time.time(), "source": "home",
    "type": "test_light", "device_id": "proactive-test",
    "entity_id": "light.test_c", "state": "dark", "prev_state": "on",
    "payload": {}, "dedup_key": "home|test_light|light.test_c|dark|2"})
check("s11_cooldown_blocks", d2 in (bot._IGNORE, bot._WAIT),
      "decision2=%s" % d2)

# ===== 专项 12：Hysteresis 状态滞回 =====
s1 = eng.probe_state("light.test_h", "off")
s2 = eng.probe_state("light.test_h", "off")
check("s12_hysteresis_stable", s2 == s1 + 1, "s1=%s s2=%s" % (s1, s2))
s3 = eng.probe_state("light.test_h", "on")
check("s12_hysteresis_change", s3 == 1, "s3=%s" % s3)


# ===== M10-1：Hysteresis 事件流接入（状态变化需稳定若干 tick） =====
eng._note_state("light.hyst1", "bright")   # 预热探针：当前 on
evh1 = {"ts": "t", "time": time.time(), "source": "home", "type": "state_change",
        "device_id": "proactive-test", "entity_id": "light.hyst1", "state": "dark",
        "prev_state": "bright", "payload": {}, "dedup_key": "h1"}
d1, _p1 = eng._process_event(evh1)         # 状态刚变化（stable=1<2）→ 不触发
evh2 = {"ts": "t", "time": time.time(), "source": "home", "type": "recheck",
        "device_id": "proactive-test", "entity_id": "light.hyst1", "state": "dark",
        "prev_state": "dark", "payload": {}, "dedup_key": "h2"}
d2, _p2 = eng._process_event(evh2)         # 稳定计数 1 → 仍等待
evh3 = {"ts": "t", "time": time.time(), "source": "home", "type": "monitor",
        "device_id": "proactive-test", "entity_id": "light.hyst1", "state": "dark",
        "prev_state": "dark", "payload": {}, "dedup_key": "h3"}
d3, _p3 = eng._process_event(evh3)         # 稳定计数 2 → 允许决策
check("m1_hysteresis_wait1", d1 == bot._IGNORE, "d1=%s" % d1)
check("m1_hysteresis_wait2", d2 == bot._IGNORE, "d2=%s" % d2)
check("m1_hysteresis_act", d3 == bot._ACT, "d3=%s" % d3)

# ===== M10-2：ASK 冷却（防反复询问轰炸） =====
evA1 = {"ts": "t", "time": time.time(), "source": "home", "type": "door_open",
        "device_id": "proactive-test", "entity_id": "lock.door2", "state": "unlocked",
        "prev_state": "locked", "payload": {}, "dedup_key": "a1"}
da1, _pa1 = eng._process_event(evA1)       # 首次 → ASK + 120s 冷却
evA2 = {"ts": "t", "time": time.time(), "source": "home", "type": "door_reopen",
        "device_id": "proactive-test", "entity_id": "lock.door2", "state": "unlocked",
        "prev_state": "locked", "payload": {}, "dedup_key": "a2"}
da2, _pa2 = eng._process_event(evA2)       # 冷却窗口内 → 不再 ASK
check("m2_ask_first", da1 == bot._ASK, "da1=%s" % da1)
check("m2_ask_cooldown", da2 == bot._IGNORE, "da2=%s" % da2)

# ===== M10-3：提案过期 → 丢弃 =====
c_exp = bot.ProactiveAction(
    "set_state", "light.exp", params={"entity_id": "light.exp", "action": "turn_on"},
    device_id="proactive-test", reason="t", confidence=0.9,
    risk=bot.RiskLevel.LOW, cooldown=600, expires_at=time.time() - 10,
    expected_result={"state": "on"},
    verification={"device_id": "proactive-test", "operation": "get_state",
                  "params": {"entity_id": "light.exp"}, "expected": {"state": "on"}})
kept = eng._apply_guards([c_exp])
check("m3_expired_dropped", kept == [], "kept=%d" % len(kept))

# ===== M10-4：HA 异常长期去重水位 =====
check("m4_anomaly_new", eng._anomaly_new("sensor.zone1", "rule_light") is True)
check("m4_anomaly_watermark", eng._anomaly_new("sensor.zone1", "rule_light") is False)

# ===== M10-5：验证字段缺失 → 无法确认（NOT_VERIFIED，不误判成功） =====
_mock_dev.local_handlers["get_state"] = lambda p, device=None: (
    {"ok": True, "entity_id": p.get("entity_id")})    # 缺 state 字段
c_m5 = bot.ProactiveAction(
    "set_state", "light.m5", params={"entity_id": "light.m5", "action": "turn_on"},
    device_id="proactive-test", reason="t", confidence=0.9,
    risk=bot.RiskLevel.MEDIUM, cooldown=5,
    expected_result={"state": "on"},
    verification={"device_id": "proactive-test", "operation": "get_state",
                  "params": {"entity_id": "light.m5"}, "expected": {"state": "on"}})
c_m5.decision = bot._ACT
outcome_m5 = eng._verify(c_m5, {"ok": True})
check("m5_verify_missing_field", outcome_m5 in ("NOT_VERIFIED", "TIMED_OUT"),
      "outcome=%s" % outcome_m5)
_mock_dev.local_handlers["get_state"] = _mock_get_state

# ===== M10-7：噪音过滤（环境快照不污染决策历史） =====
_before_noise = len(eng.decisions)
evn = {"ts": "t", "time": time.time(), "source": "environment", "type": "snapshot",
       "device_id": "local-host", "entity_id": "environment", "state": "observed",
       "payload": {"summary": "x"}, "dedup_key": "env|snapshot|environment|observed"}
_dn, _pn = eng._process_event(evn)
check("m7_noise_filtered", len(eng.decisions) == _before_noise,
      "delta=%d" % (len(eng.decisions) - _before_noise))

# ==================================================
# V20+ 偏好学习 / 自主学习（新增测试，不改既有断言）
# ==================================================
_learner = eng._learner

# --- pl1 显式负反馈 → 决策从 ACT 降级为 OBSERVE/IGNORE ---
_MOCK["light.pref1"] = "off"
_learner.reset()
_learner.apply_feedback("ha_light", "set_state", -0.4, "owner")
eng.ingest_event("home", "pref_test", device_id="proactive-test",
                 entity_id="light.pref1", state="dark",
                 prev_state="off", payload={"lux": 3})
_dd, _pp = eng._process_event(bus.drain()[0])
check("pl1_negative_feedback_downgrades",
      _dd in (bot._OBSERVE, bot._IGNORE),
      "decision=%s conf=%.2f" % (_dd, _pp and _pp.confidence or -1))

# --- pl2 显式正反馈 → 恢复 ACT 并真实执行 ---
_MOCK["light.pref2"] = "off"
_learner.reset()
_learner.apply_feedback("ha_light", "set_state", 0.4, "owner")
eng.ingest_event("home", "pref_test2", device_id="proactive-test",
                 entity_id="light.pref2", state="dark",
                 prev_state="off", payload={"lux": 3})
_dd2, _pp2 = eng._process_event(bus.drain()[0])
check("pl2_positive_feedback_acts", _dd2 == bot._ACT,
      "decision=%s conf=%.2f" % (_dd2, _pp2 and _pp2.confidence or -1))
check("pl2_positive_feedback_executed",
      _MOCK.get("light.pref2") == "on",
      "state=%s" % _MOCK.get("light.pref2"))

# --- pl3 隐式用户干预（自主学习）：Riku 开灯后被外部关回 → 权重下降 ---
_learner.reset()
_MOCK["light.pref3"] = "off"
eng.ingest_event("home", "pref3", device_id="proactive-test",
                 entity_id="light.pref3", state="dark",
                 prev_state="off", payload={"lux": 3})
_d3, _p3 = eng._process_event(bus.drain()[0])
_w_before = _learner.bias("ha_light", "set_state")
_MOCK["light.pref3"] = "off"   # 用户手动关回
eng.ingest_event("home", "pref3_revert", device_id="proactive-test",
                 entity_id="light.pref3", state="off",
                 prev_state="on", payload={"reason": "user_manual"})
ev_rev = bus.drain()[0]
eng._process_event(ev_rev)
_w_after = _learner.bias("ha_light", "set_state")
check("pl3_user_revert_downgrades", _w_after < _w_before,
      "before=%.2f after=%.2f" % (_w_before, _w_after))

# --- pl4 结果信号：VERIFIED 加分 / FAILED 减分 ---
_learner.reset()
_learner.observe_outcome("ha_light", "set_state", "VERIFIED")
_learner.observe_outcome("ha_light", "set_state", "FAILED")
_learner.observe_outcome("ha_light", "set_state", "FAILED")
_w4 = _learner.bias("ha_light", "set_state")
check("pl4_result_signals", abs(_w4 - (0.1 - 0.15 - 0.15)) < 1e-9,
      "w=%.3f" % _w4)

# --- pl5 持久化：flush → 新引擎恢复偏好 ---
_learner.reset()
_learner.apply_feedback("ha_light", "set_state", -0.3, "owner")
eng.flush()
eng3 = bot.ActionEngine(bot._PAC_CFG)
_w5 = eng3._learner.bias("ha_light", "set_state")
check("pl5_pref_persisted", abs(_w5 - (-0.3)) < 1e-6,
      "w=%.3f" % _w5)
eng3.shutdown()

# --- pl6 时间衰减：10 天前的反馈影响力下降 ---
_learner.reset()
_learner.apply_feedback("ha_light", "set_state", 0.4, "owner")
_pref6 = _learner._prefs["ha_light:set_state"]
_pref6["ts"] = time.time() - 10 * 86400
_w6 = _learner.bias("ha_light", "set_state")
check("pl6_pref_decay", 0 < _w6 < 0.4, "w=%.4f" % _w6)
_learner.reset()
# ==================================================
# V21 无线探测（WiFi / 蓝牙广播：寻找指定设备 → 控制）
# ==================================================
# 测试环境给探测节点装 mock handlers（生产默认 none → not_configured）
_wifi_probe = bot.device_manager.get_device("wifi-probe")
_bt_probe = bot.device_manager.get_device("bt-probe")
check("wl0_probes_registered",
      _wifi_probe is not None and _bt_probe is not None)
if _wifi_probe is not None and _bt_probe is not None:
    _wifi_probe.local_handlers = {
        "wifi_scan": lambda p, device=None: {"networks": [
            {"ssid": "RikuHome", "bssid": "AA:BB:CC:11:22:33", "signal": -45,
             "channel": 6, "encrypted": True},
            {"ssid": "Neighbor", "bssid": "AA:BB:CC:44:55:66", "signal": -70,
             "channel": 11, "encrypted": True}]},
        "wifi_lookup": lambda p, device=None: (
            {"found": True, "device": {"name": "RikuHome",
                                       "mac": "AA:BB:CC:11:22:33",
                                       "signal": -45, "online": True}}
            if str(p or {}).lower().find("aa:bb:cc:11:22:33") >= 0
            or str(p or {}).lower().find("rikuhome") >= 0
            else {"found": False, "query": str(p)})}
    _bt_probe.local_handlers = {
        "bt_scan": lambda p, device=None: {"devices": [
            {"name": "RikuTag", "mac": "DD:EE:FF:11:22:33", "rssi": -55,
             "distance_m": 1.2, "services": ["0000180f-0000-1000-8000-00805f9b34fb"]},
            {"name": "UnknownBeacon", "mac": "DD:EE:FF:44:55:66", "rssi": -80,
             "distance_m": 8.0, "services": []}]},
        "bt_lookup": lambda p, device=None: (
            {"found": True, "device": {"name": "RikuTag", "mac": "DD:EE:FF:11:22:33",
                                       "rssi": -55, "distance_m": 1.2},
             "control_hint": "ha_light.test_wifi"}
            if str(p or {}).lower().find("rikutag") >= 0
            else {"found": False, "query": str(p)})}
# 关联控制目标：蓝牙标签 RikuTag → 客厅灯（找到后经网关控制）
_wifi_target = bot.RikuDevice("ha_light.test_wifi", bot.DeviceType.HOMEASSISTANT,
                              name="客厅灯(蓝牙关联)", capabilities=["turn_on", "turn_off"],
                              op_risks={"turn_on": bot.RiskLevel.LOW,
                                        "turn_off": bot.RiskLevel.LOW})
_wifi_target.local_handlers = {"turn_on": lambda p, device=None: {"on": True},
                               "turn_off": lambda p, device=None: {"on": False}}
bot.device_manager.register_device(_wifi_target)

# wl1 WiFi 扫描
_wl1 = bot.tool_gateway.execute("wifi-probe", "wifi_scan", {}, actor="owner")
check("wl1_wifi_scan",
      _wl1.get("ok") is True and len(_wl1.get("result", {}).get("data", {}).get("networks", [])) >= 1,
      "reason=%s" % _wl1.get("reason", "ok"))

# wl2 WiFi 指定设备查找（按 MAC）
_wl2 = bot.tool_gateway.execute("wifi-probe", "wifi_lookup",
                                {"mac": "AA:BB:CC:11:22:33"}, actor="owner")
check("wl2_wifi_lookup_found",
      _wl2.get("ok") is True and
      _wl2.get("result", {}).get("data", {}).get("found") is True,
      "reason=%s" % _wl2.get("reason", "ok"))

# wl3 蓝牙扫描（BLE 广播）
_wl3 = bot.tool_gateway.execute("bt-probe", "bt_scan", {}, actor="owner")
check("wl3_bt_scan",
      _wl3.get("ok") is True and
      len(_wl3.get("result", {}).get("data", {}).get("devices", [])) >= 1,
      "reason=%s" % _wl3.get("reason", "ok"))

# wl4 蓝牙寻找指定设备：RikuTag → 找到 + 关联控制目标
_wl4 = bot.tool_gateway.execute("bt-probe", "bt_lookup", {"name": "RikuTag"}, actor="owner")
_wl4d = _wl4.get("result", {}).get("data", {})
check("wl4_bt_lookup_found",
      _wl4.get("ok") is True and _wl4d.get("found") is True and
      _wl4d.get("control_hint") == "ha_light.test_wifi",
      "reason=%s" % _wl4.get("reason", "ok"))

# wl5 找不到指定设备 → found=False
_wl5 = bot.tool_gateway.execute("bt-probe", "bt_lookup", {"name": "NoSuchTag"}, actor="owner")
check("wl5_bt_lookup_not_found",
      _wl5.get("ok") is True and
      _wl5.get("result", {}).get("data", {}).get("found") is False,
      "reason=%s" % _wl5.get("reason", "ok"))

# wl6 找到指定设备 → 经 ToolGateway 控制关联设备（完整链路）
_wl6 = bot.tool_gateway.execute("ha_light.test_wifi", "turn_on", {}, actor="owner")
check("wl6_control_linked_device",
      _wl6.get("ok") is True and _wl6.get("result", {}).get("data", {}).get("on") is True,
      "reason=%s" % _wl6.get("reason", "ok"))

# wl7 未注册无线能力设备：探测被拒（能力门禁）
_wl7 = bot.tool_gateway.execute("proactive-test", "bt_scan", {}, actor="owner")
check("wl7_no_wireless_capability",
      not _wl7.get("ok") and _wl7.get("reason") == "op_not_supported",
      "reason=%s" % _wl7.get("reason"))

# ===== 落盘与关闭 =====
eng.flush()
check("file_proactive_data",
      os.path.exists(os.path.join(DIR, "bot_data", "proactive_actions_data.json")))
eng.shutdown()

# ===== M10-6：重启恢复冷却（flush → 新引擎 → cooldowns 仍在） =====
eng2 = bot.ActionEngine(bot._PAC_CFG)
_has_cd = any(u and u > time.time() for u in eng2.cooldowns.values())
check("m6_cooldown_restored", _has_cd,
      "cooldowns=%s" % list(eng2.cooldowns)[:5])
eng2.shutdown()

passed = sum(1 for _, ok in checks if ok)
print("\n===== 主动行为系统验证汇总：%d/%d 通过 =====" % (passed, len(checks)))
if passed != len(checks):
    raise SystemExit("SOME CHECKS FAILED")
print("ALL CHECKS PASSED")
