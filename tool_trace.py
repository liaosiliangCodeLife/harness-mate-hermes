'''
* This is the projet for brtc R&D Platform
* @Author Leon-liao <liaosiliang@alltman.com>
* @Description //工具调用轨迹缓冲：采集 GatewayAgent 的 tool 事件，供 reply 帧附带 agent_thoughts
* @File: tool_trace.py
* @Time: 2026/10/30 09:10:00
* @All Rights Reserve By Brtc
'''

from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Any

MAX_STEPS = 30
TTL_SECONDS = 600
MAX_OBSERVATION = 300
MAX_TOOL_INPUT = 500

_lock = threading.Lock()
_inflight: dict[str, dict] = {}
_steps: dict[str, list[dict]] = {}
_turn_thread: dict[str, str] = {}
_current_thread = ""
_current_turn = ""


def current_thread() -> str:
    """当前处理的会话线程"""
    return str(_current_thread or "")


def bind_turn(thread_id: str, turn_id: str) -> None:
    """把本轮 turn 钉到入站时的 thread，后续工具步骤按 turn 找回，不被全局 _current_thread 覆盖"""
    t = str(thread_id or "").strip()
    u = str(turn_id or "").strip()
    if not t or not u:
        return
    with _lock:
        _turn_thread[u] = t


def thread_of_turn(turn_id: str) -> str:
    """按 turn 查入站时绑定的 thread；查不到返回空串"""
    u = str(turn_id or "").strip()
    if not u:
        return ""
    with _lock:
        return str(_turn_thread.get(u) or "")


def snapshot(thread_id: str = "", turn_id: str = "") -> list:
    """取指定线程+轮次的步骤副本（不清空，供实时进度帧使用）"""
    key = _bucket_key(thread_id, turn_id)
    with _lock:
        if key in _steps:
            return list(_steps[key])
        return []


def set_thread(thread_id: str) -> None:
    """标记当前处理的会话线程，工具步骤按线程分桶，避免多轮并发互相抢走"""
    global _current_thread
    _current_thread = str(thread_id or "")


def set_turn(turn_id: str) -> None:
    """标记当前轮次（message_id），步骤按 thread|turn 双键隔离，跨轮不串"""
    global _current_turn
    _current_turn = str(turn_id or "")


def _bucket_key(thread_id: str = "", turn_id: str = "") -> str:
    """桶键：thread|turn，双键隔离，跨轮/跨会话都不串"""
    t = str(thread_id or _current_thread or "").strip()
    u = str(turn_id or _current_turn or "").strip()
    return f"{t}|{u}"


def _bucket(thread_id: str = "", turn_id: str = "") -> list:
    """取指定线程+轮次（缺省当前）的步骤桶"""
    return _steps.setdefault(_bucket_key(thread_id, turn_id), [])


def _truncate(text: str, limit: int) -> str:
    """
    * @Author Leon-liao
    * @Function:
    _truncate(text, limit)
    * @Description //按字符长度截断字符串
    * @Date :2026/10/30 09:10:00
    * @Param:
    text: 原文；limit: 最大长度
    * @return：截断后的字符串
    """
    value = text or ""
    if len(value) <= limit:
        return value
    return value[:limit]


def _serialize_tool_input(args: Any) -> str:
    """
    * @Author Leon-liao
    * @Function:
    _serialize_tool_input(args)
    * @Description //将工具入参序列化为截断后的 JSON 字符串
    * @Date :2026/10/30 09:10:00
    * @Param:
    args: 工具参数（通常是 dict）
    * @return：截断后的字符串
    """
    try:
        if isinstance(args, dict):
            raw = json.dumps(args, ensure_ascii=False, default=str)
        else:
            raw = json.dumps(args if args is not None else {}, ensure_ascii=False, default=str)
    except Exception:
        raw = str(args)
    return _truncate(raw, MAX_TOOL_INPUT)


def _serialize_observation(result: Any, is_error: bool) -> str:
    """
    * @Author Leon-liao
    * @Function:
    _serialize_observation(result, is_error)
    * @Description //将工具结果序列化为 observation，错误时加前缀
    * @Date :2026/10/30 09:10:00
    * @Param:
    result: 工具返回；is_error: 是否失败
    * @return：截断后的 observation
    """
    try:
        if isinstance(result, str):
            raw = result
        else:
            raw = json.dumps(result, ensure_ascii=False, default=str)
    except Exception:
        raw = str(result)
    if is_error:
        raw = f"错误：{raw}"
    return _truncate(raw, MAX_OBSERVATION)


def record_start(
        call_id: str,
        name: str,
        args: dict | None,
        thread_id: str = "",
        turn_id: str = "",
) -> None:
    """
    * @Author Leon-liao
    * @Function:
    record_start(call_id, name, args)
    * @Description //记录工具调用开始
    * @Date :2026/10/30 09:10:00
    * @Param:
    call_id: 调用 id；name: 工具名；args: 入参
    * @return：无
    """
    with _lock:
        _inflight[str(call_id or "")] = {
            "name": name or "tool",
            "thread": str(thread_id or _current_thread or ""),
            "turn": str(turn_id or _current_turn or ""),
            "args": args if isinstance(args, dict) else {},
            "started": time.time(),
        }


def record_complete(
        call_id: str,
        name: str,
        duration: float,
        is_error: bool,
        result: Any,
        thread_id: str = "",
        turn_id: str = "",
) -> None:
    """
    * @Author Leon-liao
    * @Function:
    record_complete(call_id, name, duration, is_error, result)
    * @Description //记录工具调用完成，写入待 drain 的步骤列表
    * @Date :2026/10/30 09:10:00
    * @Param:
    call_id: 调用 id；name: 工具名；duration: 耗时秒；is_error: 是否失败；result: 返回值
    * @return：无
    """
    key = str(call_id or "")
    with _lock:
        started_meta = _inflight.pop(key, None)
        args = {}
        # 优先入参 → inflight 快照 → 全局（避免并发会话覆盖全局后串桶）
        step_thread = str(thread_id or "").strip()
        step_turn = str(turn_id or "").strip()
        if isinstance(started_meta, dict):
            if not step_thread:
                step_thread = str(started_meta.get("thread") or "")
            if not step_turn:
                step_turn = str(started_meta.get("turn") or "")
            maybe_args = started_meta.get("args")
            if isinstance(maybe_args, dict):
                args = maybe_args
            if not name:
                name = str(started_meta.get("name") or "tool")
        if not step_thread:
            step_thread = _current_thread
        if not step_turn:
            step_turn = _current_turn
        step = {
            "id": key or str(uuid.uuid4()),
            "event": "agent_action",
            "thought": "",
            "tool": name or "tool",
            "tool_input": _serialize_tool_input(args),
            "observation": _serialize_observation(result, bool(is_error)),
            "latency": round(float(duration or 0), 2),
            "created_at": int(time.time()),
        }
        bucket = _bucket(step_thread, step_turn)
        bucket.append(step)
        overflow = len(bucket) - MAX_STEPS
        if overflow > 0:
            del bucket[:overflow]
        return step


def drain(thread_id: str = "", turn_id: str = "") -> list[dict]:
    """
    * @Author Leon-liao
    * @Function:
    drain()
    * @Description //取出并清空待上报步骤；顺带丢弃超时残留
    * @Date :2026/10/30 09:10:00
    * @Param:
    无
    * @return：步骤列表
    """
    now = int(time.time())
    cutoff = now - TTL_SECONDS
    key = _bucket_key(thread_id, turn_id)
    with _lock:
        kept = [
            item for item in _steps.pop(key, [])
            if int(item.get("created_at") or 0) >= cutoff
        ]
        # 本轮结束：顺手清掉该线程其它旧轮次，避免残留到下一轮
        t = str(thread_id or _current_thread or "").strip()
        if t:
            prefix = f"{t}|"
            for k in [k for k in _steps.keys() if k.startswith(prefix) and k != key]:
                _steps.pop(k, None)
        return kept


def reset(thread_id: str = "") -> None:
    """
    * @Function: reset(thread_id)
    * @Description //新一轮开始：丢弃上一轮残留步骤（含空桶），避免跨轮串步
    * @Param: thread_id
    * @return：无
    """
    t = str(thread_id or _current_thread or "").strip()
    with _lock:
        if t:
            prefix = f"{t}|"
            for k in [k for k in _steps.keys() if k.startswith(prefix)]:
                _steps.pop(k, None)
            for u in [u for u, v in _turn_thread.items() if v == t]:
                _turn_thread.pop(u, None)
        # 空桶是 hook 拿不到线程时的兜底，跨轮残留同样要清
        _steps.pop("", None)
        _steps.pop("|", None)


def clear() -> None:
    """
    * @Author Leon-liao
    * @Function:
    clear()
    * @Description //清空 inflight 与待上报步骤
    * @Date :2026/10/30 09:10:00
    * @Param:
    无
    * @return：无
    """
    with _lock:
        _inflight.clear()
        _steps.clear()
