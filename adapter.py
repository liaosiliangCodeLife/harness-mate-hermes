'''
* This is the projet for brtc R&D Platform
* @Author Leon-liao <liaosiliang@alltman.com>
* @Description harness_mate 平台适配器，对接 Go WS 网关 /ws 端点（ws_session_id 点对点）
* @File: adapter.py
* @Time: 2026/06/30 18:30:00
* @All Rights Reserve By Brtc
'''

import asyncio
import itertools
import json
import logging
import os
import re
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import url2pathname
import hashlib

import jwt
import websockets
from websockets.exceptions import ConnectionClosed

try:
    from .file_upload import (
        OpenApiUploadError,
        extension_of,
        media_kind_for_name,
        mime_type_for_name,
        resolve_upload_url,
        upload_open_api_file,
    )
    from .approval import (
        DEFAULT_APPROVAL_TIMEOUT_MS,
        OPTION_TO_CHOICE,
        build_approval_meta,
        default_approval_options,
    )
except ImportError:  # 插件以单文件方式加载时走同目录导入
    from file_upload import (  # type: ignore
        OpenApiUploadError,
        extension_of,
        media_kind_for_name,
        mime_type_for_name,
        resolve_upload_url,
        upload_open_api_file,
    )
    from approval import (  # type: ignore
        DEFAULT_APPROVAL_TIMEOUT_MS,
        OPTION_TO_CHOICE,
        build_approval_meta,
        default_approval_options,
    )

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    ExecApprovalPrompt,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.platforms.helpers import MessageDeduplicator

logger = logging.getLogger(__name__)

_VERSION_INFO_CACHE: Optional[Dict[str, str]] = None


def _collect_version_info() -> Dict[str, str]:
    """
    * @Author Leon-liao
    * @Function: _collect_version_info()
    * @Description //收集 Hermes 与插件版本；进程内缓存，失败返回空串，不影响发消息
    * @Date :2026/11/01 23:26:39
    * @Param: 无
    * @return：含 plugin_version / hermes_version 的字典，取不到则为空串
    """
    global _VERSION_INFO_CACHE
    if _VERSION_INFO_CACHE is not None:
        return dict(_VERSION_INFO_CACHE)

    plugin_version = ""
    hermes_version = ""
    try:
        yaml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plugin.yaml")
        with open(yaml_path, "r", encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line.startswith("version:"):
                    continue
                plugin_version = line.split(":", 1)[1].strip().strip("\"'")
                break
    except Exception:
        plugin_version = ""

    try:
        from hermes_cli import __version__ as _hv  # type: ignore
        hermes_version = str(_hv or "").strip()
    except Exception:
        hermes_version = ""
    if not hermes_version:
        try:
            import importlib.metadata as _imd
            hermes_version = str(_imd.version("hermes-agent") or "").strip()
        except Exception:
            hermes_version = ""
    if not hermes_version:
        try:
            version_file = os.path.join(
                os.path.expanduser("~"),
                ".hermes",
                "hermes-agent",
                "VERSION",
            )
            with open(version_file, "r", encoding="utf-8") as fh:
                hermes_version = fh.read().strip()
        except Exception:
            hermes_version = ""

    _VERSION_INFO_CACHE = {
        "plugin_version": plugin_version or "",
        "hermes_version": hermes_version or "",
    }
    return dict(_VERSION_INFO_CACHE)


MAX_MESSAGE_LENGTH = 4000
DEDUP_MAX_SIZE = 1000
RECONNECT_MIN_DELAY = float(os.getenv("HARNESS_MATE_RECONNECT_MIN_DELAY", "1"))
RECONNECT_MAX_DELAY = float(os.getenv("HARNESS_MATE_RECONNECT_MAX_DELAY", "30"))
TYPING_THROTTLE_SECONDS = float(os.getenv("HARNESS_MATE_TYPING_INTERVAL", "8"))
HARNESS_MATE_PING_INTERVAL = float(os.getenv("HARNESS_MATE_PING_INTERVAL", "50"))
HARNESS_MATE_PING_TIMEOUT = float(os.getenv("HARNESS_MATE_PING_TIMEOUT", "90"))

# WS 网关统一点对点 WebSocket 端点（本地开发，token 由 adapter 动态拼接）
WS_GATEWAY_WS_URL = "wss://ws-agent.alltman.com:1443/ws"

# harness_mate 业务层消息类型（放在网关 data 负载内，网关本身不解析）
TYPE_MESSAGE = "message"
TYPE_APPROVAL_RESULT = "approval_result"
TYPE_REPLY = "reply"

DEFAULT_APPROVAL_TIMEOUT_SECONDS = 300
DEFAULT_API_BASE = "https://harness.alltman.com/api"
ROOM_CONTEXT_TTL_SECONDS = 600


def _cfg_get(config, key: str, default: str = "") -> str:
    """
    * @Author Leon-liao
    * @Function: _cfg_get(config, key, default)
    * @Description //从 PlatformConfig 或 dict 配置中读取字符串项
    * @Date :2026/06/30 18:30:00
    * @Param: config: 平台配置对象；key: 配置键名；default: 默认值
    * @return：配置值字符串
    """
    if isinstance(config, dict):
        value = config.get(key, default)
        return str(value) if value is not None else default
    extra = getattr(config, "extra", None) or {}
    if key in extra and extra[key]:
        return str(extra[key])
    top_level = getattr(config, key, None)
    if top_level:
        return str(top_level)
    return default


def _cfg_get_list(config, key: str, env_var: str = "") -> List[str]:
    """
    * @Author Leon-liao
    * @Function: _cfg_get_list(config, key, env_var)
    * @Description //从配置或环境变量读取逗号分隔列表
    * @Date :2026/06/30 18:30:00
    * @Param: config: 平台配置；key: extra 键名；env_var: 环境变量名
    * @return：字符串列表
    """
    extra = getattr(config, "extra", None) or {}
    raw = extra.get(key)
    if raw is None and env_var:
        raw = os.getenv(env_var, "")
    if isinstance(raw, str):
        return [item.strip() for item in raw.split(",") if item.strip()]
    if isinstance(raw, (list, tuple, set)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return []


def _entry_matches(entries: List[str], target: str) -> bool:
    """
    * @Author Leon-liao
    * @Function: _entry_matches(entries, target)
    * @Description //判断 target 是否命中白名单（支持 *）
    * @Date :2026/06/30 18:30:00
    * @Param: entries: 白名单条目；target: 待匹配值
    * @return：命中返回 True
    """
    normalized = str(target or "").strip().lower()
    for entry in entries:
        item = str(entry).strip().lower()
        if item in {"*", normalized}:
            return True
    return False


def _parse_data_field(raw_data: Any) -> Dict[str, Any]:
    """
    * @Author Leon-liao
    * @Function: _parse_data_field(raw_data)
    * @Description //解析网关消息中的 data 字段（dict 或 JSON 字符串）
    * @Date :2026/06/30 18:30:00
    * @Param: raw_data: 网关 WSMessage.data
    * @return：解析后的 dict
    """
    if isinstance(raw_data, dict):
        return raw_data
    if isinstance(raw_data, str) and raw_data.strip():
        try:
            parsed = json.loads(raw_data)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {"text": raw_data}
    return {}


def _extract_inbound_text(raw: Dict[str, Any], data: Dict[str, Any]) -> str:
    """
    * @Author Leon-liao
    * @Function: _extract_inbound_text(raw, data)
    * @Description //从网关入站消息中提取用户文本；媒体消息不把 URL 当正文
    * @Date :2026/11/06 20:45:00
    * @Param: raw: 完整 WS 消息；data: 已解析的 data 字段
    * @return：用户消息文本
    """
    kind = str(data.get("kind") or "").strip().lower()
    nested = data.get("data") if isinstance(data.get("data"), dict) else {}
    if not kind and isinstance(nested, dict):
        kind = str(nested.get("kind") or "").strip().lower()
    # 群聊媒体：kind=image/file/audio/video 时 content 是 URL，不能当话术
    if kind in {"image", "file", "audio", "video"}:
        extra = data.get("extra") if isinstance(data.get("extra"), dict) else {}
        if not extra and isinstance(nested, dict):
            extra = nested.get("extra") if isinstance(nested.get("extra"), dict) else {}
        name = str((extra or {}).get("name") or "").strip()
        return name

    for candidate in (
        data.get("text"),
        data.get("content"),
        raw.get("text"),
        raw.get("content"),
    ):
        text = str(candidate or "").strip()
        if text:
            # content 仅为 http(s) URL 且无 kind=text 时，当作媒体占位，避免 URL 当话术
            if (
                (text.startswith("http://") or text.startswith("https://"))
                and kind
                and kind != "text"
            ):
                continue
            return text
    return ""


# 一条消息最多上抛的附件数（平台单条消息实测可带 3-9 张图，防止异常 payload 打爆缓存目录）
MAX_INBOUND_FILES = 9


def _normalize_inbound_file(source: Dict[str, Any]) -> Dict[str, str]:
    """
    * @Author Leon-liao
    * @Function: _normalize_inbound_file(source)
    * @Description //把单个附件对象规范成 url/file_name/media_type/mime_type；无 url 返回空字典
    * @Date :2026/11/26 17:55:00
    * @Param: source: 平台下发的单个附件对象
    * @return：规范后的附件字典；无 url 时为空字典
    """
    url = str(source.get("url") or source.get("file_url") or "").strip()
    if not url:
        return {}
    file_name = str(source.get("file_name") or source.get("name") or "").strip()
    hinted = str(source.get("media_type") or "").strip().lower()
    if hinted in {"image", "audio", "video", "file"}:
        media_type = hinted
    else:
        media_type = media_kind_for_name(file_name or url)
    mime_type = str(source.get("mime_type") or "").strip() or mime_type_for_name(file_name or url)
    if media_type == "image" and not mime_type.startswith("image/"):
        mime_type = mime_type_for_name(file_name or url)
        if not mime_type.startswith("image/"):
            mime_type = "image/jpeg"
    return {
        "url": url,
        "file_name": file_name,
        "media_type": media_type,
        "mime_type": mime_type,
    }


def _extract_inbound_files(data: Dict[str, Any], nested: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    * @Author Leon-liao
    * @Function: _extract_inbound_files(data, nested)
    * @Description //从入站 message 中提取全部附件（一条消息可带多张图/多个文件）
     * 平台 payload.files 是数组；只取 files[0] 会让「文字 + 3 张图」只剩第一张（用户实测丢图）。
    * @Date :2026/11/26 17:55:00
    * @Param: data: 业务 data；nested: data.data
    * @return：附件列表（已规范化、去重、最多 MAX_INBOUND_FILES 个）；没有文件时为空列表
    """
    raw_items: List[Dict[str, Any]] = []
    for source in (nested, data):
        if not isinstance(source, dict):
            continue
        if str(source.get("url") or source.get("file_url") or "").strip():
            raw_items.append(source)
            break
        files = source.get("files")
        if isinstance(files, list) and files:
            raw_items.extend([item for item in files if isinstance(item, dict)])
            break
    # 群聊纯媒体：kind + content(URL)，无 url/files
    if not raw_items:
        for source in (nested, data):
            if not isinstance(source, dict):
                continue
            kind = str(source.get("kind") or "").strip().lower()
            content = str(source.get("content") or "").strip()
            if kind in {"image", "file", "audio", "video"} and (
                content.startswith("http://") or content.startswith("https://")
            ):
                extra = source.get("extra") if isinstance(source.get("extra"), dict) else {}
                raw_items.append(
                    {
                        "url": content,
                        "name": str((extra or {}).get("name") or "").strip(),
                        "media_type": kind,
                        "size": (extra or {}).get("size"),
                    }
                )
                break

    out: List[Dict[str, str]] = []
    seen = set()
    for item in raw_items:
        norm = _normalize_inbound_file(item)
        if not norm or norm["url"] in seen:
            continue
        seen.add(norm["url"])
        out.append(norm)
        if len(out) >= MAX_INBOUND_FILES:
            break
    return out


def _extract_inbound_file(data: Dict[str, Any], nested: Dict[str, Any]) -> Dict[str, str]:
    """
    * @Author Leon-liao
    * @Function: _extract_inbound_file(data, nested)
    * @Description //兼容旧调用：只返回第一个附件；新代码请用 _extract_inbound_files
    * @Date :2026/11/06 20:45:00
    * @Param: data: 业务 data；nested: data.data
    * @return：包含 url、file_name、media_type、mime_type 的字典；没有文件时为空字典
    """
    files = _extract_inbound_files(data, nested)
    return files[0] if files else {}


def _image_ext(data: bytes, fallback: str) -> str:
    """按魔数判断图片扩展名；认不出时用 fallback（cache_image_from_bytes 会再校验一次字节）。"""
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if data[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if data[:6] in {b"GIF87a", b"GIF89a"}:
        return ".gif"
    if data[:4] == b"RIFF" and len(data) >= 12 and data[8:12] == b"WEBP":
        return ".webp"
    if data[:2] == b"BM":
        return ".bmp"
    return fallback


async def _download_inbound_bytes(url: str, *, media_type: str = "document") -> bytes:
    """
    * @Author Leon-liao
    * @Function: _download_inbound_bytes(url, media_type)
    * @Description //下载平台附件到内存：SSRF 前置校验 + 重定向校验 + 与 Hermes 一致的大小上限。
     * 先按环境代理（http_proxy）下载，代理未开/连不上时改直连重试一次 —— COS 是国内服务，
     * 代理挂了不能连带把附件读图也废掉。
    * @Date :2026/11/09 23:30:00
    * @Param: url: 附件地址；media_type: image/audio/video/document（仅用于错误文案与限额口径）
    * @return：附件字节
    """
    try:
        return await _download_once(url, media_type=media_type, trust_env=True)
    except Exception as exc:
        logger.info("harness_mate 附件下载失败（已试代理），改直连重试: url=%s err=%s", url, exc)
        return await _download_once(url, media_type=media_type, trust_env=False)


async def _download_once(url: str, *, media_type: str, trust_env: bool) -> bytes:
    """单次下载：SSRF 前置校验 + 重定向校验 + 与 Hermes 一致的大小上限。"""
    from gateway.platforms.base import (
        get_inbound_media_max_bytes,
        ssrf_redirect_guard,
        validate_inbound_media_size,
    )
    from tools.url_safety import create_ssrf_safe_async_client, is_safe_url

    if not is_safe_url(url):
        raise ValueError(f"Blocked unsafe URL (SSRF protection): {url}")
    limit = get_inbound_media_max_bytes()
    headers = {"User-Agent": "Mozilla/5.0 (compatible; HermesAgent/1.0)", "Accept": "*/*"}
    async with create_ssrf_safe_async_client(
        timeout=30.0,
        follow_redirects=True,
        trust_env=trust_env,
        event_hooks={"response": [ssrf_redirect_guard]},
    ) as client:
        async with client.stream("GET", url, headers=headers) as response:
            response.raise_for_status()
            declared = response.headers.get("content-length")
            if declared:
                try:
                    validate_inbound_media_size(int(declared), media_type=media_type, max_bytes=limit)
                except ValueError:
                    raise
                except Exception:
                    pass
            chunks: List[bytes] = []
            total = 0
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                validate_inbound_media_size(total, media_type=media_type, max_bytes=limit)
                chunks.append(chunk)
    return b"".join(chunks)


async def materialize_inbound_file(inbound_file: Dict[str, str]) -> str:
    """
    * @Author Leon-liao
    * @Function: materialize_inbound_file(inbound_file)
    * @Description //把入站附件 URL 落地成本机绝对路径。
     * Hermes 准入层（gateway/session_ingress_media.py 的 _open_regular）只接受「本机绝对路径的常规文件」，
     * 直接把 http(s) URL 放进 MessageEvent.media_urls 会抛 RuntimeStoreError('invalid_params')，
     * 并且整条消息（连正文文字）一起被丢弃 —— 所以必须先下载落地再上抛。
    * @Date :2026/11/09 23:30:00
    * @Param: inbound_file: _extract_inbound_file 的返回值
    * @return：本地绝对路径；无法落地时返回空字符串（调用方降级为纯文本，URL 已在正文里）
    """
    url = str(inbound_file.get("url") or "").strip()
    if not url:
        return ""
    # 平台若已经推的是本机路径，直接复用
    if url.startswith("/") and os.path.isfile(url):
        return url
    if not url.startswith(("http://", "https://")):
        return ""
    media_type = str(inbound_file.get("media_type") or "file").strip().lower()
    file_name = str(inbound_file.get("file_name") or "").strip()
    suffix = f".{extension_of(file_name or url)}" if extension_of(file_name or url) else ""
    try:
        from gateway.platforms.base import (
            cache_audio_from_bytes_async,
            cache_document_from_bytes_async,
            cache_image_from_bytes_async,
            cache_video_from_bytes_async,
        )

        data = await _download_inbound_bytes(url, media_type=media_type)
        if media_type == "image":
            return await cache_image_from_bytes_async(data, _image_ext(data, suffix or ".jpg"))
        if media_type == "audio":
            return await cache_audio_from_bytes_async(data, suffix or ".ogg")
        if media_type == "video":
            return await cache_video_from_bytes_async(data, suffix or ".mp4")
        return await cache_document_from_bytes_async(
            data, file_name or f"document{suffix or '.bin'}"
        )
    except Exception as exc:
        logger.warning(
            "harness_mate 附件落地失败，降级为纯文本: url=%s err=%s", url, exc
        )
        return ""


# 实时进度推送用：当前 adapter 实例与其事件循环（在入站处理时登记）
_HM_INSTANCE: Any = None
_HM_LOOP: Any = None

# ---------------------------------------------------------------------------
# 运行态心跳：每 30s 向平台刷一帧 running，api 侧 Redis TTL 60s 续期。
# 只表达「在不在干活」，不带工具名/参数/结果；结束帧仍由回复链路照常发。
# ---------------------------------------------------------------------------
_HM_HEARTBEAT_INTERVAL_SECONDS = 30.0
_HM_ACTIVE_RUNS: Dict[str, Dict[str, Any]] = {}
_HM_ACTIVE_RUNS_LOCK = threading.Lock()
_HM_HEARTBEAT_THREAD: Optional[threading.Thread] = None


def _hm_run_key(account_id: str, thread_id: str) -> str:
    """活跃 turn 的登记键：一个会话只有一条心跳"""
    return f"{account_id}:{thread_id}"


def _hm_mark_running(account_id: str, thread_id: str, peer_session: str) -> None:
    """
    * @Author Leon-liao
    * @Function: _hm_mark_running(account_id, thread_id, peer_session)
    * @Description //入站消息开始处理时登记活跃 turn，并立刻补一帧 running（不必等 30s）
    * @Date :2026/11/26 18:40:00
    * @Param: account_id 账号；thread_id 会话；peer_session 回包目标
    * @return：无
    """
    account = str(account_id or "").strip()
    thread = str(thread_id or "").strip()
    if not account or not thread:
        return
    with _HM_ACTIVE_RUNS_LOCK:
        _HM_ACTIVE_RUNS[_hm_run_key(account, thread)] = {
            "account_id": account,
            "thread_id": thread,
            "peer": str(peer_session or "").strip(),
        }
    logger.info("hm run start: acct=%s tid=%s peer=%s", account, thread, peer_session)
    _hm_ensure_heartbeat_thread()
    _hm_heartbeat_once()


def _hm_mark_idle(account_id: str, thread_id: str) -> None:
    """
    * @Author Leon-liao
    * @Function: _hm_mark_idle(account_id, thread_id)
    * @Description //本轮结束先摘活跃登记，再让调用方发 done；顺序反了会被心跳重新点亮状态
    * @Date :2026/11/26 18:40:00
    * @Param: account_id 账号；thread_id 会话
    * @return：无
    """
    account = str(account_id or "").strip()
    thread = str(thread_id or "").strip()
    if not account or not thread:
        return
    with _HM_ACTIVE_RUNS_LOCK:
        _HM_ACTIVE_RUNS.pop(_hm_run_key(account, thread), None)


def _hm_clear_active_runs() -> None:
    """WS 断开时清空：重连后回包目标不确定，状态交给 api 侧 TTL 自然过期"""
    with _HM_ACTIVE_RUNS_LOCK:
        _HM_ACTIVE_RUNS.clear()


def _hm_heartbeat_once() -> None:
    """把当前所有活跃 turn 各发一帧 running（空正文，不带工具事件）"""
    inst = _HM_INSTANCE
    loop = _HM_LOOP
    if inst is None or loop is None or getattr(loop, "is_closed", lambda: True)():
        return
    with _HM_ACTIVE_RUNS_LOCK:
        runs = list(_HM_ACTIVE_RUNS.values())
    if runs:
        logger.info("hm heartbeat tick: runs=%d", len(runs))
    for run in runs:
        try:
            payload: Dict[str, Any] = {
                "type": TYPE_REPLY,
                "account_id": run["account_id"],
                "thread_id": run["thread_id"],
                "bot_id": getattr(inst, "bot_id", ""),
                "data": {
                    "text": "",
                    "message_id": "",
                    "status": "running",
                    "heartbeat": True,
                    "done": False,
                },
            }
            try:
                payload.update(inst._room_fields(run["thread_id"]))
            except Exception:
                pass
            asyncio.run_coroutine_threadsafe(inst._send_ws(run["peer"], payload), loop)
        except Exception:
            logger.debug("hm 心跳发送失败", exc_info=True)


def _hm_heartbeat_loop() -> None:
    """守护线程：每 30s 刷一次；没有活跃 turn 时什么都不发"""
    while True:
        time.sleep(_HM_HEARTBEAT_INTERVAL_SECONDS)
        try:
            _hm_heartbeat_once()
        except Exception:
            logger.debug("hm 心跳循环异常", exc_info=True)


def _hm_ensure_heartbeat_thread() -> None:
    """首次登记时起守护线程（daemon，随进程退出，不做 join）"""
    global _HM_HEARTBEAT_THREAD
    if _HM_HEARTBEAT_THREAD is not None and _HM_HEARTBEAT_THREAD.is_alive():
        return
    _HM_HEARTBEAT_THREAD = threading.Thread(
        target=_hm_heartbeat_loop, name="hm-heartbeat", daemon=True
    )
    _HM_HEARTBEAT_THREAD.start()



def _push_tool_progress(step: Dict[str, Any], turn_id: str = "", thread_id: str = "") -> None:
    """工具执行完一步就推一帧状态帧（带当前线程的步骤快照），供客户端实时显示执行过程"""
    inst = _HM_INSTANCE
    loop = _HM_LOOP
    if not inst or loop is None or not isinstance(step, dict):
        logger.info("hm progress skip: inst=%s loop=%s step=%s", bool(inst), loop is not None, isinstance(step, dict))
        return
    try:
        from . import tool_trace as _tt
        tid = str(thread_id or "").strip() or _tt.current_thread()
        if not tid:
            logger.info("hm progress skip: empty thread")
            return
        target = None
        for (acct, t), peer in list(getattr(inst, "_reply_targets", {}).items()):
            if str(t) == tid and peer:
                target = (str(acct), str(t), str(peer))
                break
        if not target:
            logger.info("hm progress skip: no target tid=%s keys=%s", tid, list(getattr(inst, "_reply_targets", {}).keys())[:5])
            return
        acct, thread_id, peer = target
        turn_id = str(turn_id or "").strip()
        snap = _tt.snapshot(thread_id, turn_id)
        if not snap:
            logger.info("hm progress skip: empty snapshot tid=%s", thread_id)
            return
        # 本轮已结束（done 后已清 mapping）：不再发孤儿进度帧
        if turn_id:
            if turn_id not in inst._msg_by_turn:
                logger.info("hm progress skip: turn closed tid=%s turn=%s", tid, turn_id)
                return
        elif not inst._turn_msg_id.get(tid):
            logger.info("hm progress skip: turn closed tid=%s", tid)
            return
        # 归属用本轮回复的消息 ID；取不到才回退老的 prog- 前缀（兼容未升级路径）
        message_id = (
            inst._msg_by_turn.get(turn_id)
            or inst._turn_message_id(thread_id)
            or ("prog-" + thread_id)
        )
        data = {
            "text": "",
            "message_id": message_id,
            "status": "thinking",
            "agent_thoughts": snap,
            "tool_events": snap,
            "done": False,
        }
        payload: Dict[str, Any] = {
            "type": TYPE_REPLY,
            "account_id": acct,
            "thread_id": thread_id,
            "bot_id": inst.bot_id,
            "data": data,
        }
        try:
            payload.update(inst._room_fields(thread_id))
        except Exception:
            pass

        async def _fire() -> None:
            await inst._send_ws(peer, payload)

        asyncio.run_coroutine_threadsafe(_fire(), loop)
        logger.info("hm progress sent: tid=%s steps=%d peer=%s", thread_id, len(snap), peer)
    except Exception:
        logger.warning("harness_mate 推送工具进度失败", exc_info=True)


class HarnessMateAdapter(BasePlatformAdapter):
    """Go WS 网关 Hermes 端平台适配器。"""

    PLATFORM = "harness_mate"
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    SUPPORTS_MESSAGE_EDITING = True

    def __init__(self, config):
        """
        * @Author Leon-liao
        * @Function: HarnessMateAdapter.__init__(config)
        * @Description //初始化 harness_mate 适配器，读取 bot_id（ws_session_id）与 bot_key（JWT 鉴权）
        * @Date :2026/07/02 12:00:00
        * @Param: config: 平台配置，需包含 bot_id、bot_key
        """
        platform = Platform("harness_mate")
        super().__init__(config=config, platform=platform)
        self.bot_id = (
            _cfg_get(config, "bot_id", "")
            or _cfg_get(config, "ws_session_id", "")
            or os.getenv("HARNESS_MATE_BOT_ID", "").strip()
            or os.getenv("HARNESS_MATE_WS_SESSION_ID", "").strip()
        ).strip()
        self.bot_key = (
            _cfg_get(config, "bot_key", "")
            or _cfg_get(config, "jwt_secret", "")
            or os.getenv("HARNESS_MATE_BOT_KEY", "").strip()
            or os.getenv("HARNESS_MATE_JWT_SECRET", "").strip()
        ).strip()
        self.ws_session_id = self.bot_id
        self.gateway_url = WS_GATEWAY_WS_URL.rstrip("/")
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._read_task: Optional[asyncio.Task] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._should_reconnect = False
        self._reconnect_delay = RECONNECT_MIN_DELAY
        self._send_lock = asyncio.Lock()
        self._dedup = MessageDeduplicator(max_size=DEDUP_MAX_SIZE)
        self._reply_targets: Dict[Tuple[str, str], str] = {}
        self._peer_by_device: Dict[str, str] = {}
        self._last_peer_session: str = ""
        # 最近一次入站上下文（含房间），仅在 metadata 缺 thread 时作房间归位；靠 TTL 与新入站覆盖
        self._last_inbound_by_chat: Dict[str, Dict[str, Any]] = {}
        # P3：房间会话键 room:<room_id> → {room_id, reply_to}
        self._room_by_thread: Dict[str, Dict[str, str]] = {}
        self._reply_fingerprints: Dict[str, str] = {}
        # 单聊流式已发送全文前缀（thread:message_id → 累计正文），用于算增量后缀
        self._stream_sent: Dict[str, str] = {}
        # 本轮回复的消息 ID：进度帧 / 正文帧 / done 帧共用，保证服务端按 message_id 归并成一条
        self._turn_msg_id: Dict[str, str] = {}
        # turn_id → 本轮消息 ID（Hermes 每轮唯一，天然按轮隔离）
        self._msg_by_turn: Dict[str, str] = {}
        # thread_id → 最近一次工具 hook 绑定到的 turn_id（正文帧用它找本轮 message_id）
        self._active_turn_by_thread: Dict[str, str] = {}
        self._typing_last_sent: Dict[Tuple[str, str], float] = {}
        self._typing_lock = asyncio.Lock()
        # 审批：approval_id → {session_key, chat_id, thread_id, peer, timer}
        self._approval_counter = itertools.count(1)
        self._approval_state: Dict[str, Dict[str, Any]] = {}
        self.upload_url = resolve_upload_url(_cfg_get(config, "upload_url", ""))
        self.api_base = (
            _cfg_get(config, "api_base", "")
            or os.getenv("HARNESS_MATE_API_BASE", "").strip()
            or DEFAULT_API_BASE
        ).rstrip("/")

        extra = getattr(config, "extra", None) or {}
        self._dm_policy = str(
            extra.get("dm_policy") or os.getenv("HARNESS_MATE_DM_POLICY", "open")
        ).strip().lower()
        self._allow_from = _cfg_get_list(
            config, "allow_from", "HARNESS_MATE_ALLOWED_PEERS"
        )
        if not self._allow_from:
            self._allow_from = _cfg_get_list(
                config, "allow_from", "HARNESS_MATE_ALLOWED_DEVICES"
            )

    def _is_peer_allowed(self, peer_session_id: str) -> bool:
        """
        * @Author Leon-liao
        * @Function: _is_peer_allowed(peer_session_id)
        * @Description //判断对端 ws_session_id 是否允许向 Hermes 发消息
        * @Date :2026/07/02 10:00:00
        * @Param: peer_session_id: str，对端 ws_session_id
        * @return：允许返回 True
        """
        if self._dm_policy == "disabled":
            return False
        if self._dm_policy == "open" or os.getenv(
            "HARNESS_MATE_ALLOW_ALL_DEVICES", ""
        ).lower() in {"1", "true", "yes", "on"}:
            return True
        if not self._allow_from:
            return self._dm_policy != "allowlist"
        return _entry_matches(self._allow_from, peer_session_id)

    def _build_session_token(self) -> str:
        """
        * @Author Leon-liao
        * @Function: _build_session_token()
        * @Description //生成 ws_gateway 握手 JWT（仅含 ws_session_id）
        * @Date :2026/07/02 10:00:00
        * @Param: 无
        * @return：JWT 字符串
        """
        now = int(time.time())
        return jwt.encode(
            {
                "ws_session_id": self.ws_session_id,
                "exp": now + 86400 * 30,
                "iat": now,
            },
            self.bot_key,
            algorithm="HS256",
        )

    def _build_connect_url(self) -> str:
        """
        * @Author Leon-liao
        * @Function: _build_connect_url()
        * @Description //拼接带 token 的 WebSocket 连接 URL
        * @Date :2026/07/02 10:00:00
        * @Param: 无
        * @return：完整 ws://.../ws?token=... URL
        """
        token = self._build_session_token()
        return f"{self.gateway_url}?token={token}"

    async def connect(self, is_reconnect: bool = False, **kwargs) -> bool:
        """
        * @Author Leon-liao
        * @Function: connect(is_reconnect, **kwargs)
        * @Description //连接 ws_gateway /ws 并启动读循环；重连由 Gateway 调度
        * @Date :2026/07/09 10:20:00
        * @Param: is_reconnect: bool，Gateway 重连时为 True；kwargs: 兼容未来扩展参数
        * @return：连接成功返回 True，否则 False
        """
        if not self.bot_id or not self.bot_key:
            message = "harness_mate 缺少 bot_id 或 bot_key"
            self._set_fatal_error("harness_mate_missing_config", message, retryable=False)
            logger.error(message)
            return False

        self._should_reconnect = True
        self._reconnect_delay = RECONNECT_MIN_DELAY
        if await self._establish_connection():
            logger.info(
                "harness_mate 已连接 WS 网关: bot_id=%s%s",
                self.bot_id,
                " (reconnect)" if is_reconnect else "",
            )
            return True

        if is_reconnect:
            message = "harness_mate 重连 WS 网关失败，等待 Gateway 下次重试"
        else:
            message = "harness_mate 首次连接 WS 网关失败，等待 Gateway 重连调度"
        self._set_fatal_error("harness_mate_connect_error", message, retryable=True)
        logger.warning(message)
        self._mark_disconnected()
        return False

    async def _establish_connection(self) -> bool:
        """
        * @Author Leon-liao
        * @Function: _establish_connection()
        * @Description //建立 WebSocket 连接并启动读循环
        * @Date :2026/07/01 16:00:00
        * @Param: 无
        * @return：连接成功返回 True
        """
        try:
            url = self._build_connect_url()
            self._ws = await websockets.connect(
                url,
                ping_interval=HARNESS_MATE_PING_INTERVAL,
                ping_timeout=HARNESS_MATE_PING_TIMEOUT,
                close_timeout=5,
                max_size=1024 * 1024,
            )
            self._read_task = asyncio.create_task(self._read_loop())
            self._mark_connected()
            return True
        except Exception as exc:
            logger.warning("harness_mate 连接 WS 网关失败: %s", exc)
            await self._cleanup_ws()
            return False

    async def _handle_connection_lost(self) -> None:
        """
        * @Author Leon-liao
        * @Function: _handle_connection_lost()
        * @Description //连接断开后标记状态并启动后台重连任务
        * @Date :2026/07/01 16:00:00
        * @Param: 无
        """
        self._mark_disconnected()
        if not self._should_reconnect:
            return
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        """
        * @Author Leon-liao
        * @Function: _reconnect_loop()
        * @Description //指数退避重连 WS 网关，直到成功或主动 disconnect
        * @Date :2026/07/01 16:00:00
        * @Param: 无
        """
        delay = self._reconnect_delay
        while self._should_reconnect:
            logger.warning(
                "harness_mate Gateway 断线，%.1fs 后重连 (bot_id=%s)...",
                delay,
                self.bot_id,
            )
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                return
            if not self._should_reconnect:
                return
            if await self._establish_connection():
                self._reconnect_delay = RECONNECT_MIN_DELAY
                logger.info(
                    "harness_mate 已重连 WS 网关: bot_id=%s",
                    self.bot_id,
                )
                return
            delay = min(delay * 2, RECONNECT_MAX_DELAY)
            self._reconnect_delay = delay

    async def _cleanup_ws(self) -> None:
        """
        * @Author Leon-liao
        * @Function: _cleanup_ws()
        * @Description //取消读循环并关闭 WebSocket 连接
        * @Date :2026/06/30 18:30:00
        * @Param: 无
        """
        if self._read_task and not self._read_task.done():
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("harness_mate 读循环退出异常")
        self._read_task = None

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                logger.exception("harness_mate 关闭 WebSocket 失败")
        self._ws = None

    async def disconnect(self) -> None:
        """
        * @Author Leon-liao
        * @Function: disconnect()
        * @Description //主动断开与 WS 网关的连接，停止自动重连
        * @Date :2026/07/01 16:00:00
        * @Param: 无
        """
        self._should_reconnect = False
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        self._reconnect_task = None
        try:
            await self._cleanup_ws()
            self._dedup.clear()
            self._reply_targets.clear()
            _hm_clear_active_runs()
            self._peer_by_device.clear()
            self._typing_last_sent.clear()
            self._mark_disconnected()
            logger.info("harness_mate 已断开: bot_id=%s", self.bot_id)
        except Exception:
            logger.exception("harness_mate 断开失败")

    async def get_chat_info(self, chat_id: str) -> dict:
        """
        * @Author Leon-liao
        * @Function: get_chat_info(chat_id)
        * @Description //返回账号会话的基本信息
        * @Date :2026/06/30 18:30:00
        * @Param: chat_id: str，账号/会话标识
        * @return：包含 name 与 type 的字典
        """
        return {"name": chat_id, "type": "dm"}

    async def _read_loop(self) -> None:
        """
        * @Author Leon-liao
        * @Function: _read_loop()
        * @Description //持续读取网关推送的消息并分发处理
        * @Date :2026/06/30 18:30:00
        * @Param: 无
        """
        ws = self._ws
        if ws is None:
            return
        try:
            async for raw_text in ws:
                try:
                    payload = json.loads(raw_text)
                except json.JSONDecodeError:
                    logger.warning("harness_mate 收到非 JSON 消息，已忽略")
                    continue
                if not isinstance(payload, dict):
                    continue
                await self._on_gateway_message(payload)
        except asyncio.CancelledError:
            raise
        except ConnectionClosed as exc:
            logger.warning(
                "harness_mate WS 连接已关闭: code=%s reason=%s",
                exc.code,
                exc.reason,
            )
        except Exception:
            logger.exception("harness_mate 读循环异常")
        finally:
            self._ws = None
            if not self._should_reconnect:
                self._mark_disconnected()
                return
            await self._handle_connection_lost()

    async def _on_gateway_message(self, raw: Dict[str, Any]) -> None:
        """
        * @Author Leon-liao
        * @Function: _on_gateway_message(raw)
        * @Description //处理网关点对点入站消息，解析 data 中的业务负载
        * @Date :2026/07/02 10:00:00
        * @Param: raw: dict，含 from/to/data
        """
        peer_session = str(raw.get("from") or "").strip()
        if not peer_session:
            logger.debug("harness_mate 入站消息缺少 from，已忽略")
            return

        data = _parse_data_field(raw.get("data"))
        msg_type = str(data.get("type") or "").strip()
        # 平台下行的审批决策：不投给 agent，直接解掉本地等待
        if msg_type == TYPE_APPROVAL_RESULT:
            await self._on_approval_result(data)
            return
        if msg_type == TYPE_MESSAGE:
            await self._on_peer_message(peer_session, data, raw)
            return
        logger.debug("harness_mate 忽略业务消息类型: %s", msg_type)

    async def _on_peer_message(
        self,
        peer_session: str,
        data: Dict[str, Any],
        raw: Dict[str, Any],
    ) -> None:
        """
        * @Author Leon-liao
        * @Function: _on_peer_message(peer_session, data, raw)
        * @Description //处理对端经网关转发的 message，转换为 MessageEvent
        * @Date :2026/07/02 10:00:00
        * @Param: peer_session: 对端 ws_session_id；data: 业务 data；raw: 完整网关消息
        """
        global _HM_INSTANCE, _HM_LOOP
        _HM_INSTANCE = self
        try:
            _HM_LOOP = asyncio.get_running_loop()
        except Exception:
            pass
        try:
            nested = _parse_data_field(data.get("data"))
            text = _extract_inbound_text(data, nested)
            inbound_files = _extract_inbound_files(data, nested)
            if inbound_files:
                # 多附件：把每个 URL 都补进正文兜底（落地失败的附件至少还能从正文点开）
                missing_urls = [item["url"] for item in inbound_files if item["url"] not in text]
                if missing_urls:
                    joined = "\n".join(missing_urls)
                    text = f"{text}\n{joined}".strip() if text else joined
            if not text:
                logger.debug("harness_mate 入站消息无文本内容，已忽略")
                return

            msg_id = str(
                data.get("msg_id")
                or nested.get("msg_id")
                or nested.get("message_id")
                or raw.get("msg_id")
                or ""
            ).strip()
            if msg_id and self._dedup.is_duplicate(msg_id):
                logger.debug("harness_mate 重复消息已忽略: %s", msg_id)
                return

            if not self._is_peer_allowed(peer_session):
                logger.debug("harness_mate 对端未授权: %s", peer_session)
                return

            # ★ 审批决策：#approval <approval_id> <option_id>，命中后回调 Hermes resolver，不投给 agent
            approval_match = re.match(
                r"^#approval\s+(\S+)\s+(allow-once|reject-once)\s*$",
                text.strip(),
            )
            if approval_match:
                await self._handle_approval_decision(
                    approval_match.group(1),
                    approval_match.group(2),
                )
                return

            account_id = str(data.get("account_id") or self.bot_id).strip()
            thread_id = str(data.get("thread_id") or "").strip()
            # P3：房间消息（平台按 target_agent_ids 下发时带 room_id + message_id）。
            # 房间单独一个会话键 room:<room_id>，避免和私聊共用上下文
            room_id = str(data.get("room_id") or nested.get("room_id") or "").strip()
            room_reply_to = str(
                data.get("message_id") or nested.get("message_id") or ""
            ).strip()
            if room_id:
                thread_id = thread_id or f"room:{room_id}"
                self._room_by_thread[thread_id] = {
                    "room_id": room_id,
                    "reply_to": room_reply_to,
                }
            # 新的一轮必须换新的消息 ID：正文帧与工具进度帧都靠它取同一条
            # （旧轮的帧仍走 _msg_by_turn[旧 turn]，不会因为 renew 被抢走）
            turn_id = self._turn_message_id(thread_id, renew=True)
            try:
                from . import tool_trace as _tt_in
                _tt_in.set_thread(thread_id)
                _tt_in.bind_turn(thread_id, turn_id)
            except Exception:
                pass
            self._last_inbound_by_chat[account_id] = {
                "thread_id": thread_id,
                "room_id": room_id,
                "reply_to": room_reply_to,
                "at": time.monotonic(),
            }
            device_id = str(
                data.get("from_device") or data.get("device_id") or peer_session
            ).strip()
            self._reply_targets[(account_id, thread_id)] = peer_session
            self._peer_by_device[device_id] = peer_session
            self._last_peer_session = peer_session
            # 运行态心跳：本轮从这一秒开始算在干活，立刻补一帧再每 30s 续期
            _hm_mark_running(account_id, thread_id, peer_session)

            message_type = MessageType.TEXT
            media_urls: List[str] = []
            media_types: List[str] = []
            primary_media_type = ""
            for inbound_file in inbound_files:
                # 平台推的是 COS 的 https URL；Hermes 准入层只收「本机绝对路径的常规文件」，
                # 直接塞 URL 会被判 invalid_params 并连正文一起丢弃 —— 先下载落地再上抛。
                local_path = await materialize_inbound_file(inbound_file)
                if not local_path:
                    continue
                media_urls.append(local_path)
                media_types.append(inbound_file["mime_type"])
                if not primary_media_type:
                    primary_media_type = inbound_file["media_type"]
            if media_urls:
                message_type = {
                    "image": MessageType.PHOTO,
                    "video": MessageType.VIDEO,
                    "audio": MessageType.AUDIO,
                }.get(primary_media_type, MessageType.DOCUMENT)

            event = MessageEvent(
                source=self.build_source(
                    chat_id=account_id,
                    chat_type="dm",
                    user_id=device_id,
                    user_name=device_id,
                    thread_id=thread_id or None,
                ),
                text=text,
                message_type=message_type,
                media_urls=media_urls,
                media_types=media_types,
                raw_message=raw,
                message_id=msg_id or None,
            )
            # turn 开始前按需打补丁：register 时 gateway_agent 通常尚未 import
            _ensure_tool_trace_patch()
            await self.handle_message(event)
        except Exception:
            logger.exception("harness_mate 处理对端消息失败")

    def _turn_message_id(self, thread_id: str, renew: bool = False) -> str:
        """
        * @Author Leon-liao
        * @Function: _turn_message_id(thread_id, renew)
        * @Description //取本轮回复的消息 ID。一轮 = 一次用户请求：收到入站消息时 renew 生成，之后进度帧/正文帧/done 帧全部复用它
        * @Date :2026/11/20 23:30:00
        * @Param: thread_id: 会话线程；renew: 为新的一轮重新生成
        * @return：消息 ID；thread_id 为空时返回空串
        """
        tid = str(thread_id or "").strip()
        if not tid:
            return ""
        if renew or not self._turn_msg_id.get(tid):
            turn = str(uuid.uuid4())
            self._turn_msg_id[tid] = turn
            # 新的一轮：清掉该线程遗留步骤并切到本轮桶，避免跨轮串步
            try:
                from . import tool_trace as _tt
                _tt.reset(tid)
                _tt.set_turn(turn)
            except Exception:
                pass
        return self._turn_msg_id[tid]

    def _clear_turn_message_id(self, thread_id: str) -> None:
        """本轮结束（done 已发）后清掉映射，下一轮重新生成，避免跨轮复用同一个消息 ID"""
        self._turn_msg_id.pop(str(thread_id or "").strip(), None)

    def _bind_turn(self, thread_id: str, turn_id: str) -> str:
        """把 Hermes 的 turn_id 绑定到一个稳定的消息 ID；返回该消息 ID。"""
        tid = str(thread_id or "").strip()
        turn = str(turn_id or "").strip()
        if not turn:
            # 兼容：老版本 Hermes 不带 turn_id，退回线程级逻辑
            return self._turn_message_id(tid)
        msg_id = self._msg_by_turn.get(turn)
        if not msg_id:
            # 必须复用本轮入站时生成的 ID，否则正文帧（入站 ID）和工具进度帧（这里新建）
            # 会落到两条消息上，表现为「正文一条 + 工具一条」或者空壳消息
            msg_id = self._turn_message_id(tid) or str(uuid.uuid4())
            self._msg_by_turn[turn] = msg_id
        if tid:
            self._active_turn_by_thread[tid] = turn
        return msg_id

    def _msg_id_for_thread(self, thread_id: str) -> str:
        """正文帧用：取该线程当前活跃 turn 的消息 ID；没有则退回线程级/新建。"""
        tid = str(thread_id or "").strip()
        turn = self._active_turn_by_thread.get(tid, "")
        if turn:
            msg = self._msg_by_turn.get(turn)
            if msg:
                return msg
        return self._turn_message_id(tid)

    def _dump_undeliverable(
        self,
        chat_id: str,
        metadata: Optional[Dict[str, Any]],
        *,
        reason: str,
    ) -> None:
        """
        * @Author Leon-liao
        * @Function: _dump_undeliverable(chat_id, metadata, reason)
        * @Description //归属不明的回复落盘，供事后补发；绝不猜线程投递
        * @Date :2026/11/22 21:17:00
        * @Param: chat_id / metadata / reason
        * @return：无
        """
        try:
            path = os.path.expanduser(
                "~/.hermes/cache/harness_mate_undeliverable.jsonl"
            )
            os.makedirs(os.path.dirname(path), exist_ok=True)
            record = {
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "chat_id": str(chat_id or ""),
                "reason": str(reason or ""),
                "metadata": metadata if isinstance(metadata, dict) else {},
            }
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        except Exception:
            logger.exception("harness_mate 落盘 undeliverable 失败")

    def _resolve_target(
        self,
        chat_id: str,
        metadata: Optional[Dict[str, Any]],
    ) -> Tuple[str, str]:
        """
        * @Author Leon-liao
        * @Function: _resolve_target(chat_id, metadata)
        * @Description //解析 thread_id 与目标 ws_session_id。thread 只信 metadata；无 thread 时仅允许「最近入站房间」归位，绝不跨会话猜
        * @Date :2026/07/02 11:30:00
        * @Param: chat_id: 账号 ID；metadata: 发送元数据
        * @return：(thread_id, to_ws_session) 元组
        """
        metadata = metadata or {}
        meta_thread = str(
            metadata.get("thread_id") or metadata.get("session_id") or ""
        ).strip()
        recent = self._last_inbound_by_chat.get(chat_id) or {}
        recent_thread = ""
        if (
            recent
            and (time.monotonic() - float(recent.get("at") or 0))
            <= ROOM_CONTEXT_TTL_SECONDS
        ):
            recent_thread = str(recent.get("thread_id") or "").strip()

        thread_id = meta_thread
        # 房间归位：只在 metadata 完全没给线程时，才用「最近入站房间」兜底
        if (
            not meta_thread
            and recent_thread
            and recent.get("room_id")
        ):
            logger.info(
                "harness_mate 房间回包归位 chat=%s meta_thread=%s -> %s",
                chat_id,
                meta_thread,
                recent_thread,
            )
            thread_id = recent_thread

        to_ws_session = str(self._reply_targets.get((chat_id, thread_id), "")).strip()
        if not to_ws_session:
            to_ws_session = str(self._last_peer_session or "").strip()
        if not to_ws_session:
            device_hint = str(
                metadata.get("to_device") or metadata.get("user_id") or ""
            ).strip()
            if device_hint:
                to_ws_session = str(self._peer_by_device.get(device_hint, "")).strip()
        if not to_ws_session:
            to_ws_session = str(
                metadata.get("to_ws_session") or metadata.get("to") or ""
            ).strip()
        logger.debug(
            "harness_mate 解析回包目标 chat=%s meta_thread=%s recent_thread=%s -> thread=%s ws=%s",
            chat_id, meta_thread, recent_thread, thread_id, to_ws_session,
        )
        return thread_id, to_ws_session

    def _typing_key(self, to_ws_session: str, thread_id: str) -> Tuple[str, str]:
        """
        * @Author Leon-liao
        * @Function: _typing_key(to_ws_session, thread_id)
        * @Description //构造思考状态节流键
        * @Date :2026/07/02 10:00:00
        * @Param: to_ws_session: 目标 ws_session_id；thread_id: 会话线程
        * @return：(to_ws_session, thread_id) 元组
        """
        return to_ws_session, thread_id

    def _reset_typing_throttle(self, to_ws_session: str, thread_id: str) -> None:
        """
        * @Author Leon-liao
        * @Function: _reset_typing_throttle(to_ws_session, thread_id)
        * @Description //正文回复后重置思考节流，便于下一轮立即显示思考中
        * @Date :2026/07/02 10:00:00
        * @Param: to_ws_session: 目标 ws_session_id；thread_id: 会话线程
        """
        self._typing_last_sent.pop(self._typing_key(to_ws_session, thread_id), None)

    def _delta_payload(
        self,
        thread_id: str,
        message_id: str,
        content: str,
        *,
        streaming: bool,
        finalize: bool,
        is_room: bool,
    ) -> tuple[str, bool]:
        """流式帧只发新增后缀；done / 一次性消息 / 群聊发全量。返回 (text, delta)。"""
        text = content or ""
        key = f"{thread_id}:{message_id}"
        if finalize or not streaming or is_room:
            self._stream_sent.pop(key, None)
            return text, False
        prev = self._stream_sent.get(key, "")
        if prev and text.startswith(prev):
            self._stream_sent[key] = text
            return text[len(prev):], True
        self._stream_sent[key] = text
        return text, False

    def _build_reply_data(
        self,
        content: str,
        message_id: str,
        metadata: Optional[Dict[str, Any]],
        *,
        finalize: bool = False,
        thread_id: str = "",
    ) -> Dict[str, Any]:
        """
        * @Author Leon-liao
        * @Function: _build_reply_data(content, message_id, metadata, finalize)
        * @Description //构造发往 App 的 reply.data，支持流式 delta 与思考过程
        * @Date :2026/07/01 18:00:00
        * @Param: content: 回复正文；message_id: 消息 ID；metadata: 发送元数据；finalize: 是否为流式最终帧
        * @return：reply.data 字典
        """
        metadata = metadata or {}
        _ensure_tool_trace_patch()
        is_streaming = bool(
            metadata.get("delta")
            or metadata.get("expect_edits")
            or metadata.get("stream")
        )
        is_final = bool(
            finalize
            or metadata.get("done")
            or metadata.get("final")
            or metadata.get("notify")
        )
        is_room = bool(self._room_fields(thread_id))
        text_out, delta_out = self._delta_payload(
            thread_id,
            message_id,
            content,
            streaming=is_streaming,
            finalize=is_final,
            is_room=is_room,
        )
        reply_data: Dict[str, Any] = {
            "text": text_out[: self.MAX_MESSAGE_LENGTH],
            "message_id": message_id,
        }
        if delta_out:
            reply_data["delta"] = True
        if is_final:
            reply_data["done"] = True
            try:
                from . import tool_trace as _tt
                # 优先按 Hermes turn_id 取桶；无 turn 时退回 message_id（老路径）
                turn = self._active_turn_by_thread.get(str(thread_id or "").strip(), "")
                _steps = _tt.drain(thread_id, turn or message_id)
                logger.info("harness_mate drain agent_thoughts=%d", len(_steps))
            except Exception:
                _steps = []
            if _steps:
                reply_data["agent_thoughts"] = _steps
                # 私聊链路前端读的是 tool_events（ConversationChatCard 收集 → message_info.tool_events），两个都发，兼容两条路
                reply_data["tool_events"] = _steps
        else:
            # 非最终帧：仅当正文命中工具动作词表时打 status=tool（思考占位走 _send_typing）
            _hint_text = str(reply_data.get("text") or "")
            if _is_tool_hint_text(_hint_text, has_tool_events=_has_tool_hint_action(_hint_text)):
                reply_data["status"] = "tool"
        reasoning = metadata.get("reasoning")
        if reasoning:
            reply_data["reasoning"] = reasoning
        reply_data.update({k: v for k, v in _collect_version_info().items() if v})
        return reply_data

    def _room_fields(self, thread_id: str) -> Dict[str, str]:
        """
        * @Author Leon-liao
        * @Function: _room_fields(thread_id)
        * @Description //房间会话的回包附加字段（room_id / reply_to），非房间会话返回空字典
        * @Date :2026/10/29 16:25:00
        * @Param: thread_id: 会话线程（房间为 room:<room_id>）
        * @return：dict
        """
        room_ctx = self._room_by_thread.get(str(thread_id or ""))
        if not room_ctx:
            return {}
        return {
            "room_id": str(room_ctx.get("room_id") or ""),
            "reply_to": str(room_ctx.get("reply_to") or ""),
        }

    async def _send_reply(
        self,
        chat_id: str,
        to_ws_session: str,
        thread_id: str,
        content: str,
        message_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: _send_reply(...)
        * @Description //向目标 ws_session_id 发送 type=reply 业务消息（支持流式）
        * @Date :2026/07/02 10:00:00
        * @Param: chat_id: 账号 ID；to_ws_session: 目标 ws_session_id；thread_id: 会话线程；content: 正文；message_id: 消息 ID；metadata: 元数据；finalize: 是否最终帧
        * @return：SendResult 发送结果
        """
        if not str(thread_id or "").strip():
            logger.warning(
                "harness_mate 无法解析 thread_id: chat=%s metadata=%s",
                chat_id,
                metadata,
            )
            self._dump_undeliverable(
                chat_id, metadata, reason="thread_id not resolved"
            )
            return SendResult(success=False, error="thread_id not resolved")
        if not to_ws_session:
            logger.warning(
                "harness_mate 无法解析 to_ws_session: chat=%s thread=%s metadata=%s",
                chat_id,
                thread_id,
                metadata,
            )
            return SendResult(success=False, error="to_ws_session not resolved")
        if self._ws is None:
            return SendResult(success=False, error="WebSocket not connected")

        reply_data = self._build_reply_data(
            content,
            message_id,
            metadata,
            finalize=finalize,
            thread_id=thread_id,
        )
        fingerprint = json.dumps(reply_data, ensure_ascii=False, sort_keys=True)
        cache_key = f"{to_ws_session}:{thread_id}:{message_id}"
        if self._reply_fingerprints.get(cache_key) == fingerprint:
            logger.debug(
                "harness_mate 跳过重复 reply 帧: peer=%s msg=%s",
                to_ws_session,
                message_id,
            )
            return SendResult(success=True, message_id=message_id)
        self._reply_fingerprints[cache_key] = fingerprint
        if reply_data.get("done"):
            self._reply_fingerprints.pop(cache_key, None)
            # 先摘活跃登记再发 done：顺序反了心跳会把状态重新点亮
            _hm_mark_idle(chat_id, thread_id)

        business_payload = {
            "type": TYPE_REPLY,
            "account_id": chat_id,
            "thread_id": thread_id,
            "bot_id": self.bot_id,
            "data": reply_data,
        }
        # P3：房间消息的回包必须带回 room_id + 原 message_id，平台才能归到房间
        business_payload.update(self._room_fields(thread_id))
        ok = await self._send_ws(to_ws_session, business_payload)
        if not ok:
            return SendResult(success=False, error="ws send failed")
        # done 已送达：清掉本轮 turn / 消息 ID 映射（避免跨轮复用）
        if reply_data.get("done"):
            turn = self._active_turn_by_thread.get(thread_id, "")
            if turn and self._msg_by_turn.get(turn) == message_id:
                self._msg_by_turn.pop(turn, None)
                self._active_turn_by_thread.pop(thread_id, None)
                try:
                    from . import tool_trace as _tt
                    _tt.reset(thread_id)
                except Exception:
                    pass
            if self._turn_msg_id.get(thread_id) == message_id:
                self._clear_turn_message_id(thread_id)
        self._reset_typing_throttle(to_ws_session, thread_id)
        logger.debug(
            "harness_mate 已发送 reply: peer=%s thread=%s msg=%s delta=%s done=%s len=%d",
            to_ws_session,
            thread_id,
            message_id,
            reply_data.get("delta"),
            reply_data.get("done"),
            len(str(reply_data.get("text") or "")),
        )
        return SendResult(success=True, message_id=message_id)

    def _approval_timeout_seconds(self) -> float:
        """
        * @Author Leon-liao
        * @Function: _approval_timeout_seconds()
        * @Description //读取 approvals.timeout，缺省 300 秒
        * @Date :2026/10/29 21:01:51
        * @Param: 无
        * @return：超时秒数
        """
        try:
            from tools.approval_context import _get_approval_timeout
            return float(_get_approval_timeout())
        except Exception:
            env_ms = os.getenv("HARNESS_MATE_APPROVAL_TIMEOUT_MS", "").strip()
            if env_ms:
                try:
                    return max(float(env_ms) / 1000.0, 1.0)
                except ValueError:
                    pass
            return float(DEFAULT_APPROVAL_TIMEOUT_SECONDS)

    def _format_approval_fallback_text(self, prompt: ExecApprovalPrompt) -> str:
        """
        * @Author Leon-liao
        * @Function: _format_approval_fallback_text(prompt)
        * @Description //拼装审批兜底文本（平台授权页 + /approve）
        * @Date :2026/10/29 21:01:51
        * @Param: prompt: ExecApprovalPrompt
        * @return：文本内容
        """
        command = str(getattr(prompt, "command", "") or "")
        first_line = command.strip().splitlines()[0] if command.strip() else "需要执行的命令"
        description = str(getattr(prompt, "description", "") or "").strip()
        lines = ["⚠️ 需要授权：%s" % first_line]
        if description:
            lines.append(description)
        lines.append("")
        lines.append("在平台左侧「授权」页点「允许一次」；也可以在本对话直接回复 /approve 或 /deny")
        return "\n".join(lines)

    async def _finalize_approval(
        self,
        approval_id: str,
        choice: str,
        *,
        reason: Optional[str] = None,
    ) -> None:
        """
        * @Author Leon-liao
        * @Function: _finalize_approval(approval_id, choice, reason)
        * @Description //统一解阻塞：取消定时器、清状态、调用 resolve_gateway_approval
        * @Date :2026/10/29 21:01:51
        * @Param: approval_id: 客户端审批 id；choice: once/deny；reason: 拒绝原因
        * @return：无
        """
        state = self._approval_state.pop(approval_id, None)
        if not state:
            return
        timer = state.get("timer")
        if timer is not None:
            try:
                timer.cancel()
            except Exception:
                pass
        session_key = str(state.get("session_key") or "")
        if not session_key:
            return
        try:
            from tools.approval import resolve_gateway_approval
            count = resolve_gateway_approval(session_key, choice, reason=reason)
            logger.info(
                "harness_mate 审批已解阻塞: id=%s choice=%s resolved=%d reason=%s",
                approval_id,
                choice,
                count,
                reason,
            )
        except Exception:
            logger.exception(
                "harness_mate 回调 resolve_gateway_approval 失败: id=%s",
                approval_id,
            )

    async def _send_exec_approval_prompt(self, prompt: ExecApprovalPrompt) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: _send_exec_approval_prompt(prompt)
        * @Description //发 approval 帧 + 文本兜底，并后台轮询平台 lookup 拿决策
        * @Date :2026/10/29 21:01:51
        * @Param: prompt: ExecApprovalPrompt
        * @return：SendResult(success=True)
        """
        client_id = "hermes-%s" % uuid.uuid4().hex[:12]
        chat_id = str(getattr(prompt, "chat_id", "") or "").strip()
        session_key = str(getattr(prompt, "session_key", "") or "").strip()
        metadata = getattr(prompt, "metadata", None) or {}
        if not isinstance(metadata, dict):
            metadata = {}

        thread_id, to_ws_session = self._resolve_target(chat_id, metadata)
        command = str(getattr(prompt, "command", "") or "")
        title = command[:120] if command.strip() else "需要执行的命令"
        action = "exec.shell"
        risk_level = "high" if bool(getattr(prompt, "smart_denied", False)) else "medium"
        digest = hashlib.sha256(command.encode("utf-8")).hexdigest()
        options = [
            {"option_id": "allow-once", "kind": "allow_once", "name": "允许一次"},
            {"option_id": "reject-once", "kind": "reject_once", "name": "拒绝"},
        ]
        try:
            meta = build_approval_meta(
                command=command,
                description=str(getattr(prompt, "description", "") or ""),
            )
            title = str(meta.get("title") or title)
            action = str(meta.get("action") or action)
            raw_risk = meta.get("risk_level")
            if isinstance(raw_risk, int) or (isinstance(raw_risk, str) and str(raw_risk).isdigit()):
                risk_level = {"0": "low", "1": "low", "2": "medium", "3": "high"}.get(
                    str(int(raw_risk)), risk_level
                )
            elif raw_risk:
                risk_level = str(raw_risk)
            if meta.get("payload_digest"):
                digest = str(meta["payload_digest"])
            options = default_approval_options()
        except Exception:
            logger.debug("harness_mate build_approval_meta 失败，使用默认审批字段", exc_info=True)

        expires_in_ms = int(self._approval_timeout_seconds() * 1000)
        approval_payload = {
            "type": TYPE_REPLY,
            "account_id": chat_id,
            "thread_id": thread_id,
            "bot_id": self.bot_id,
            "data": {
                "approval": {
                    "approval_id": client_id,
                    "title": title,
                    "action": action,
                    "risk_level": risk_level,
                    "payload_digest": digest,
                    "options": options,
                    "expires_in_ms": expires_in_ms,
                }
            },
        }
        approval_payload.update(self._room_fields(thread_id))

        if to_ws_session and self._ws is not None:
            ok = await self._send_ws(to_ws_session, approval_payload)
            if not ok:
                logger.warning(
                    "harness_mate 审批帧发送失败: peer=%s client_id=%s",
                    to_ws_session,
                    client_id,
                )
        else:
            logger.warning(
                "harness_mate 审批帧无法发送: chat=%s thread=%s peer=%s connected=%s",
                chat_id,
                thread_id,
                to_ws_session,
                self._ws is not None,
            )

        try:
            await self.send(
                chat_id,
                self._format_approval_fallback_text(prompt),
                metadata=metadata,
            )
        except Exception:
            logger.exception("harness_mate 审批兜底文本发送失败")

        loop = asyncio.get_running_loop()
        timer = loop.call_later(
            max(expires_in_ms, 1000) / 1000.0,
            lambda: asyncio.create_task(
                self._finalize_approval(client_id, "deny", reason="本地超时")
            ),
        )
        self._approval_state[client_id] = {
            "session_key": session_key,
            "chat_id": chat_id,
            "thread_id": thread_id,
            "peer": to_ws_session,
            "timer": timer,
        }

        logger.info(
            "harness_mate 已发起风险审批: client_id=%s chat=%s thread=%s peer=%s",
            client_id,
            chat_id,
            thread_id,
            to_ws_session,
        )
        return SendResult(success=True, message_id=client_id)

    async def _handle_approval_decision(
        self,
        approval_id: str,
        option_id: str,
        *,
        timed_out: bool = False,
    ) -> None:
        """
        * @Author Leon-liao
        * @Function: _handle_approval_decision(approval_id, option_id, timed_out)
        * @Description //处理 App #approval 回执或本地超时，回调 resolve_gateway_approval
        * @Date :2026/10/29 21:01:51
        * @Param: approval_id: 审批 ID；option_id: allow-once|reject-once；timed_out: 是否超时拒绝
        * @return：无
        """
        choice = OPTION_TO_CHOICE.get(option_id, "deny")
        if timed_out:
            choice = "deny"
            reason = "本地超时"
        else:
            reason = None if choice == "once" else "用户拒绝"
        await self._finalize_approval(approval_id, choice, reason=reason)

    async def _on_approval_result(self, data: Dict[str, Any]) -> None:
        """
        * @Author Leon-liao
        * @Function: _on_approval_result(data)
        * @Description //处理平台下行推送的审批决策帧（approval_result），命中本地等待立即解阻塞
        * @Date :2026/11/01 13:15:00
        * @Param: data: 网关下发的业务负载，含 approval_result
        * @return：无
        """
        result = data.get("approval_result")
        if not isinstance(result, dict):
            result = data
        approval_id = str(result.get("approval_id") or "").strip()
        if not approval_id:
            logger.debug("harness_mate 审批决策帧缺少 approval_id，已忽略")
            return
        try:
            status = int(str(result.get("status")).strip())
        except (TypeError, ValueError):
            logger.warning("harness_mate 审批决策帧缺少有效 status: %s", result)
            return
        if status == 3:
            await self._handle_approval_decision(approval_id, "reject-once", timed_out=True)
            return
        option_id = str(result.get("option_id") or "").strip()
        if not option_id:
            option_id = "allow-once" if status == 1 else "reject-once"
        logger.info(
            "harness_mate 收到审批决策推送: id=%s status=%d option=%s",
            approval_id,
            status,
            option_id,
        )
        await self._handle_approval_decision(approval_id, option_id)

    async def _send_ws(self, to: str, data: Dict[str, Any]) -> bool:

        """
        * @Author Leon-liao
        * @Function: _send_ws(to, data)
        * @Description //向 ws_gateway 发送点对点 WSMessage
        * @Date :2026/07/02 10:00:00
        * @Param: to: 目标 ws_session_id；data: 业务负载
        * @return：发送成功返回 True
        """
        if self._ws is None:
            return False
        payload = {"to": to, "data": data}
        try:
            async with self._send_lock:
                await self._ws.send(json.dumps(payload, ensure_ascii=False))
            return True
        except Exception:
            logger.exception("harness_mate WS 发送失败")
            return False

    def _file_name_from_url(self, url: str) -> str:
        """
        * @Author Leon-liao
        * @Function: _file_name_from_url(url)
        * @Description //从 URL 路径中取出文件名
        * @Date :2026/10/07 20:12:00
        * @Param: url: 文件地址
        * @return：文件名；无法解析时返回空字符串
        """
        path = str(url or "").split("?", 1)[0].rstrip("/")
        name = path.rsplit("/", 1)[-1]
        if not name or "." not in name:
            return ""
        return name

    async def _send_file_message(
        self,
        chat_id: str,
        to_ws_session: str,
        thread_id: str,
        url: str,
        file_name: str,
        media_type: str,
        caption: str,
        message_id: str,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: _send_file_message(...)
        * @Description //把文件 URL 作为 reply 发给当前会话，供对端直接打开
        * @Date :2026/10/07 20:12:00
        * @Param: chat_id: 账号 ID；to_ws_session: 目标会话；thread_id: 线程；url: 文件地址；file_name: 文件名；media_type: image 或 file；caption: 说明文字；message_id: 消息 ID
        * @return：SendResult 发送结果
        """
        if not to_ws_session:
            return SendResult(success=False, error="to_ws_session not resolved")
        if self._ws is None:
            return SendResult(success=False, error="WebSocket not connected", retryable=True)

        reply_data = {
            "text": (caption or "")[: self.MAX_MESSAGE_LENGTH],
            "message_id": message_id,
            "done": True,
            "url": url,
            "file_name": file_name,
            "media_type": media_type,
            "files": [
                {
                    "url": url,
                    "name": file_name or "",
                    "media_kind": media_type,
                    "is_image": media_type == "image",
                }
            ],
        }
        reply_data.update({k: v for k, v in _collect_version_info().items() if v})
        fingerprint = json.dumps(reply_data, ensure_ascii=False, sort_keys=True)
        cache_key = f"{to_ws_session}:{thread_id}:{message_id}:file"
        if self._reply_fingerprints.get(cache_key) == fingerprint:
            return SendResult(success=True, message_id=message_id)
        self._reply_fingerprints[cache_key] = fingerprint

        file_payload = {
            "type": TYPE_REPLY,
            "account_id": chat_id,
            "thread_id": thread_id,
            "bot_id": self.bot_id,
            "data": reply_data,
        }
        file_payload.update(self._room_fields(thread_id))
        ok = await self._send_ws(to_ws_session, file_payload)
        if not ok:
            return SendResult(success=False, error="ws send failed", retryable=True)
        self._reset_typing_throttle(to_ws_session, thread_id)
        logger.info(
            "harness_mate 已发送文件: peer=%s kind=%s name=%s",
            to_ws_session,
            media_type,
            file_name or url,
        )
        return SendResult(success=True, message_id=message_id, raw_response={"url": url})

    async def _deliver_local_file(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str],
        metadata: Optional[Dict[str, Any]],
        media_type: str,
        file_name: Optional[str] = None,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: _deliver_local_file(...)
        * @Description //先调用免授权上传接口，再把返回的 URL 发给当前会话
        * @Date :2026/10/07 20:12:00
        * @Param: chat_id: 账号 ID；file_path: 本地文件；caption: 说明；metadata: 路由元数据；media_type: image 或 file；file_name: 展示文件名
        * @return：SendResult 发送结果
        """
        metadata = metadata or {}
        thread_id, to_ws_session = self._resolve_target(chat_id, metadata)
        thread_id = str(thread_id or "").strip()
        if not to_ws_session:
            return SendResult(success=False, error="to_ws_session not resolved")
        if self._ws is None:
            return SendResult(success=False, error="WebSocket not connected", retryable=True)
        display_name = str(file_name or "").strip() or os.path.basename(str(file_path or ""))
        kind = media_type if media_type in {"image", "audio", "video", "file"} else media_kind_for_name(display_name)
        message_id = str(metadata.get("message_id") or uuid.uuid4())
        room_id = str(self._room_fields(thread_id).get("room_id") or "").strip()
        try:
            # 上传接口按 conversation.thread_id 校验会话归属，必须传 td-xxx，不能传 peer 的 ws_session_id；
            # 房间会话无 conversation，额外透传 room_id 供后端按房间成员兜底
            url = await asyncio.to_thread(
                upload_open_api_file,
                file_path,
                self.bot_id,
                self.bot_key,
                thread_id,
                self.upload_url,
                room_id=room_id,
            )
        except OpenApiUploadError as exc:
            logger.warning("harness_mate 文件上传失败: %s", exc.message)
            return SendResult(success=False, error=exc.message, retryable=exc.retryable)
        except Exception as exc:
            logger.exception("harness_mate 文件上传异常")
            return SendResult(success=False, error=str(exc), retryable=True)
        return await self._send_file_message(
            chat_id,
            to_ws_session,
            thread_id,
            url,
            display_name,
            kind,
            caption or "",
            message_id,
        )

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: send_image(chat_id, image_url, caption, reply_to, metadata)
        * @Description //发送图片。本地 file:// 先上传，远程地址直接随 reply 下发
        * @Date :2026/10/07 20:12:00
        * @Param: chat_id: 账号 ID；image_url: 图片地址；caption: 说明；reply_to: 兼容参数；metadata: 路由元数据
        * @return：SendResult 发送结果
        """
        del reply_to
        image_url = str(image_url or "").strip()
        if image_url.startswith("file://"):
            return await self.send_image_file(
                chat_id,
                url2pathname(image_url[7:]),
                caption=caption,
                metadata=metadata,
            )
        if not image_url.startswith(("http://", "https://")):
            return SendResult(success=False, error="图片地址无效")
        metadata = metadata or {}
        thread_id, to_ws_session = self._resolve_target(chat_id, metadata)
        file_name = self._file_name_from_url(image_url)
        message_id = str(metadata.get("message_id") or uuid.uuid4())
        return await self._send_file_message(
            chat_id,
            to_ws_session,
            thread_id,
            image_url,
            file_name,
            "image",
            caption or "",
            message_id,
        )

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: send_image_file(chat_id, image_path, caption, reply_to, metadata, **kwargs)
        * @Description //上传本地图片并发送给当前会话
        * @Date :2026/10/07 20:12:00
        * @Param: chat_id: 账号 ID；image_path: 本地图片路径；caption: 说明；metadata: 路由元数据
        * @return：SendResult 发送结果
        """
        del reply_to, kwargs
        if extension_of(image_path) and media_kind_for_name(image_path) != "image":
            return SendResult(success=False, error=f"该.{extension_of(image_path)}扩展的文件不允许作为图片上传")
        return await self._deliver_local_file(
            chat_id,
            image_path,
            caption,
            metadata,
            "image",
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: send_document(chat_id, file_path, caption, file_name, reply_to, metadata, **kwargs)
        * @Description //上传本地文档或图片并发送给当前会话
        * @Date :2026/10/07 20:12:00
        * @Param: chat_id: 账号 ID；file_path: 本地文件路径；caption: 说明；file_name: 展示名；metadata: 路由元数据
        * @return：SendResult 发送结果
        """
        del reply_to
        if not file_name:
            file_name = str(kwargs.get("filename") or kwargs.get("file_name") or "").strip() or None
        kind = "file"
        shown = file_name or file_path
        if media_kind_for_name(shown) == "image":
            kind = "image"
        return await self._deliver_local_file(
            chat_id,
            file_path,
            caption,
            metadata,
            kind,
            file_name=file_name,
        )

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: send_voice(chat_id, audio_path, caption, reply_to, metadata, **kwargs)
        * @Description //上传本地音频并发送给当前会话，reply 帧 media_type 为 audio
        * @Date :2026/10/08 20:31:00
        * @Param: chat_id: 账号 ID；audio_path: 本地音频路径；caption: 说明；reply_to: 兼容参数；metadata: 路由元数据
        * @return：SendResult 发送结果
        """
        del reply_to, kwargs
        if extension_of(audio_path) and media_kind_for_name(audio_path) != "audio":
            return SendResult(success=False, error=f"该.{extension_of(audio_path)}扩展的文件不允许作为音频上传")
        return await self._deliver_local_file(
            chat_id,
            audio_path,
            caption,
            metadata,
            "audio",
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: send_video(chat_id, video_path, caption, reply_to, metadata, **kwargs)
        * @Description //上传本地视频并发送给当前会话，reply 帧 media_type 为 video
        * @Date :2026/10/08 20:31:00
        * @Param: chat_id: 账号 ID；video_path: 本地视频路径；caption: 说明；reply_to: 兼容参数；metadata: 路由元数据
        * @return：SendResult 发送结果
        """
        del reply_to, kwargs
        if extension_of(video_path) and media_kind_for_name(video_path) != "video":
            return SendResult(success=False, error=f"该.{extension_of(video_path)}扩展的文件不允许作为视频上传")
        return await self._deliver_local_file(
            chat_id,
            video_path,
            caption,
            metadata,
            "video",
        )

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """
        * @Author Leon-liao
        * @Function: send_typing(chat_id, metadata)
        * @Description //向设备推送「思考中」状态（节流，避免每 2s 刷屏）
        * @Date :2026/07/01 21:10:00
        * @Param: chat_id: 账号 ID；metadata: 含 thread_id / to_device
        """
        metadata = metadata or {}
        thread_id, to_ws_session = self._resolve_target(chat_id, metadata)
        if not to_ws_session:
            return

        typing_key = self._typing_key(to_ws_session, thread_id)
        async with self._typing_lock:
            now = time.monotonic()
            last_sent = self._typing_last_sent.get(typing_key, 0.0)
            if now - last_sent < TYPING_THROTTLE_SECONDS:
                logger.debug(
                    "harness_mate 跳过重复思考状态: peer=%s thread=%s",
                    to_ws_session,
                    thread_id,
                )
                return
            self._typing_last_sent[typing_key] = now

        await self._send_ws(
            to_ws_session,
            {
                "type": TYPE_REPLY,
                "account_id": chat_id,
                "thread_id": thread_id,
                "data": {
                    "status": "thinking",
                    "text": "思考中...",
                },
            },
        )

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: send(chat_id, content, reply_to, metadata)
        * @Description //经 WS 网关向目标设备发送 type=reply 消息（支持流式首帧）
        * @Date :2026/07/01 18:00:00
        * @Param: chat_id: 账号 ID；content: 消息文本；metadata: 含 thread_id / to_device / delta / expect_edits
        * @return：SendResult 发送结果
        """
        del reply_to
        metadata = metadata or {}
        _ensure_tool_trace_patch()
        thread_id, to_ws_session = self._resolve_target(chat_id, metadata)
        # 正文帧复用本轮消息 ID：优先取活跃 turn 绑定的 ID，与进度帧、done 帧归并
        msg_id = str(
            metadata.get("message_id")
            or self._msg_id_for_thread(thread_id)
            or uuid.uuid4()
        )
        try:
            return await self._send_reply(
                chat_id,
                to_ws_session,
                thread_id,
                content,
                msg_id,
                metadata,
                finalize=bool(metadata.get("notify")),
            )
        except Exception as exc:
            logger.exception("harness_mate 发送消息失败")
            return SendResult(success=False, error=str(exc), retryable=True)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """
        * @Author Leon-liao
        * @Function: edit_message(chat_id, message_id, content, finalize, metadata)
        * @Description //流式更新已发送消息，供 GatewayStreamConsumer 增量推送
        * @Date :2026/07/01 18:00:00
        * @Param: chat_id: 账号 ID；message_id: 首帧消息 ID；content: 当前累计正文；finalize: 是否最终帧；metadata: 路由元数据
        * @return：SendResult 发送结果
        """
        metadata = metadata or {}
        metadata = {**metadata, "stream": True}
        if finalize:
            metadata = {**metadata, "stream_finalize": True}
        thread_id, to_ws_session = self._resolve_target(chat_id, metadata)
        try:
            return await self._send_reply(
                chat_id,
                to_ws_session,
                thread_id,
                content,
                message_id,
                metadata,
                finalize=finalize,
            )
        except Exception as exc:
            logger.exception("harness_mate 流式编辑失败")
            return SendResult(success=False, error=str(exc), retryable=True)


def _check_deps() -> bool:
    """
    * @Author Leon-liao
    * @Function: _check_deps()
    * @Description //检查 harness_mate 所需 Python 依赖是否已安装
    * @Date :2026/06/30 18:30:00
    * @Param: 无
    * @return：依赖齐全返回 True，否则 False
    """
    try:
        import jwt  # noqa: F401
        import websockets  # noqa: F401
        return True
    except ImportError:
        return False


def _validate_config(config) -> bool:
    """
    * @Author Leon-liao
    * @Function: _validate_config(config)
    * @Description //校验 harness_mate 最小配置是否完整
    * @Date :2026/06/30 18:30:00
    * @Param: config: PlatformConfig
    * @return：配置完整返回 True
    """
    extra = getattr(config, "extra", {}) or {}
    bot_id = str(
        extra.get("bot_id")
        or extra.get("ws_session_id")
        or ""
    ).strip()
    bot_key = str(
        extra.get("bot_key")
        or extra.get("jwt_secret")
        or ""
    ).strip()
    if bot_id and bot_key:
        return True
    return _env_enablement() is not None


def _is_connected(config=None) -> bool:
    """
    * @Author Leon-liao
    * @Function: _is_connected(config)
    * @Description //判断 harness_mate 是否已配置或已建立连接
    * @Date :2026/06/30 18:30:00
    * @Param: config: PlatformConfig 或适配器实例
    * @return：已连接或已配置返回 True，否则 False
    """
    if config is None:
        return False
    if isinstance(config, PlatformConfig):
        return _validate_config(config)
    ws = getattr(config, "_ws", None)
    return ws is not None and not getattr(ws, "closed", True)


def _env_enablement() -> Optional[dict]:
    """
    * @Author Leon-liao
    * @Function: _env_enablement()
    * @Description //从环境变量种子化 PlatformConfig.extra
    * @Date :2026/06/30 18:30:00
    * @Param: 无
    * @return：配置 dict 或 None
    """
    bot_id = os.getenv("HARNESS_MATE_BOT_ID", "").strip()
    bot_key = os.getenv("HARNESS_MATE_BOT_KEY", "").strip()
    if not bot_id:
        bot_id = os.getenv("HARNESS_MATE_WS_SESSION_ID", "").strip()
    if not bot_key:
        bot_key = os.getenv("HARNESS_MATE_JWT_SECRET", "").strip()
    if not bot_id or not bot_key:
        return None

    seed: Dict[str, Any] = {
        "bot_id": bot_id,
        "bot_key": bot_key,
    }
    allowed = os.getenv("HARNESS_MATE_ALLOWED_DEVICES", "").strip()
    if allowed:
        seed["allow_from"] = allowed
    dm_policy = os.getenv("HARNESS_MATE_DM_POLICY", "").strip()
    if dm_policy:
        seed["dm_policy"] = dm_policy
    return seed


async def _standalone_send(pconfig, chat_id, message, *, thread_id=None, **kwargs):
    """
    * @Author Leon-liao
    * @Function: _standalone_send(...)
    * @Description //无 Gateway 进程时独立发送消息（Cron 等场景）
    * @Date :2026/06/30 18:30:00
    * @Param: pconfig: PlatformConfig；chat_id: 账号 ID；message: 文本
    * @return：发送结果 dict
    """
    del kwargs
    if not _check_deps():
        return {"error": "harness_mate 依赖未安装"}
    try:
        adapter = HarnessMateAdapter(pconfig)
        connected = await adapter.connect()
        if not connected:
            err = getattr(adapter, "fatal_error_message", None) or "connect failed"
            return {"error": f"harness_mate: {err}"}
        try:
            metadata = {"thread_id": thread_id} if thread_id else None
            result = await adapter.send(chat_id, message, metadata=metadata)
            if not result.success:
                return {"error": f"harness_mate send failed: {result.error}"}
            return {
                "success": True,
                "platform": "harness_mate",
                "chat_id": chat_id,
                "message_id": result.message_id,
            }
        finally:
            await adapter.disconnect()
    except Exception as exc:
        return {"error": f"harness_mate send failed: {exc}"}


def _build_adapter(config) -> HarnessMateAdapter:
    """
    * @Author Leon-liao
    * @Function: _build_adapter(config)
    * @Description //工厂函数，创建 HarnessMateAdapter 实例
    * @Date :2026/06/30 18:30:00
    * @Param: config: 平台配置对象
    * @return：HarnessMateAdapter 实例
    """
    return HarnessMateAdapter(config)


def _patch_harness_mate_thread_metadata() -> None:
    """
    * @Author Leon-liao
    * @Function: _patch_harness_mate_thread_metadata()
    * @Description //为 harness_mate 注入业务层 to_device/user_id，路由 ws_session_id 由适配器 _reply_targets 解析
    * @Date :2026/07/02 11:30:00
    * @Param: 无
    * @return：无
    """
    if globals().get("_HARNESS_MATE_THREAD_METADATA_PATCHED"):
        return

    def _enrich(metadata: Optional[Dict[str, Any]], source: Any) -> Optional[Dict[str, Any]]:
        if metadata is None:
            return None
        platform = getattr(source, "platform", None)
        platform_name = str(getattr(platform, "value", platform) or "")
        if platform_name != "harness_mate":
            return metadata
        user_id = getattr(source, "user_id", None)
        if not user_id:
            return metadata
        enriched = dict(metadata)
        enriched.setdefault("to_device", str(user_id))
        enriched.setdefault("user_id", str(user_id))
        return enriched

    try:
        import gateway.platforms.base as base_module

        _orig_base = base_module._thread_metadata_for_source

        def _base_patched(source, reply_to_message_id=None):
            return _enrich(_orig_base(source, reply_to_message_id), source)

        base_module._thread_metadata_for_source = _base_patched
    except Exception:
        logger.debug("harness_mate 跳过 base 线程元数据补丁", exc_info=True)

    try:
        # 修复（v0.5.3 在新版 Hermes 上的加载超时问题）：原实现直接
        # `from gateway.run import GatewayRunner`，在部分 Hermes 进程
        # （gateway 启动/重载窗口）中会触发 bootstrap relaunch 并长时间
        # 阻塞插件加载线程（表现为 "load timed out ... import + register()
        # never returned"，平台注册被丢弃）。改为仅在 gateway.run 已被
        # 宿主进程加载时打补丁 —— gateway 进程本身必然已加载 gateway.run，
        # 补丁照常生效；其他场景跳过，不再阻塞注册。
        import sys as _sys

        _gw_run_module = _sys.modules.get("gateway.run")
        _GatewayRunner = (
            getattr(_gw_run_module, "GatewayRunner", None) if _gw_run_module else None
        )
        if _GatewayRunner is None:
            logger.debug(
                "harness_mate 跳过 GatewayRunner 线程元数据补丁 (gateway.run 未加载)"
            )
        else:
            _orig_runner = _GatewayRunner._thread_metadata_for_source

            def _runner_patched(self, source, reply_to_message_id=None):
                return _enrich(
                    _orig_runner(self, source, reply_to_message_id),
                    source,
                )

            _GatewayRunner._thread_metadata_for_source = _runner_patched
    except Exception:
        logger.debug("harness_mate 跳过 GatewayRunner 线程元数据补丁", exc_info=True)

    globals()["_HARNESS_MATE_THREAD_METADATA_PATCHED"] = True


_TOOL_HINT_ACTION_VERBS = (
    "运行代码",
    "运行了",
    "执行了",
    "读取了",
    "写入了",
    "修改了",
    "搜索",
    "网页",
    "查看",
    "图片",
    "加载了",
    "派发",
    "待办",
    "记录",
    "设置",
    "定时",
    "查询",
    "会话",
    "生成",
    "语音",
    "操作了",
    "浏览器",
    "电脑",
)


def _has_tool_hint_action(text: str) -> bool:
    """正文是否包含工具动作词（与客户端词表一致）"""
    t = (text or "").strip()
    if not t:
        return False
    return any(verb in t for verb in _TOOL_HINT_ACTION_VERBS)


def _is_tool_hint_text(text: str, *, has_tool_events: bool) -> bool:
    """
    * @Author Leon-liao
    * @Function: _is_tool_hint_text(text, has_tool_events)
    * @Description //与客户端同规则：判定是否为工具提示文本（emoji + 动作词 + 参数）
    * @Date :2026/11/23 18:45:00
    * @Param: text: 帧正文；has_tool_events: 本帧/本轮是否带工具事件
    * @return：是工具提示返回 True
    """
    t = (text or "").strip()
    if not t:
        return False
    if t.startswith(("💻", "🖥", "🔧", "⏳")):
        return True
    if not has_tool_events:
        return False
    first = next(iter(t), "")
    cp = ord(first) if first else 0
    is_emoji = (0x1F300 <= cp <= 0x1FAFF) or (0x2600 <= cp <= 0x27BF)
    if not is_emoji:
        return False
    if len(t) > 120 or len(t.split("\n")) > 2:
        return False
    # 避免「✅ 已上线」这类正常短回复被当成工具提示
    return _has_tool_hint_action(t)


def _ensure_tool_trace_patch() -> None:
    """
    * @Author Leon-liao
    * @Function: _ensure_tool_trace_patch()
    * @Description //按需给 GatewayAgent 打工具事件补丁；register 时模块可能尚未加载，故每次发送/回包前再试
    * @Date :2026/10/30 11:12:00
    * @Param: 无
    * @return：无
    """
    if globals().get("_HARNESS_MATE_TOOL_TRACE_PATCHED"):
        return
    try:
        import sys as _sys2
        _gw_agent_mod = _sys2.modules.get("hermes_cli.gateway_agent")
        if _gw_agent_mod is None:
            try:
                import hermes_cli.gateway_agent as _gw_agent_mod  # noqa: N812
            except Exception:
                logger.debug(
                    "harness_mate 跳过工具事件补丁 (hermes_cli.gateway_agent 未加载)",
                    exc_info=True,
                )
                return
        _GatewayAgent = getattr(_gw_agent_mod, "GatewayAgent", None)
        if _GatewayAgent is None:
            logger.debug("harness_mate 跳过工具事件补丁 (GatewayAgent 不存在)")
            return
        if getattr(_GatewayAgent, "_hm_tool_trace_patched", False):
            globals()["_HARNESS_MATE_TOOL_TRACE_PATCHED"] = True
            return

        from . import tool_trace as _tool_trace

        # 关键：事件分发用的是模块级 _HANDLERS 字典（模块加载时已绑定旧函数对象），
        # 替换 GatewayAgent._on_tool_start 类方法对它无效，必须改字典本身。
        _handlers = getattr(_gw_agent_mod, "_HANDLERS", None)
        if not isinstance(_handlers, dict):
            logger.debug("harness_mate 跳过工具事件补丁 (_HANDLERS 不存在)")
            return
        _orig_on_tool_start = _handlers.get("tool.start")
        _orig_on_tool_complete = _handlers.get("tool.complete")
        if _orig_on_tool_start is None or _orig_on_tool_complete is None:
            logger.debug("harness_mate 跳过工具事件补丁 (_HANDLERS 缺少 tool.start/complete)")
            return

        def _hm_on_tool_start(self, payload, stream_callback, starts):
            try:
                name = payload.get("name") or payload.get("tool_name") or "tool"
                args = payload.get("args") if isinstance(payload.get("args"), dict) else {}
                call_id = payload.get("tool_id") or payload.get("tool_call_id") or ""
                _tool_trace.record_start(str(call_id), str(name), args)
                logger.info("harness_mate tool.start id=%s name=%s", call_id, name)
            except Exception:
                logger.debug("harness_mate 记录工具开始失败", exc_info=True)
            return _orig_on_tool_start(self, payload, stream_callback, starts)

        def _hm_on_tool_complete(self, payload, stream_callback, starts):
            try:
                name = payload.get("name") or payload.get("tool_name") or "tool"
                call_id = payload.get("tool_id") or payload.get("tool_call_id") or ""
                duration = float(payload.get("duration_s") or 0.0)
                _tool_trace.record_complete(
                    str(call_id), str(name), duration,
                    bool(payload.get("is_error")), payload.get("result"),
                )
            except Exception:
                logger.debug("harness_mate 记录工具完成失败", exc_info=True)
            return _orig_on_tool_complete(self, payload, stream_callback, starts)

        _handlers["tool.start"] = _hm_on_tool_start
        _handlers["tool.complete"] = _hm_on_tool_complete
        _GatewayAgent._hm_tool_trace_patched = True
        globals()["_HARNESS_MATE_TOOL_TRACE_PATCHED"] = True
        logger.info("harness_mate 已打上 _HANDLERS 工具事件补丁")
    except Exception:
        logger.debug("harness_mate 跳过工具事件补丁", exc_info=True)


def _hm_hook_pre_tool_call(tool_name=None, args=None, tool_call_id=None, **kwargs):
    """官方 pre_tool_call hook：工具开始执行（在 agent 进程内触发，跨进程可靠）"""
    try:
        from . import tool_trace as _tt
        turn_id = str(kwargs.get("turn_id") or "").strip()
        thread_id = _tt.thread_of_turn(turn_id) or _tt.current_thread()
        if thread_id:
            _tt.set_thread(thread_id)
        inst = _HM_INSTANCE
        if inst is not None:
            inst._bind_turn(thread_id, turn_id)
        if turn_id:
            _tt.set_turn(turn_id)
        _tt.record_start(
            str(tool_call_id or ""),
            str(tool_name or ""),
            args if isinstance(args, dict) else {},
            thread_id=thread_id,
            turn_id=turn_id,
        )
        logger.info("harness_mate hook pre_tool_call name=%s", tool_name)
    except Exception:
        logger.debug("harness_mate hook pre_tool_call 失败", exc_info=True)
    return None


def _hm_hook_post_tool_call(tool_name=None, args=None, tool_call_id=None, result=None, duration_ms=None, **kwargs):
    """官方 post_tool_call hook：工具执行结束，补齐耗时与结果摘要，并实时推一帧进度"""
    try:
        from . import tool_trace as _tt
        turn_id = str(kwargs.get("turn_id") or "").strip()
        thread_id = _tt.thread_of_turn(turn_id) or _tt.current_thread()
        if thread_id:
            _tt.set_thread(thread_id)
        inst = _HM_INSTANCE
        if inst is not None:
            inst._bind_turn(thread_id, turn_id)
        if turn_id:
            _tt.set_turn(turn_id)
        step = _tt.record_complete(
            str(tool_call_id or ""),
            str(tool_name or ""),
            float(duration_ms or 0) / 1000.0,
            False,
            result,
            thread_id=thread_id,
            turn_id=turn_id,
        )
        try:
            _push_tool_progress(step, turn_id=turn_id, thread_id=thread_id)  # type: ignore[arg-type]
        except Exception:
            logger.debug("harness_mate 进度帧推送异常", exc_info=True)
        logger.info("harness_mate hook post_tool_call name=%s", tool_name)
    except Exception:
        logger.debug("harness_mate hook post_tool_call 失败", exc_info=True)
    return None


def _register_tool_trace_hooks(ctx) -> None:
    """把工具事件采集挂到 Hermes 官方 hooks 上（不需要改 Hermes 本体）"""
    try:
        ctx.register_hook("pre_tool_call", _hm_hook_pre_tool_call)
        ctx.register_hook("post_tool_call", _hm_hook_post_tool_call)
        logger.info("harness_mate 已注册 pre/post_tool_call hook")
    except Exception:
        logger.warning("harness_mate 注册 tool hook 失败", exc_info=True)


def register(ctx) -> None:
    """
    * @Author Leon-liao
    * @Function: register(ctx)
    * @Description //Hermes 插件入口，注册 harness_mate 平台
    * @Date :2026/06/30 18:30:00
    * @Param: ctx: Hermes 插件注册上下文
    """
    _patch_harness_mate_thread_metadata()
    _register_tool_trace_hooks(ctx)
    ctx.register_platform(
        name="harness_mate",
        label="HarnessMate",
        adapter_factory=_build_adapter,
        check_fn=_check_deps,
        validate_config=_validate_config,
        is_connected=_is_connected,
        required_env=["HARNESS_MATE_BOT_ID", "HARNESS_MATE_BOT_KEY"],
        env_enablement_fn=_env_enablement,
        allowed_users_env="HARNESS_MATE_ALLOWED_DEVICES",
        allow_all_env="HARNESS_MATE_ALLOW_ALL_DEVICES",
        cron_deliver_env_var="HARNESS_MATE_BOT_ID",
        standalone_sender_fn=_standalone_send,
        max_message_length=MAX_MESSAGE_LENGTH,
        install_hint="pip install websockets PyJWT",
        emoji="🧩",
        allow_update_command=True,
    )
