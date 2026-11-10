"""DashScope task protocol transport, loaded only when a provider is used."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from urllib.parse import urlsplit
from uuid import uuid4

from ..errors import ProtocolError, ProviderError


@dataclass(frozen=True)
class ConnectionConfig:
    api_key: str = field(repr=False)
    url: str = "wss://dashscope.aliyuncs.com/api-ws/v1/inference"
    allow_insecure: bool = False

    def __post_init__(self):
        if not self.api_key.strip():
            raise ValueError("api_key must not be empty")
        parsed = urlsplit(self.url)
        if not parsed.hostname or parsed.scheme not in ({"wss", "ws"} if self.allow_insecure else {"wss"}):
            raise ValueError("url must use wss:// (ws:// requires allow_insecure=True for local tests)")
        if parsed.username or parsed.password:
            raise ValueError("Credentials must be passed through api_key")


class TaskSocket:
    def __init__(self, socket, task_id, limits):
        self.socket, self.task_id, self.limits = socket, task_id, limits

    @classmethod
    async def open(cls, config, payload, limits):
        try:
            from websockets.asyncio.client import connect
        except ImportError as exc:
            raise ImportError("Install the provider extra: pip install 'streaming-tts[qwen]'") from exc
        socket = None
        try:
            socket = await connect(
                config.url,
                additional_headers={"Authorization": f"Bearer {config.api_key}"},
                open_timeout=limits.start_timeout,
                close_timeout=limits.close_timeout,
                max_size=max(1048576, limits.max_chunk_bytes),
                max_queue=limits.buffer_size,
                compression=None,
            )
            session = cls(socket, str(uuid4()), limits)
            await session.command("run-task", payload)
            raw = await socket.recv()
            if isinstance(raw, bytes) or session.parse(raw)[0] != "task-started":
                raise ProtocolError("Expected task-started before streaming")
            return session
        except BaseException:
            if socket is not None:
                try:
                    async with asyncio.timeout(limits.close_timeout):
                        await socket.close()
                except Exception:
                    pass
            raise

    async def command(self, action, payload):
        await self.socket.send(
            json.dumps(
                {
                    "header": {"action": action, "task_id": self.task_id, "streaming": "duplex"},
                    "payload": payload,
                },
                ensure_ascii=False,
            )
        )

    def parse(self, raw):
        try:
            message = json.loads(raw)
            header = message["header"]
            if header["task_id"] != self.task_id:
                raise ProtocolError("Unexpected task_id")
            event = header["event"]
            if event == "task-failed":
                raise ProviderError(
                    str(header.get("error_message") or "Provider task failed"),
                    code=str(header.get("error_code") or "upstream_error"),
                )
            payload = message.get("payload", {})
            if not isinstance(payload, dict):
                raise TypeError("Invalid payload")
            return event, payload
        except (KeyError, TypeError, ValueError) as exc:
            raise ProtocolError("Malformed provider message") from exc

    async def finish_input(self):
        await self.command("finish-task", {"input": {}})

    async def aclose(self):
        await self.socket.close()
