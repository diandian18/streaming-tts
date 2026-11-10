from __future__ import annotations

import os
from dataclasses import dataclass

from ..errors import ProtocolError, UnsupportedCapabilityError
from ..types import Capabilities
from ._websocket import ConnectionConfig, TaskSocket


@dataclass(frozen=True)
class QwenConfig(ConnectionConfig):
    model: str = "qwen-audio-3.1-tts-flash"

    @classmethod
    def from_env(cls):
        return cls(
            api_key=os.getenv("DASHSCOPE_API_KEY", ""),
            url=os.getenv("TTS_WS_URL", "wss://dashscope.aliyuncs.com/api-ws/v1/inference"),
            model=os.getenv("TTS_MODEL", "qwen-audio-3.1-tts-flash"),
        )


class QwenProvider:
    """Qwen-Audio-TTS duplex task protocol (not Qwen-TTS-Realtime)."""

    capabilities = Capabilities(sample_rates=(8000, 16000, 22050, 24000, 44100, 48000))

    def __init__(self, config: QwenConfig):
        self.config = config

    async def open_session(self, options, limits):
        fmt = options.audio_format
        if fmt.encoding != "pcm" or fmt.channels != 1 or fmt.sample_width != 2:
            raise UnsupportedCapabilityError(
                "Qwen adapter currently returns mono signed 16-bit little-endian PCM"
            )
        reserved = {"format", "sample_rate", "voice", "text_type", "enable_ssml"} & options.parameters.keys()
        if reserved:
            raise ValueError(f"Parameters are managed by the adapter: {sorted(reserved)}")
        parameters = {
            "volume": 50,
            "rate": 1.0,
            "pitch": 1.0,
            **options.parameters,
            "text_type": "PlainText",
            "voice": options.voice,
            "format": "pcm",
            "sample_rate": fmt.sample_rate,
            "enable_ssml": False,
        }
        task = await TaskSocket.open(
            self.config,
            {
                "task_group": "audio",
                "task": "tts",
                "function": "SpeechSynthesizer",
                "model": self.config.model,
                "parameters": parameters,
                "input": {},
            },
            limits,
        )
        return _Session(task)


class _Session:
    def __init__(self, task):
        self.task = task

    async def send(self, text):
        await self.task.command("continue-task", {"input": {"text": text}})

    async def finish_input(self):
        await self.task.finish_input()

    async def receive(self):
        while True:
            raw = await self.task.socket.recv()
            if isinstance(raw, bytes):
                return raw
            event, _ = self.task.parse(raw)
            if event == "task-finished":
                return None
            if event != "result-generated":
                raise ProtocolError(f"Unexpected TTS event: {event}")
            # Timing/usage metadata is optional and doesn't change the audio stream.

    async def aclose(self):
        await self.task.aclose()
