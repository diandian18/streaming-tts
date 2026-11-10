from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class AudioFormat:
    encoding: str = "pcm"
    sample_rate: int = 16000
    channels: int = 1
    sample_width: int = 2

    def __post_init__(self):
        if self.encoding not in {"pcm", "wav", "mp3", "opus"}:
            raise ValueError("Unsupported audio encoding")
        for name in ("sample_rate", "channels", "sample_width"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True)
class Capabilities:
    streaming_input: bool = True
    streaming_output: bool = True
    encodings: tuple[str, ...] = ("pcm",)
    sample_rates: tuple[int, ...] = (16000,)
    hotwords: bool = False


@dataclass(frozen=True)
class SynthesisOptions:
    voice: str = "longanhuan_v3.1"
    audio_format: AudioFormat = field(default_factory=AudioFormat)
    parameters: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self.voice, str) or not self.voice.strip():
            raise ValueError("voice must not be empty")


@dataclass(frozen=True)
class SynthesisStarted:
    stream_id: str
    audio_format: AudioFormat


@dataclass(frozen=True)
class AudioChunk:
    data: bytes
    sequence: int


@dataclass(frozen=True)
class SynthesisCompleted:
    stream_id: str
    text_chars: int
    audio_bytes: int
    elapsed_seconds: float


TTSEvent = SynthesisStarted | AudioChunk | SynthesisCompleted
