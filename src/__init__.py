"""Streaming TTS. Core imports require only the Python standard library."""

from ._runtime import Observation, Observer, StreamLimits
from .client import TTSClient
from .errors import (
    BusyError,
    ProtocolError,
    ProviderError,
    SpeechError,
    SpeechTimeoutError,
    StreamClosedError,
    UnsupportedCapabilityError,
)
from .processors import TextProcessor
from .providers.base import TTSProvider, TTSSession
from .types import (
    AudioChunk,
    AudioFormat,
    Capabilities,
    SynthesisCompleted,
    SynthesisOptions,
    SynthesisStarted,
    TTSEvent,
)

__all__ = [
    "TextProcessor",
    "TTSClient",
    "TTSProvider",
    "TTSSession",
    "TTSEvent",
    "AudioFormat",
    "AudioChunk",
    "Capabilities",
    "SynthesisOptions",
    "SynthesisStarted",
    "SynthesisCompleted",
    "StreamLimits",
    "Observation",
    "Observer",
    "SpeechError",
    "ProviderError",
    "ProtocolError",
    "SpeechTimeoutError",
    "StreamClosedError",
    "UnsupportedCapabilityError",
    "BusyError",
]
