from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Protocol

from ..errors import ProtocolError, SpeechError


@dataclass(frozen=True)
class FinishInput:
    pass


@dataclass(frozen=True)
class CancelInput:
    pass


class WebSocketConnection(Protocol):
    async def recv(self) -> str | bytes: ...
    async def send(self, message: str | bytes) -> None: ...


class MessageCodec(Protocol):
    def decode_message(self, message: str | bytes) -> InputCommand: ...
    def encode_event(self, event) -> Iterable[str | bytes]: ...
    def encode_error(self, error: SpeechError) -> Iterable[str | bytes]: ...
    def encode_cancelled(self) -> Iterable[str | bytes]: ...


def _control(message):
    try:
        data = json.loads(message)
        if not isinstance(data, dict):
            raise ValueError("Expected object")
        if data.get("type") == "finish":
            return FinishInput()
        if data.get("type") == "cancel":
            return CancelInput()
        return data
    except (ValueError, TypeError) as exc:
        raise ProtocolError("Expected a JSON control object") from exc


def _json(value):
    return json.dumps(value, ensure_ascii=False)


@dataclass(frozen=True)
class TextInput:
    text: str


InputCommand = TextInput | FinishInput | CancelInput


class DefaultCodec:
    """JSON text/control input; JSON lifecycle messages and raw binary PCM output."""

    def decode_message(self, message):
        if not isinstance(message, str):
            raise ProtocolError("TTS input must be JSON text")
        command = _control(message)
        if isinstance(command, (FinishInput, CancelInput)):
            return command
        if command.get("type") == "text" and isinstance(command.get("text"), str):
            return TextInput(command["text"])
        raise ProtocolError("Expected text, finish or cancel")

    def encode_event(self, event):
        from ..types import AudioChunk, SynthesisCompleted, SynthesisStarted

        if isinstance(event, AudioChunk):
            return [event.data]
        names = {SynthesisStarted: "synthesis.started", SynthesisCompleted: "synthesis.completed"}
        return [_json({"type": names[type(event)], **asdict(event)})]

    def encode_error(self, error):
        return [_json({"type": "error", "code": error.code, "message": str(error)})]

    def encode_cancelled(self):
        return [_json({"type": "cancelled"})]
