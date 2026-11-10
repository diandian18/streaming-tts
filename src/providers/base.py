from typing import Protocol

from .._runtime import StreamLimits
from ..types import Capabilities, SynthesisOptions


class TTSSession(Protocol):
    async def send(self, text: str) -> None: ...
    async def finish_input(self) -> None: ...
    async def receive(self) -> bytes | None: ...
    async def aclose(self) -> None: ...


class TTSProvider(Protocol):
    capabilities: Capabilities

    async def open_session(self, options: SynthesisOptions, limits: StreamLimits) -> TTSSession: ...
