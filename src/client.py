from collections.abc import AsyncIterable, AsyncIterator, Sequence
from contextlib import aclosing

from ._runtime import BaseClient, Observer, StreamLimits
from .errors import UnsupportedCapabilityError
from .processors import TextProcessor
from .providers.base import TTSProvider
from .stream import TTSStream
from .types import SynthesisOptions, SynthesisStarted, TTSEvent


async def _single(text):
    yield text


class TTSClient(BaseClient):
    def __init__(
        self,
        provider: TTSProvider,
        *,
        text_processors: Sequence[TextProcessor] = (),
        limits: StreamLimits | None = None,
        observer: Observer | None = None,
    ):
        super().__init__(provider, limits=limits, observer=observer)
        self.text_processors = tuple(text_processors)

    async def synthesize(
        self,
        text: str | AsyncIterable[str],
        options: SynthesisOptions | None = None,
    ) -> AsyncIterator[TTSEvent]:
        options = options or SynthesisOptions()
        caps, fmt = self.capabilities, options.audio_format
        if not isinstance(text, str) and not isinstance(text, AsyncIterable):
            raise TypeError("text must be a string or an AsyncIterable[str]")
        if not isinstance(text, str) and not caps.streaming_input:
            raise UnsupportedCapabilityError("Provider does not support streaming text input")
        if (
            not caps.streaming_output
            or fmt.encoding not in caps.encodings
            or fmt.sample_rate not in caps.sample_rates
        ):
            raise UnsupportedCapabilityError("Provider does not support the requested streaming audio format")
        source = _single(text) if isinstance(text, str) else aiter(text)
        stream = TTSStream(self, options, source=source)
        async with stream:
            yield SynthesisStarted(stream.stream_id, fmt)
            async with aclosing(stream.events()) as events:
                async for event in events:
                    yield event
