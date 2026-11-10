from time import monotonic

from ._runtime import BaseStream
from .errors import ProtocolError
from .types import AudioChunk, SynthesisCompleted


class TTSStream(BaseStream):
    """Private per-synthesis runtime; clients expose a managed async iterator."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sequence = 0

    def _validate_input(self, text):
        if not isinstance(text, str):
            raise TypeError("Text stream must yield strings")
        if (
            len(text) > self.limits.max_text_chars
            or self.input_units + len(text) > self.limits.max_total_chars
        ):
            raise ValueError("Text exceeds configured character limits")
        return text

    def _convert_output(self, audio):
        if not isinstance(audio, bytes) or not audio or len(audio) > self.limits.max_chunk_bytes:
            raise ProtocolError("TTS provider returned an empty, invalid or oversized audio chunk")
        self._sequence += 1
        self.output_units += len(audio)
        return AudioChunk(audio, self._sequence)

    async def _process_input(self, text):
        for processor in self.client.text_processors:
            text = await processor(text)
            if text is None:
                return None
            if not isinstance(text, str):
                raise TypeError("Text processors must return str or None")
        return text

    def _completion(self):
        return SynthesisCompleted(
            self.stream_id, self.input_units, self.output_units, monotonic() - self.started_at
        )
