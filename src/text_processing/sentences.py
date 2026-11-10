"""Optional segmentation. Preserves text and never collects the entire source."""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterable, AsyncIterator
from time import monotonic


class SentenceBuffer:
    def __init__(self, *, min_chars: int = 1, max_chars: int = 200, punctuation: str = "。！？!?；;\n."):
        if not 1 <= min_chars <= max_chars:
            raise ValueError("Require 1 <= min_chars <= max_chars")
        self.min_chars, self.max_chars, self.punctuation = min_chars, max_chars, punctuation
        self.pending = ""

    def feed(self, text: str) -> list[str]:
        if not isinstance(text, str):
            raise TypeError("Text stream must yield strings")
        self.pending += text
        result = []
        while self.pending:
            boundary = next(
                (
                    i + 1
                    for i, char in enumerate(self.pending[: self.max_chars])
                    if char in self.punctuation and i + 1 >= self.min_chars
                ),
                None,
            )
            if boundary is None:
                if len(self.pending) < self.max_chars:
                    break
                boundary = self.max_chars
            result.append(self.pending[:boundary])
            self.pending = self.pending[boundary:]
        return result

    def flush(self) -> str:
        result, self.pending = self.pending, ""
        return result


async def segment_text(
    source: AsyncIterable[str],
    *,
    min_chars: int = 1,
    max_chars: int = 200,
    max_wait: float = 0.3,
    punctuation: str = "。！？!?；;\n.",
) -> AsyncIterator[str]:
    """Flush on punctuation, length or elapsed waiting time.

    Waiting-time flushes don't cancel/restart a pending source read. Closing this
    iterator also closes its owned source iterator when that supports aclose().
    """
    if not math.isfinite(max_wait) or max_wait <= 0:
        raise ValueError("max_wait must be finite and positive")
    buffer = SentenceBuffer(min_chars=min_chars, max_chars=max_chars, punctuation=punctuation)
    iterator, pending, deadline = aiter(source), None, None

    async def read_next():
        return await anext(iterator)

    try:
        while True:
            if buffer.pending and deadline is not None and monotonic() >= deadline:
                yield buffer.flush()
                deadline = None
            if pending is None:
                pending = asyncio.create_task(read_next())
            timeout = max(0, deadline - monotonic()) if deadline is not None else None
            done, _ = await asyncio.wait((pending,), timeout=timeout)
            if not done:
                continue
            try:
                text = pending.result()
            except StopAsyncIteration:
                if buffer.pending:
                    yield buffer.flush()
                return
            pending = None
            for sentence in buffer.feed(text):
                yield sentence
            if buffer.pending:
                deadline = deadline or monotonic() + max_wait
            else:
                deadline = None
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        if hasattr(iterator, "aclose"):
            await iterator.aclose()
