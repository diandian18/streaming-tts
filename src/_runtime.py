"""Private bounded streaming runtime. Each distribution owns its own copy."""

from __future__ import annotations

import asyncio
import logging
import math
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from time import monotonic
from typing import Any
from uuid import uuid4

from .errors import (
    BusyError,
    ProtocolError,
    ProviderError,
    SpeechError,
    SpeechTimeoutError,
    StreamClosedError,
)


@dataclass(frozen=True)
class StreamLimits:
    buffer_size: int = 16
    max_chunk_bytes: int = 65536
    max_text_chars: int = 20000
    max_total_chars: int = 200000
    max_streams: int = 8
    start_timeout: float = 10.0
    input_timeout: float = 20.0
    send_timeout: float = 10.0
    idle_timeout: float = 30.0
    finish_timeout: float = 15.0
    total_timeout: float = 300.0
    close_timeout: float = 2.0
    observer_timeout: float = 0.1

    def __post_init__(self):
        for name, value in vars(self).items():
            if name.endswith("timeout"):
                if not math.isfinite(value) or value <= 0:
                    raise ValueError(f"{name} must be finite and positive")
            elif not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True)
class Observation:
    kind: str
    stream_id: str
    elapsed_seconds: float
    input_units: int = 0
    output_units: int = 0


Observer = Callable[[Observation], Awaitable[None]]


class Channel:
    """Completion/errors are out of band, so a full buffer cannot hide failure."""

    def __init__(self, size: int):
        self._items = deque()
        self._size = size
        self._condition = asyncio.Condition()
        self.ended = False
        self.error = None

    async def send(self, item: Any) -> None:
        async with self._condition:
            await self._condition.wait_for(lambda: self.ended or len(self._items) < self._size)
            if self.ended:
                raise self.error or StreamClosedError()
            self._items.append(item)
            self._condition.notify_all()

    async def finish(self, error: Exception | None = None) -> None:
        async with self._condition:
            if not self.ended:
                self.ended, self.error = True, error
                if error is not None:
                    self._items.clear()
            self._condition.notify_all()

    async def receive(self) -> Any:
        async with self._condition:
            await self._condition.wait_for(lambda: self.ended or self._items)
            if self.error is not None:
                raise self.error
            if self._items:
                item = self._items.popleft()
                self._condition.notify_all()
                return item
            raise StopAsyncIteration


class Observations:
    def __init__(self, callback: Observer | None, stream, timeout: float):
        self.callback, self.stream, self.timeout = callback, stream, timeout
        self.queue = asyncio.Queue(maxsize=32)
        self.task = None
        self.dropped = 0

    def emit(self, kind: str) -> None:
        if self.callback is None:
            return
        if self.task is None:
            self.task = asyncio.create_task(self._run(), name="speech-observer")
        try:
            self.queue.put_nowait(
                Observation(
                    kind,
                    self.stream.stream_id,
                    monotonic() - self.stream.started_at,
                    self.stream.input_units,
                    self.stream.output_units,
                )
            )
        except asyncio.QueueFull:
            self.dropped += 1

    async def _run(self):
        while True:
            event = await self.queue.get()
            try:
                async with asyncio.timeout(self.timeout):
                    await self.callback(event)
            except Exception:
                logging.getLogger(__name__).warning("Speech observer failed", exc_info=True)
            finally:
                self.queue.task_done()

    async def aclose(self):
        if self.task is not None:
            try:
                async with asyncio.timeout(self.timeout):
                    await self.queue.join()
            except TimeoutError:
                pass
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)


class BaseClient:
    def __init__(self, provider, *, limits: StreamLimits | None = None, observer: Observer | None = None):
        self.provider = provider
        self.limits = limits or StreamLimits()
        self.observer = observer
        self._streams = set()
        self._closed = False

    @property
    def capabilities(self):
        return self.provider.capabilities

    def _attach(self, stream):
        if self._closed:
            raise StreamClosedError("Client is closed")
        if len(self._streams) >= self.limits.max_streams:
            raise BusyError()
        self._streams.add(stream)

    async def __aenter__(self):
        if self._closed:
            raise StreamClosedError("Client is closed")
        return self

    async def __aexit__(self, *_):
        await self.aclose()

    async def aclose(self):
        self._closed = True
        await asyncio.gather(*(s.aclose() for s in tuple(self._streams)))


class BaseStream:
    def __init__(self, client: BaseClient, options, *, source: AsyncIterator | None = None):
        self.client, self.options, self.source = client, options, source
        self.limits = client.limits
        self.stream_id = str(uuid4())
        self.started_at = monotonic()
        self.input_units = self.output_units = 0
        self.inputs = Channel(self.limits.buffer_size)
        self.outputs = Channel(self.limits.buffer_size)
        self.session = self.worker = self._opening = self._cleanup_task = None
        self._closed = self._claimed = self.finishing = self._cleaning = False
        self.observations = Observations(client.observer, self, self.limits.observer_timeout)

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *_):
        await self.aclose()

    async def start(self):
        if self._closed:
            raise StreamClosedError()
        if self._opening is not None or self.worker is not None:
            raise RuntimeError("Stream has already been started")
        self.client._attach(self)
        self.started_at = monotonic()
        try:
            self._opening = asyncio.create_task(self.client.provider.open_session(self.options, self.limits))
            async with asyncio.timeout(self.limits.start_timeout):
                self.session = await self._opening
            self.observations.emit("started")
            self.worker = asyncio.create_task(self._run(), name=f"speech-{self.stream_id}")
            return self
        except BaseException as exc:
            await self.aclose()
            if isinstance(exc, TimeoutError):
                raise SpeechTimeoutError("Stream startup timed out") from exc
            if isinstance(exc, (SpeechError, ValueError, asyncio.CancelledError)):
                raise
            raise ProviderError("Could not start provider session", code="connection_error") from exc

    def _ensure_started(self):
        if self.worker is None or self._closed:
            raise StreamClosedError("Stream is not running")

    async def _send(self):
        iterator = self.source
        input_deadline = monotonic() + self.limits.input_timeout
        while True:
            try:
                async with asyncio.timeout_at(input_deadline):
                    item = await (anext(iterator) if iterator is not None else self.inputs.receive())
            except StopAsyncIteration:
                break
            item = self._validate_input(item)
            async with asyncio.timeout_at(input_deadline):
                item = await self._process_input(item)
            if item is not None:
                item = self._validate_input(item)
            if not item:
                await asyncio.sleep(0)
                continue
            async with asyncio.timeout(self.limits.send_timeout):
                await self.session.send(item)
            self.input_units += len(item)
            input_deadline = monotonic() + self.limits.input_timeout
            self.observations.emit("input")
        self.finishing = True
        async with asyncio.timeout(self.limits.send_timeout):
            await self.session.finish_input()

    async def _process_input(self, item):
        return item

    async def _process_output(self, event):
        return event

    async def _receive(self):
        while True:
            async with asyncio.timeout(self.limits.idle_timeout):
                event = await self.session.receive()
            if event is None:
                if not self.finishing:
                    raise ProtocolError("Provider completed before input was finished")
                return
            async with asyncio.timeout(self.limits.send_timeout):
                event = await self._process_output(event)
            if event is None:
                continue
            event = self._convert_output(event)
            await self.outputs.send(event)
            self.observations.emit("output")

    async def _run(self):
        sender = asyncio.create_task(self._send(), name="speech-send")
        receiver = asyncio.create_task(self._receive(), name="speech-receive")
        try:
            async with asyncio.timeout(self.limits.total_timeout):
                done, _ = await asyncio.wait((sender, receiver), return_when=asyncio.FIRST_COMPLETED)
                if sender in done:
                    await sender
                    async with asyncio.timeout(self.limits.finish_timeout):
                        await receiver
                else:
                    await receiver
                    await sender
                await self.outputs.send(self._completion())
                await self.outputs.finish()
            self.observations.emit("completed")
        except asyncio.CancelledError:
            await self.outputs.finish(StreamClosedError())
            self.observations.emit("cancelled")
            raise
        except Exception as exc:
            if isinstance(exc, TimeoutError):
                error = SpeechTimeoutError("Streaming deadline exceeded")
                error.__cause__ = exc
            elif isinstance(exc, SpeechError):
                error = exc
            else:
                error = ProviderError("Streaming operation failed", code="stream_error")
                error.__cause__ = exc
            await self.outputs.finish(error)
            self.observations.emit("failed")
        finally:
            self._cleaning = True
            sender.cancel()
            receiver.cancel()
            await asyncio.gather(sender, receiver, return_exceptions=True)
            await self.inputs.finish(StreamClosedError())
            await self._cleanup()
            self.client._streams.discard(self)

    async def _cleanup(self):
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_resources(), name="speech-cleanup")
        await asyncio.shield(self._cleanup_task)

    async def _cleanup_resources(self):
        try:
            async with asyncio.timeout(self.limits.close_timeout):
                if self.source is not None and hasattr(self.source, "aclose"):
                    await self.source.aclose()
        except Exception:
            logging.getLogger(__name__).debug("Could not close text source", exc_info=True)
        try:
            async with asyncio.timeout(self.limits.close_timeout):
                if self.session is not None:
                    await self.session.aclose()
        except Exception:
            logging.getLogger(__name__).debug("Could not close speech session", exc_info=True)
        await self.observations.aclose()

    async def events(self):
        self._ensure_started()
        if self._claimed:
            raise RuntimeError("Only one event consumer is allowed per stream")
        self._claimed = True
        try:
            while True:
                try:
                    event = await self.outputs.receive()
                except StopAsyncIteration:
                    return
                yield event
        finally:
            await self.aclose()

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        await self.outputs.finish(StreamClosedError())
        await self.inputs.finish(StreamClosedError())
        if self._opening is not None:
            if not self._opening.done():
                self._opening.cancel()
            await asyncio.gather(self._opening, return_exceptions=True)
            # Cancellation can arrive after open_session returns but before start()
            # assigns the result. Recover ownership before closing the session.
            if self.session is None and not self._opening.cancelled() and self._opening.exception() is None:
                self.session = self._opening.result()
        if self.worker is not None:
            if not self.worker.done() and not self._cleaning:
                self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
        await self._cleanup()
        self.client._streams.discard(self)
