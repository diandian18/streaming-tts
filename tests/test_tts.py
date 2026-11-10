import asyncio
import unittest
from contextlib import aclosing

from streaming_tts import (
    AudioChunk,
    AudioFormat,
    Capabilities,
    ProtocolError,
    ProviderError,
    SpeechTimeoutError,
    StreamLimits,
    SynthesisCompleted,
    SynthesisStarted,
    TTSClient,
    UnsupportedCapabilityError,
)
from streaming_tts._runtime import Channel
from streaming_tts.text_processing import SentenceBuffer, segment_text


class Session:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.sent = []
        self.closed = False
        self.finished = False
        self.block = None

    async def send(self, data):
        if self.block is not None:
            await self.block.wait()
        self.sent.append(data)
        await self.queue.put(self.result(data))

    async def finish_input(self):
        self.finished = True
        await self.queue.put(None)

    async def receive(self):
        value = await self.queue.get()
        if isinstance(value, Exception):
            raise value
        return value

    async def aclose(self):
        self.closed = True


class Provider:
    capabilities = Capabilities(hotwords=True)

    def __init__(self):
        self.sessions = []

    async def open_session(self, options, limits):
        session = Session()
        session.result = lambda text: text.encode()
        self.sessions.append(session)
        return session


class ChannelTests(unittest.IsolatedAsyncioTestCase):
    async def test_backpressure_and_failure_wakes_blocked_sender(self):
        channel = Channel(1)
        await channel.send("first")
        blocked = asyncio.create_task(channel.send("second"))
        await asyncio.sleep(0)
        self.assertFalse(blocked.done())
        await channel.finish(ProviderError("failed", code="upstream"))
        with self.assertRaises(ProviderError):
            await blocked
        with self.assertRaises(ProviderError):
            await channel.receive()

    async def test_completion_drains_buffer(self):
        channel = Channel(1)
        await channel.send("last")
        await channel.finish()
        self.assertEqual(await channel.receive(), "last")
        with self.assertRaises(StopAsyncIteration):
            await channel.receive()

    def test_limits_validate_deadlines_and_capacity(self):
        for kwargs in [{"buffer_size": 0}, {"idle_timeout": float("nan")}, {"close_timeout": -1}]:
            with self.assertRaises(ValueError):
                StreamLimits(**kwargs)
        with self.assertRaises(ValueError):
            AudioFormat(sample_rate=0)


class TTSTests(unittest.IsolatedAsyncioTestCase):
    async def test_audio_before_source_finishes(self):
        released, closed = asyncio.Event(), asyncio.Event()

        async def source():
            try:
                yield "first"
                await released.wait()
                yield "second"
            finally:
                closed.set()

        provider = Provider()
        async with TTSClient(provider) as client:
            async with aclosing(client.synthesize(source())) as events:
                self.assertIsInstance(await anext(events), SynthesisStarted)
                first = await anext(events)
                self.assertIsInstance(first, AudioChunk)
                self.assertEqual(first.data, b"first")
                self.assertFalse(released.is_set())
                released.set()
                rest = [event async for event in events]
                self.assertEqual([e.sequence for e in rest if isinstance(e, AudioChunk)], [2])
                self.assertIsInstance(rest[-1], SynthesisCompleted)
                self.assertEqual(rest[-1].text_chars, 11)
                self.assertEqual(rest[-1].audio_bytes, 11)
        self.assertTrue(closed.is_set())
        self.assertTrue(provider.sessions[0].closed)

    async def test_full_text_and_empty_source(self):
        async def empty():
            if False:
                yield ""

        for source, expected in [("hello", [b"hello"]), (empty(), [])]:
            async with TTSClient(Provider()) as client:
                result = [event async for event in client.synthesize(source)]
            self.assertIsInstance(result[0], SynthesisStarted)
            self.assertIsInstance(result[-1], SynthesisCompleted)
            self.assertEqual([e.data for e in result if isinstance(e, AudioChunk)], expected)

    async def test_early_close_cancels_source(self):
        closed = asyncio.Event()

        async def source():
            try:
                yield "first"
                await asyncio.Future()
            finally:
                closed.set()

        provider = Provider()
        async with TTSClient(provider) as client:
            async with aclosing(client.synthesize(source())) as events:
                await anext(events)
                await anext(events)
        self.assertTrue(closed.is_set())
        self.assertTrue(provider.sessions[0].closed)

    async def test_client_close_cleans_paused_generator(self):
        provider = Provider()
        client = TTSClient(provider)
        events = client.synthesize("hello")
        await anext(events)
        await client.aclose()
        self.assertTrue(provider.sessions[0].closed)
        await events.aclose()

    async def test_source_failure_propagates_and_closes(self):
        async def source():
            yield "first"
            raise RuntimeError("LLM disconnected")

        provider = Provider()
        async with TTSClient(provider) as client:
            with self.assertRaises(ProviderError) as caught:
                async for _ in client.synthesize(source()):
                    pass
            self.assertIsInstance(caught.exception.__cause__, RuntimeError)
        self.assertTrue(provider.sessions[0].closed)

    async def test_streaming_capability_is_not_silently_emulated(self):
        provider = Provider()
        provider.capabilities = Capabilities(streaming_input=False)

        async def source():
            yield "hello"

        async with TTSClient(provider) as client:
            with self.assertRaises(UnsupportedCapabilityError):
                await anext(client.synthesize(source()))
            events = [e async for e in client.synthesize("hello")]
            self.assertIsInstance(events[-1], SynthesisCompleted)

    async def test_text_limit_and_output_limit(self):
        provider = Provider()
        limits = StreamLimits(max_text_chars=4)
        async with TTSClient(provider, limits=limits) as client:
            with self.assertRaises(ProviderError):
                async for _ in client.synthesize("hello"):
                    pass
        provider = Provider()
        async with TTSClient(provider, limits=StreamLimits(max_chunk_bytes=2)) as client:
            with self.assertRaises(ProtocolError):
                async for _ in client.synthesize("hello"):
                    pass

    async def test_timeout_closes_pending_source(self):
        closed = asyncio.Event()

        async def source():
            try:
                await asyncio.Future()
                yield "never"
            finally:
                closed.set()

        provider = Provider()
        async with TTSClient(provider, limits=StreamLimits(input_timeout=0.02)) as client:
            with self.assertRaises(SpeechTimeoutError):
                async for _ in client.synthesize(source()):
                    pass
        self.assertTrue(closed.is_set())
        self.assertTrue(provider.sessions[0].closed)

    async def test_concurrent_streams_do_not_mix_audio(self):
        async with TTSClient(Provider()) as client:

            async def collect(text):
                events = [e async for e in client.synthesize(text)]
                return b"".join(e.data for e in events if isinstance(e, AudioChunk))

            self.assertEqual(await asyncio.gather(collect("one"), collect("two")), [b"one", b"two"])


class SentenceTests(unittest.IsolatedAsyncioTestCase):
    def test_punctuation_and_length_preserve_text(self):
        buffer = SentenceBuffer(max_chars=4)
        text = "你好。abcdefgh!尾巴"
        chunks = buffer.feed(text)
        chunks.append(buffer.flush())
        self.assertEqual("".join(chunks), text)
        self.assertTrue(all(len(chunk) <= 4 for chunk in chunks))

    async def test_timer_flush_does_not_cancel_pending_source_read(self):
        released, cancelled = asyncio.Event(), asyncio.Event()

        async def source():
            yield "hello"
            try:
                await released.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            yield "world."

        async with aclosing(segment_text(source(), max_wait=0.02)) as chunks:
            self.assertEqual(await asyncio.wait_for(anext(chunks), 1), "hello")
            self.assertFalse(cancelled.is_set())
            released.set()
            self.assertEqual([item async for item in chunks], ["world."])

    async def test_close_releases_source(self):
        closed = asyncio.Event()

        async def source():
            try:
                yield "hello"
                await asyncio.Future()
            finally:
                closed.set()

        async with aclosing(segment_text(source(), max_wait=0.01)) as chunks:
            self.assertEqual(await anext(chunks), "hello")
        self.assertTrue(closed.is_set())


class ProcessorTests(unittest.IsolatedAsyncioTestCase):
    async def test_text_processor_transforms_input_before_synthesis(self):
        async def clean(text):
            return text.strip() or None

        provider = Provider()

        async def source():
            yield "  "
            yield " hello "

        async with TTSClient(provider, text_processors=[clean]) as client:
            events = [e async for e in client.synthesize(source())]
        self.assertEqual(provider.sessions[0].sent, ["hello"])
        self.assertEqual(events[-1].text_chars, 5)

    async def test_empty_fragments_do_not_extend_input_deadline(self):
        async def source():
            while True:
                yield ""
                await asyncio.sleep(0.001)

        provider = Provider()
        async with TTSClient(provider, limits=StreamLimits(input_timeout=0.02)) as client:
            with self.assertRaises(SpeechTimeoutError):
                async for _ in client.synthesize(source()):
                    pass
        self.assertTrue(provider.sessions[0].closed)


class CancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_task_cancellation_propagates_and_releases_connection(self):
        entered, closed = asyncio.Event(), asyncio.Event()

        async def source():
            try:
                entered.set()
                await asyncio.Future()
                yield "never"
            finally:
                closed.set()

        provider = Provider()
        async with TTSClient(provider) as client:

            async def consume():
                async for _ in client.synthesize(source()):
                    pass

            task = asyncio.create_task(consume())
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(closed.is_set())
        self.assertTrue(provider.sessions[0].closed)

    async def test_slow_consumer_applies_backpressure_and_can_cancel(self):
        provider = Provider()
        produced = 0

        async def source():
            nonlocal produced
            for _ in range(100):
                produced += 1
                yield "hello"

        async with TTSClient(provider, limits=StreamLimits(buffer_size=1)) as client:
            async with aclosing(client.synthesize(source())) as events:
                await anext(events)
                # A provider must also be bounded. This fake's queue is limited here.
                provider.sessions[0].queue = asyncio.Queue(maxsize=1)
                await asyncio.sleep(0.01)
                self.assertLess(produced, 100)
        self.assertTrue(provider.sessions[0].closed)
