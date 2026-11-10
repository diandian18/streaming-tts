import asyncio
import json
import unittest

from streaming_tts import (
    AudioChunk,
    ProtocolError,
    ProviderError,
    SynthesisCompleted,
    SynthesisStarted,
    TTSClient,
)
from streaming_tts.providers.qwen import QwenConfig, QwenProvider
from websockets.asyncio.server import serve


def response(task_id, event, payload=None, **header):
    return json.dumps({"header": {"task_id": task_id, "event": event, **header}, "payload": payload or {}})


class WireTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_websocket_task_flow(self):
        requests = []
        disconnected = asyncio.Event()

        async def handler(socket):
            try:
                request = json.loads(await socket.recv())
                requests.append(request)
                self.assertEqual(socket.request.headers["Authorization"], "Bearer test-key")
                task_id = request["header"]["task_id"]
                self.assertEqual(request["header"]["action"], "run-task")
                await socket.send(response(task_id, "task-started"))
                data = await socket.recv()
                requests.append(data)
                self.assertEqual(json.loads(data)["payload"]["input"]["text"], "hello")
                await socket.send(b"\x01\x00")
                finish = json.loads(await socket.recv())
                self.assertEqual(finish["header"]["action"], "finish-task")
                self.assertEqual(finish["header"]["task_id"], task_id)
                await socket.send(response(task_id, "task-finished"))
                await socket.wait_closed()
            finally:
                disconnected.set()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            config = QwenConfig(api_key="test-key", url=f"ws://127.0.0.1:{port}", allow_insecure=True)
            provider = QwenProvider(config)
            async with TTSClient(provider) as client:
                events = [e async for e in client.synthesize("hello")]
                self.assertIsInstance(events[0], SynthesisStarted)
                self.assertEqual(events[1], AudioChunk(b"\x01\x00", 1))
                self.assertIsInstance(events[-1], SynthesisCompleted)
            await asyncio.wait_for(disconnected.wait(), 1)
        self.assertEqual(requests[0]["payload"]["task"], "tts")

    async def test_start_failure_retains_code_and_closes_connection(self):
        disconnected = asyncio.Event()

        async def handler(socket):
            task_id = json.loads(await socket.recv())["header"]["task_id"]
            await socket.send(
                response(
                    task_id, "task-failed", error_code="InvalidApiKey", error_message="invalid credential"
                )
            )
            await socket.wait_closed()
            disconnected.set()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            config = QwenConfig(api_key="test", url=f"ws://127.0.0.1:{port}", allow_insecure=True)
            async with TTSClient(QwenProvider(config)) as client:
                with self.assertRaises(ProviderError) as caught:
                    await anext(client.synthesize("hello"))
                self.assertEqual(caught.exception.code, "InvalidApiKey")
            await asyncio.wait_for(disconnected.wait(), 1)

    async def test_wrong_task_id_is_rejected(self):
        async def handler(socket):
            await socket.recv()
            await socket.send(response("another-task", "task-started"))
            await socket.wait_closed()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            config = QwenConfig(api_key="test", url=f"ws://127.0.0.1:{port}", allow_insecure=True)
            async with TTSClient(QwenProvider(config)) as client:
                with self.assertRaises(ProtocolError):
                    await anext(client.synthesize("hello"))

    def test_config_hides_key_and_requires_secure_url(self):
        self.assertNotIn("secret", repr(QwenConfig(api_key="secret")))
        with self.assertRaises(ValueError):
            QwenConfig(api_key="secret", url="ws://example.com")
