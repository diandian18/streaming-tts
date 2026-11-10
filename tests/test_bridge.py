import asyncio
import json
import unittest

from streaming_tts import ProtocolError, TTSClient
from streaming_tts.integrations import DefaultCodec, serve_connection

from .test_tts import Provider


class Connection:
    def __init__(self):
        self.incoming = asyncio.Queue()
        self.outgoing = asyncio.Queue()

    async def recv(self):
        return await self.incoming.get()

    async def send(self, message):
        await self.outgoing.put(message)


class BridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_full_bridge_and_custom_event_mapping(self):
        class CustomCodec(DefaultCodec):
            def encode_event(self, event):
                return ["custom"] + list(super().encode_event(event))

        connection = Connection()
        provider = Provider()
        async with TTSClient(provider) as client:
            task = asyncio.create_task(serve_connection(connection, client, codec=CustomCodec()))
            await connection.incoming.put(json.dumps({"type": "text", "text": "hello"}))
            await connection.incoming.put(json.dumps({"type": "finish"}))
            await asyncio.wait_for(task, 1)
        messages = []
        while not connection.outgoing.empty():
            messages.append(connection.outgoing.get_nowait())
        self.assertIn("custom", messages)
        events = [json.loads(m) for m in messages if isinstance(m, str) and m != "custom"]
        self.assertEqual(events[-1]["type"], "synthesis.completed")
        self.assertTrue(provider.sessions[0].closed)

    async def test_explicit_cancel_sends_one_terminal_and_cleans_up(self):
        connection = Connection()
        provider = Provider()
        async with TTSClient(provider) as client:
            task = asyncio.create_task(serve_connection(connection, client))
            await connection.incoming.put(json.dumps({"type": "cancel"}))
            await asyncio.wait_for(task, 1)
        messages = []
        while not connection.outgoing.empty():
            messages.append(connection.outgoing.get_nowait())
        self.assertEqual(json.loads(messages[-1])["type"], "cancelled")
        self.assertEqual(
            sum(isinstance(m, str) and json.loads(m).get("type") == "cancelled" for m in messages), 1
        )
        self.assertTrue(all(session.closed for session in provider.sessions))

    async def test_invalid_input_emits_error_and_cleans_up(self):
        connection = Connection()
        provider = Provider()
        async with TTSClient(provider) as client:
            task = asyncio.create_task(serve_connection(connection, client))
            await connection.incoming.put("not json")
            await asyncio.wait_for(task, 1)
        messages = []
        while not connection.outgoing.empty():
            messages.append(connection.outgoing.get_nowait())
        self.assertEqual(json.loads(messages[-1])["type"], "error")
        self.assertTrue(all(session.closed for session in provider.sessions))

    def test_invalid_control_rejected(self):
        codec = DefaultCodec()
        for message in ["[]", '{"type":"unknown"}']:
            with self.assertRaises(ProtocolError):
                codec.decode_message(message)
