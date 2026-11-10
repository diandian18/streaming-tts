from __future__ import annotations

import asyncio
from contextlib import aclosing

from .._runtime import Channel
from ..errors import ProtocolError, SpeechError, StreamClosedError
from .protocol import CancelInput, DefaultCodec, FinishInput, TextInput


async def _exchange(receive, send, connection, codec):
    tasks = [asyncio.create_task(receive), asyncio.create_task(send)]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        cancelled = False
        for task in done:
            cancelled = (await task) == "cancel" or cancelled
        if cancelled:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for message in codec.encode_cancelled():
                await connection.send(message)
    except SpeechError as error:
        # Stop output first so that no audio is sent after the error terminal.
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for message in codec.encode_error(error):
            await connection.send(message)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def serve_connection(connection, client, *, options=None, codec=None):
    """Serve one task per client connection; the caller owns client and socket.

    Accepts any object implementing async recv()/send(). A codec may map an
    event to zero, one or many text/binary messages. No web framework is required.
    """
    codec = codec or DefaultCodec()
    inputs = Channel(client.limits.buffer_size)

    async def source():
        while True:
            try:
                yield await inputs.receive()
            except StopAsyncIteration:
                return

    async def receive():
        while True:
            command = codec.decode_message(await connection.recv())
            if isinstance(command, TextInput):
                if len(command.text) > client.limits.max_text_chars:
                    raise ProtocolError("Text chunk exceeds max_text_chars")
                await inputs.send(command.text)
            elif isinstance(command, FinishInput):
                await inputs.finish()
            elif isinstance(command, CancelInput):
                return "cancel"
            else:
                raise ProtocolError("Codec returned an invalid input command")

    async def send():
        async with aclosing(client.synthesize(source(), options)) as events:
            async for event in events:
                for message in codec.encode_event(event):
                    await connection.send(message)

    try:
        await _exchange(receive(), send(), connection, codec)
    finally:
        await inputs.finish(StreamClosedError())
