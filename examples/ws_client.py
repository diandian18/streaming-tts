"""Connect to `python main.py serve` and write returned PCM into a WAV file."""

import asyncio
import json
import wave

from websockets.asyncio.client import connect


async def main():
    async with connect("ws://127.0.0.1:8766") as socket:

        async def send():
            for text in ["你好。", "这段音频来自双向流式合成。"]:
                await socket.send(json.dumps({"type": "text", "text": text}))
                await asyncio.sleep(0.2)
            await socket.send(json.dumps({"type": "finish"}))

        async def receive():
            with wave.open("output.wav", "wb") as audio:
                while True:
                    message = await socket.recv()
                    if isinstance(message, bytes):
                        audio.writeframesraw(message)
                        continue
                    event = json.loads(message)
                    if event["type"] == "synthesis.started":
                        fmt = event["audio_format"]
                        audio.setparams(
                            (
                                fmt["channels"],
                                fmt["sample_width"],
                                fmt["sample_rate"],
                                0,
                                "NONE",
                                "not compressed",
                            )
                        )
                    if event["type"] in {"synthesis.completed", "cancelled", "error"}:
                        print(event)
                        return

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(send())
            tasks.create_task(receive())


if __name__ == "__main__":
    asyncio.run(main())
