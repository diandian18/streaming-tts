"""Any LLM implementation can provide the async strings in text_stream()."""

import asyncio
import wave
from contextlib import aclosing

from streaming_tts import AudioChunk, TTSClient
from streaming_tts.providers.qwen import QwenConfig, QwenProvider
from streaming_tts.text_processing import segment_text


async def text_stream():
    for text in ["你好，", "这是独立的语音合成库。", "文本还在持续输入。"]:
        yield text
        await asyncio.sleep(0.1)


async def main():
    async with TTSClient(QwenProvider(QwenConfig.from_env())) as client:
        with wave.open("output.wav", "wb") as audio:
            audio.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
            async with aclosing(client.synthesize(segment_text(text_stream()))) as events:
                async for event in events:
                    if isinstance(event, AudioChunk):
                        audio.writeframesraw(event.data)


if __name__ == "__main__":
    asyncio.run(main())
