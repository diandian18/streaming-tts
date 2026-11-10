"""Run from the checkout: python main.py synthesize '你好' / serve."""

import argparse
import asyncio
import wave
from contextlib import aclosing
from pathlib import Path

from src import AudioChunk, SynthesisOptions, TTSClient
from src.integrations import serve_connection
from src.providers.qwen import QwenConfig, QwenProvider
from src.text_processing import segment_text


async def synthesize(client, args):
    text = Path(args.text_file).read_text() if args.text_file else args.text
    if text is None:
        raise ValueError("Provide text or --text-file")

    async def deltas():
        for offset in range(0, len(text), 8):
            yield text[offset : offset + 8]
            await asyncio.sleep(args.delay)

    source = segment_text(deltas()) if args.stream else text
    with wave.open(args.output, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(16000)
        async with aclosing(client.synthesize(source, SynthesisOptions(voice=args.voice))) as events:
            async for event in events:
                if isinstance(event, AudioChunk):
                    audio.writeframesraw(event.data)
    print(f"Wrote {args.output}")


async def run(args):
    async with TTSClient(QwenProvider(QwenConfig.from_env())) as client:
        if args.command == "synthesize":
            await synthesize(client, args)
        else:
            from websockets.asyncio.server import serve

            async def handle(socket):
                await serve_connection(socket, client)

            async with serve(handle, args.host, args.port, max_size=65536, max_queue=16):
                print(f"TTS WebSocket: ws://{args.host}:{args.port}", flush=True)
                await asyncio.Future()


def main():
    parser = argparse.ArgumentParser(description="Independent streaming TTS")
    commands = parser.add_subparsers(dest="command", required=True)
    synthesis = commands.add_parser("synthesize", help="Synthesize a WAV file")
    synthesis.add_argument("text", nargs="?")
    synthesis.add_argument("--text-file")
    synthesis.add_argument("--output", default="output.wav")
    synthesis.add_argument("--voice", default="longanhuan_v3.1")
    synthesis.add_argument(
        "--stream", action="store_true", help="Demonstrate incremental text + segmentation"
    )
    synthesis.add_argument(
        "--delay", type=float, default=0.05, help="Delay between demonstration text deltas"
    )
    server = commands.add_parser("serve", help="Run the example WebSocket server")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()
    if args.command == "synthesize" and args.delay < 0:
        parser.error("--delay must be nonnegative")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
