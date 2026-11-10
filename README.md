# streaming-tts

独立的异步流式语音合成库。支持完整文本和 `AsyncIterable[str]`，可以持续输入文本、同时读取音频。
Python 3.12+。核心仅依赖标准库，Qwen 适配器按需安装 `websockets`。

## 安装与运行

在项目目录中执行：

```bash
uv sync --extra qwen --extra dev
export DASHSCOPE_API_KEY='你的 API Key'
uv run main.py synthesize '你好，这是流式语音合成。' --output output.wav
uv run main.py synthesize '第一句。第二句还在生成。' --stream --output output.wav
uv run main.py synthesize --text-file /path/to/text.txt --stream --output output.wav
```

安装到其他应用：`pip install '.[qwen]'`；只使用自定义 Provider：`pip install .`。

默认适配 `qwen-audio-3.1-tts-flash` 的双向 task 协议，默认音色 `longanhuan_v3.1`。
可以通过 `TTS_MODEL`、`TTS_WS_URL` 修改模型和端点，凭证、音色和端点应属于相同区域。
模型必须兼容同一套 task 协议；这里未实现 Qwen-TTS-Realtime 的 session 协议。

适配器输出单声道 s16le PCM，采样率支持 8000/16000/22050/24000/44100/48000，默认 16000。
根目录演示程序使用 16000Hz，写入 WAV 容器；核心音频事件中仍是原始 PCM。

## 库接口

```python
from contextlib import aclosing

from streaming_tts import AudioChunk, SynthesisOptions, TTSClient
from streaming_tts.providers.qwen import QwenConfig, QwenProvider


async def speak(text_source):
    provider = QwenProvider(QwenConfig.from_env())
    options = SynthesisOptions(voice="longanhuan_v3.1", parameters={"rate": 1.1})
    async with TTSClient(provider) as client:
        async with aclosing(client.synthesize(text_source, options)) as events:
            async for event in events:
                if isinstance(event, AudioChunk):
                    await send_to_player(event.data)
```

`text_source` 可以是完整字符串，也可以是任意框架提供的异步正文字符串流。
核心不解释工具参数、模型消息或业务字段。

事件：

- `SynthesisStarted(stream_id, audio_format)`：任务已启动，声明音频格式。
- `AudioChunk(data, sequence)`：原始音频块，序号从 1 开始。
- `SynthesisCompleted(stream_id, text_chars, audio_bytes, elapsed_seconds)`：任务正常结束和统计。

`client.capabilities.streaming_input` 明确声明是否支持持续输入文本。Provider 不支持时，
传入异步文本流会抛 `UnsupportedCapabilityError`，不会默默收齐全文。
`parameters` 可以传 `volume/rate/pitch/language_hints/instruction` 等厂商选项。
`format/sample_rate/voice/text_type/enable_ssml` 由类型化选项和适配器管理，不能覆盖。

## 可选分句与文本处理

```python
from streaming_tts.text_processing import segment_text

sentences = segment_text(text_deltas, min_chars=1, max_chars=200, max_wait=0.3)
# client.synthesize(sentences)
```

按标点、长度或等待时间输出片段，保持输入文本内容和顺序；时间刷新不会取消正在进行的文本读取。
调用方已经提供合适句段时，直接传入即可。同步场景可使用 `SentenceBuffer.feed()` / `flush()`。

```python
async def clean(text):
    return text.strip() or None


# TTSClient(provider, text_processors=[clean])
```

文本处理器按顺序异步执行，返回字符串或 `None`（丢弃），处理后重新校验字符上限。
处理器异常会使当前任务失败，观察钩子则只记录状态。

## 最小 WebSocket 服务

```bash
uv run main.py serve --port 8766
# 另一个终端
uv run examples/ws_client.py
```

默认文本输入：`{"type":"text","text":"你好。"}`；最后发送 finish 控制消息。
输出为 `synthesis.started` JSON、PCM 二进制块、`synthesis.completed` JSON。
音频格式包含在开始事件中，完成事件包含统计信息。

官方协议参考：[TTS 客户端事件](https://help.aliyun.com/en/model-studio/qwen-audio-tts-client-events)、
[服务端事件](https://help.aliyun.com/en/model-studio/qwen-audio-tts-server-events)。

## 生命周期和资源限制

- 正常完成只产生一次完成事件；错误抛出 `SpeechError` 子类，保留 `code`，失败后不产生完成事件。
- `finish_input()` / 文本迭代结束是输入 EOF，仍需继续消费输出，直到完成。
- `asyncio.CancelledError` 保持取消语义；取消、失败、提前退出都会关闭当前厂商连接。
- ASR 使用 `async with client.open_stream(...)`；TTS 若可能提前 `break`，使用
  `contextlib.aclosing(client.synthesize(...))`。异步生成器的普通 `break` 不保证立即关闭。
- 客户端上下文退出或 `await client.aclose()` 会关闭它拥有的活动流；每条流只允许一个事件消费者。
- 每条任务独占厂商连接，没有自动重试、连接池或 Redis 依赖。结果已经输出后不会重放请求。
- Provider 拥有单次连接；核心客户端不接管传入 Provider 的全局资源。自定义 Provider 如维护全局资源，
  由应用自行关闭。

`StreamLimits` 可控制下列参数，时间单位为秒：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `buffer_size` | 16 | 输入/输出各自的最大待处理块数 |
| `max_chunk_bytes` | 65536 | 单个音频块最大字节数 |
| `max_text_chars` | 20000 | 单个文本块/识别结果的最大字符数 |
| `max_total_chars` | 200000 | 一次 TTS 请求的最大输入字符数 |
| `max_streams` | 8 | 每个客户端最大活动任务数，超出立即抛 `BusyError` |
| `start_timeout` | 10 | 建连及等待厂商 task-started |
| `input_timeout` | 20 | 有效输入之间的最长间隔，空文本不会续期 |
| `send_timeout` | 10 | 单次厂商写入或结果处理器的最长执行时间 |
| `idle_timeout` | 30 | 等待下一个厂商输出的最长时间 |
| `finish_timeout` | 15 | 输入发送完成后，等待输出结束的最长时间 |
| `total_timeout` | 300 | 启动后的任务总时限 |
| `close_timeout` | 2 | 关闭源迭代器、厂商连接的各自等待上限 |
| `observer_timeout` | 0.1 | 观察回调及观察队列收尾的时间上限 |

缓冲满时生产者等待，形成背压。总时限和结束时限包含慢消费者带来的等待，消费者必须持续读取。
单流内存由块数与块大小限制；厂商 WS 的控制消息上限为 1 MiB，底层读缓冲同样有界。
默认厂商适配器只暴露 mono / s16le / PCM，不进行转码、重采样或声道转换。

## 观察钩子

```python
async def observe(event):
    print(event.kind, event.stream_id, event.elapsed_seconds)


# Client(provider, observer=observe)
```

观察事件包含 `started/input/output/completed/failed/cancelled`、耗时和累计输入/输出单位。
回调必须是非阻塞异步函数；执行超时与普通异常会隔离并记录日志，不改变识别或合成结果。
每条流独立维护最多 32 个观察事件，满时丢弃新观察事件；它不是可靠消息通道。
需要修改处理结果时请使用显式处理器。

## 客户端 WebSocket 协议扩展

`integrations.serve_connection(socket, client, options=..., codec=...)` 服务一条任务，调用方拥有 socket。
连接对象只需实现异步 `recv()`、`send()`，无需 FastAPI。流结束后该函数返回，由服务器决定关闭连接。

`MessageCodec` 接口：

```python
def decode_message(message) -> InputCommand: ...
def encode_event(event) -> Iterable[str | bytes]: ...
def encode_error(error) -> Iterable[str | bytes]: ...
def encode_cancelled() -> Iterable[str | bytes]: ...
```

可以完全替换字段和消息类型，一个事件可以输出零条、一条或多条消息。参见
[`examples/custom_protocol.py`](examples/custom_protocol.py)。
`{"type":"finish"}` 表示输入 EOF；`{"type":"cancel"}` 显式取消，返回 `{"type":"cancelled"}`。
默认协议错误返回 `{"type":"error","code":"...","message":"..."}`。

示例服务器默认只监听回环地址，每条客户端连接对应一个任务；正式服务可在应用层添加鉴权和连接管理。
厂商连接的 WS 消息由 Provider 管理，与本节的客户端消息协议互相独立。

## 扩展 Provider

实现 `providers.base` 的结构化 Protocol：Provider 声明 `capabilities`，并实现
`async open_session(options, limits)`。返回的 Session 实现：

```python
async def send(value): ...
async def finish_input(): ...
async def receive(): ...
async def aclose(): ...
```

`receive()` 返回识别事件或音频块；只有整次任务正常结束时返回 `None`。
Provider 自行处理心跳和控制消息，识别片段确认不能返回 `None`。
启动协程被取消时，Provider 必须关闭它已建立但尚未返回的连接。
调用选项不支持的能力时应抛 `UnsupportedCapabilityError`，不得悄悄改变流式语义。

## 开发与构建

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv build
```

测试使用模拟 Provider、内存 WS 传输和本机临时 WebSocket 服务，无需云端凭证。
云端连通性、音色权限、实际首音延迟和并发容量需要使用自己的凭证运行示例确认。

源码直接放在根目录 `src/`，没有额外的包名子目录。`pyproject.toml` 将该目录映射为发行包的
Python 包名；源码 checkout 的 `main.py` 使用 `src` 导入，安装后的使用方使用正式包名。
两个发行包互不依赖，可以安装在同一个环境中。
