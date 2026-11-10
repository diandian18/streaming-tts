"""Stable errors; provider errors retain the upstream code and explanation."""


class SpeechError(RuntimeError):
    def __init__(self, message: str, *, code: str = "speech_error"):
        super().__init__(message)
        self.code = code


class ProviderError(SpeechError):
    pass


class ProtocolError(SpeechError):
    def __init__(self, message: str):
        super().__init__(message, code="protocol_error")


class StreamClosedError(SpeechError):
    def __init__(self, message: str = "Stream is closed"):
        super().__init__(message, code="stream_closed")


class SpeechTimeoutError(SpeechError):
    def __init__(self, message: str):
        super().__init__(message, code="timeout")


class UnsupportedCapabilityError(SpeechError):
    def __init__(self, message: str):
        super().__init__(message, code="unsupported_capability")


class BusyError(SpeechError):
    def __init__(self):
        super().__init__("Maximum concurrent streams reached", code="busy")
