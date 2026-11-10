from collections.abc import Awaitable, Callable

TextProcessor = Callable[[str], Awaitable[str | None]]
