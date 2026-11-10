"""One audio event can become a JSON header followed by binary audio."""

import json

from streaming_tts import AudioChunk
from streaming_tts.integrations import DefaultCodec, TextInput


class AppCodec(DefaultCodec):
    def decode_message(self, message):
        if isinstance(message, str):
            data = json.loads(message)
            if data.get("action") == "speak":
                return TextInput(data["payload"]["content"])
        return super().decode_message(message)

    def encode_event(self, event):
        if isinstance(event, AudioChunk):
            return [json.dumps({"action": "audio", "sequence": event.sequence}), event.data]
        return super().encode_event(event)


# await serve_connection(socket, client, codec=AppCodec())
