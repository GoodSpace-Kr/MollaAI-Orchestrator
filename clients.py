from __future__ import annotations

import json
from typing import AsyncIterator

import httpx
import websockets

class SttWsClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self._ws: websockets.ClientConnection | None = None

    async def connect(self) -> None:
        self._ws = await websockets.connect(self.url, max_size=None)
        await self._ws.recv()

    async def start(self, session_id: str, sample_rate: int) -> None:
        ws = self._require_ws()
        await ws.send(
            json.dumps(
                {
                    "type": "start",
                    "session_id": session_id,
                    "encoding": "pcm16",
                    "config": {
                        "sample_rate": sample_rate,
                        "channels": 1,
                    },
                }
            )
        )
        await ws.recv()

    async def send_audio(self, payload: bytes) -> None:
        await self._require_ws().send(payload)

    async def receive_events(self) -> AsyncIterator[dict]:
        ws = self._require_ws()
        async for raw_message in ws:
            if isinstance(raw_message, bytes):
                continue
            yield json.loads(raw_message)

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None

    def _require_ws(self) -> websockets.ClientConnection:
        if self._ws is None:
            raise RuntimeError("STT websocket is not connected.")
        return self._ws


class LlmHttpClient:
    def __init__(self, base_url: str) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=None)

    async def stream_tokens(self, query: str) -> AsyncIterator[str]:
        async with self._client.stream("POST", "/chat/tokens", json={"query": query}) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                stripped = line.strip()
                if not stripped or not stripped.startswith("data:"):
                    continue
                payload = stripped[5:].strip()
                if payload == "[DONE]":
                    break
                data = json.loads(payload)
                token = data.get("token")
                if isinstance(token, str) and token:
                    yield token

    async def close(self) -> None:
        await self._client.aclose()


class TtsHttpClient:
    def __init__(self, base_url: str) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=None)

    async def stream_wav(self, *, text: str, voice: str, lang_code: str, sample_rate: int) -> AsyncIterator[bytes]:
        payload = {
            "text": text,
            "voice": voice,
            "lang_code": lang_code,
            "sample_rate": sample_rate,
        }
        async with self._client.stream("POST", "/v1/tts/stream", json=payload) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                if chunk:
                    yield chunk

    async def close(self) -> None:
        await self._client.aclose()
