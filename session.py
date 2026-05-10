from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from fastapi import WebSocket

from audio import (
    WavStreamBuffer,
    bytes_to_pcm16,
    decode_base64_payload,
    encode_base64_payload,
    mulaw_to_pcm16,
    pcm16_to_bytes,
    pcm16_to_mulaw,
    resample_pcm16,
)
from clients import LlmHttpClient, SttWsClient, TtsHttpClient
from config import OrchestratorConfig


SENTENCE_END_RE = re.compile(r"(.+?[.!?]+(?:\s+|$))", re.DOTALL)
GREETING_TEXT = "Hi. this is MollaAI English. welcome to here !"
logger = logging.getLogger("molla.orchestrator")


@dataclass(slots=True)
class CallContext:
    stream_id: str = ""
    call_id: str = ""
    account_id: str = ""
    custom_parameters: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SentenceSynthesis:
    text: str
    frame_queue: asyncio.Queue[bytes | None]
    task: asyncio.Task[None]


class CallSession:
    def __init__(
        self,
        *,
        websocket: WebSocket,
        config: OrchestratorConfig,
        stt_client: SttWsClient,
        llm_client: LlmHttpClient,
        tts_client: TtsHttpClient,
    ) -> None:
        self.websocket = websocket
        self.config = config
        self.stt_client = stt_client
        self.llm_client = llm_client
        self.tts_client = tts_client
        self.context = CallContext()
        self.sequence_number = 1
        self.response_lock = asyncio.Lock()
        self.playback_lock = asyncio.Lock()
        self.closed = False
        self.sentence_buffer = ""
        self._stt_task: asyncio.Task[None] | None = None
        self._response_tasks: set[asyncio.Task[None]] = set()

    async def open(self) -> None:
        await self.stt_client.connect()

    async def start(self, payload: dict[str, Any]) -> None:
        start = payload.get("start", {})
        self.context = CallContext(
            stream_id=str(start.get("streamId", "")),
            call_id=str(start.get("callId", "")),
            account_id=str(start.get("accountId", "")),
            custom_parameters=dict(start.get("customParameters", {}) or {}),
        )
        session_id = self.context.call_id or self.context.stream_id or "call-session"
        await self.stt_client.start(session_id=session_id, sample_rate=self.config.stt_sample_rate)
        self._stt_task = asyncio.create_task(self._consume_stt_events(), name=f"stt-events:{session_id}")
        greeting_task = asyncio.create_task(self._play_greeting(), name=f"greeting:{session_id}")
        self._response_tasks.add(greeting_task)
        greeting_task.add_done_callback(self._response_tasks.discard)

    async def handle_media(self, payload: dict[str, Any]) -> None:
        media = payload.get("media", {})
        encoded = media.get("payload")
        if not isinstance(encoded, str):
            return

        mulaw_audio = decode_base64_payload(encoded)
        pcm8k = mulaw_to_pcm16(mulaw_audio)
        pcm_stt = resample_pcm16(pcm8k, 8000, self.config.stt_sample_rate)
        await self.stt_client.send_audio(pcm16_to_bytes(pcm_stt))

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True

        if self._stt_task is not None:
            self._stt_task.cancel()
            try:
                await self._stt_task
            except asyncio.CancelledError:
                pass

        for task in list(self._response_tasks):
            task.cancel()
        for task in list(self._response_tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass

        await self.stt_client.close()
        await self.llm_client.close()
        await self.tts_client.close()

    async def _consume_stt_events(self) -> None:
        async for event in self.stt_client.receive_events():
            event_type = event.get("type")
            logger.info(
                "stt_event type=%s session_id=%s revision=%s text=%r",
                event_type,
                event.get("session_id", ""),
                event.get("revision", ""),
                str(event.get("text", ""))[:200],
            )
            if event_type != "final":
                continue

            text = str(event.get("text", "")).strip()
            if not text:
                continue

            task = asyncio.create_task(self._run_response_pipeline(text), name=f"response:{self.context.call_id}")
            self._response_tasks.add(task)
            task.add_done_callback(self._response_tasks.discard)

    async def _run_response_pipeline(self, transcript: str) -> None:
        async with self.response_lock:
            logger.info(
                "llm_request call_id=%s transcript=%r",
                self.context.call_id,
                transcript[:200],
            )
            self.sentence_buffer = ""
            token_started = False
            sentences: list[SentenceSynthesis] = []

            try:
                async for token in self.llm_client.stream_tokens(transcript):
                    token_started = True
                    ready_sentences = self._take_ready_sentences(token)
                    if not ready_sentences:
                        continue

                    self._enqueue_sentence_tasks(ready_sentences, sentences)

                if not token_started:
                    return

                tail = self._flush_sentence_buffer()
                if tail:
                    self._enqueue_sentence_tasks([tail], sentences)

                if sentences:
                    await self._play_sentence_batch(sentences)
            except asyncio.CancelledError:
                for sentence in sentences:
                    sentence.task.cancel()
                for sentence in sentences:
                    try:
                        await sentence.task
                    except asyncio.CancelledError:
                        pass
                raise

    async def _play_greeting(self) -> None:
        async with self.response_lock:
            sentences: list[SentenceSynthesis] = []
            self._enqueue_sentence_tasks([GREETING_TEXT], sentences)
            if sentences:
                await self._play_sentence_batch(sentences)

    def _take_ready_sentences(self, token: str) -> list[str]:
        self.sentence_buffer += token
        ready: list[str] = []

        while True:
            match = SENTENCE_END_RE.match(self.sentence_buffer)
            if not match:
                break
            sentence = match.group(1).strip()
            if sentence:
                ready.append(sentence)
            self.sentence_buffer = self.sentence_buffer[match.end() :]

        buffered = self.sentence_buffer.strip()
        if len(buffered) >= self.config.llm_sentence_hard_limit:
            ready.append(buffered)
            self.sentence_buffer = ""
        elif len(buffered) >= self.config.llm_sentence_soft_limit and token.endswith(" "):
            ready.append(buffered)
            self.sentence_buffer = ""

        return ready

    def _flush_sentence_buffer(self) -> str:
        remaining = self.sentence_buffer.strip()
        self.sentence_buffer = ""
        return remaining

    def _enqueue_sentence_tasks(self, sentences: list[str], task_bucket: list[SentenceSynthesis]) -> None:
        for index, sentence in enumerate(sentences, start=len(task_bucket) + 1):
            cleaned = sentence.strip()
            if not cleaned:
                continue
            frame_queue: asyncio.Queue[bytes | None] = asyncio.Queue()
            task = asyncio.create_task(
                self._produce_sentence_audio(cleaned, frame_queue),
                name=f"tts:{self.context.call_id}:{index}",
            )
            task_bucket.append(
                SentenceSynthesis(
                    text=cleaned,
                    frame_queue=frame_queue,
                    task=task,
                )
            )

    async def _play_sentence_batch(self, sentences: list[SentenceSynthesis]) -> None:
        async with self.playback_lock:
            for sentence in sentences:
                while True:
                    frame = await sentence.frame_queue.get()
                    if frame is None:
                        break
                    await self._send_audio_frame(frame)
                await sentence.task

    async def _produce_sentence_audio(self, text: str, frame_queue: asyncio.Queue[bytes | None]) -> None:
        if not text:
            await frame_queue.put(None)
            return
        logger.info(
            "tts_request call_id=%s text=%r",
            self.context.call_id,
            text[:200],
        )

        try:
            async for frame in self._stream_tts_frames(text):
                await frame_queue.put(frame)
        finally:
            await frame_queue.put(None)

    async def _stream_tts_frames(self, text: str) -> AsyncIterator[bytes]:
        wav_buffer = WavStreamBuffer()
        frame_size = self.config.outbound_frame_bytes
        pending_mulaw = bytearray()

        async for wav_chunk in self.tts_client.stream_wav(
            text=text,
            voice=self.config.tts_voice,
            lang_code=self.config.tts_lang_code,
            sample_rate=self.config.tts_sample_rate,
        ):
            pcm_bytes = wav_buffer.push(wav_chunk)
            if not pcm_bytes:
                continue

            pcm24k = bytes_to_pcm16(pcm_bytes)
            pcm8k = resample_pcm16(pcm24k, self.config.tts_sample_rate, 8000)
            pending_mulaw.extend(pcm16_to_mulaw(pcm8k))

            while len(pending_mulaw) >= frame_size:
                yield bytes(pending_mulaw[:frame_size])
                del pending_mulaw[:frame_size]

        if pending_mulaw:
            yield bytes(pending_mulaw)

    async def _send_audio_frame(self, frame: bytes) -> None:
        if not frame:
            return
        await self._send_clawops_media(frame)
        await asyncio.sleep(self.config.outbound_frame_ms)

    async def _send_clawops_media(self, frame: bytes) -> None:
        payload = {
            "event": "media",
            "streamId": self.context.stream_id,
            "sequenceNumber": str(self._next_sequence_number()),
            "media": {
                "payload": encode_base64_payload(frame),
            },
        }
        await self.websocket.send_json(payload)

    def _next_sequence_number(self) -> int:
        current = self.sequence_number
        self.sequence_number += 1
        return current
