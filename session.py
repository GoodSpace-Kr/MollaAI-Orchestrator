from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from fastapi import WebSocket
import httpx

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
from transcript_store import TranscriptStore


SENTENCE_END_RE = re.compile(r"(.+?[.!?]+(?:\s+|$))", re.DOTALL)
GREETING_TEXT = "Hi. this is MollaAI English. welcome to here !"
logger = logging.getLogger("molla.orchestrator")


@dataclass(slots=True)
class CallContext:
    stream_id: str = ""
    call_id: str = ""
    user_id: str = ""
    custom_parameters: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SentenceSynthesis:
    index: int
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
        transcript_store: TranscriptStore,
    ) -> None:
        self.websocket = websocket
        self.config = config
        self.stt_client = stt_client
        self.llm_client = llm_client
        self.tts_client = tts_client
        self.transcript_store = transcript_store
        self.context = CallContext()
        self.sequence_number = 1
        self.response_lock = asyncio.Lock()
        self.playback_lock = asyncio.Lock()
        self.closed = False
        self.sentence_buffer = ""
        self.session_id = "call-session"
        self.backend_session_id = ""
        self.turn_index = 0
        self._stt_task: asyncio.Task[None] | None = None
        self._response_tasks: set[asyncio.Task[None]] = set()

    async def open(self) -> None:
        await self.stt_client.connect()

    async def start(self, payload: dict[str, Any]) -> None:
        start = payload.get("start", {})
        self.context = CallContext(
            stream_id=str(start.get("streamId", "")),
            call_id=str(start.get("callId", "")),
            user_id=str(start.get("userId", start.get("accountId", ""))),
            custom_parameters=dict(start.get("customParameters", {}) or {}),
        )
        self.session_id = self.context.call_id or self.context.stream_id or "call-session"
        await self.transcript_store.start_session(
            self.session_id,
            metadata={
                "call_id": self.context.call_id,
                "stream_id": self.context.stream_id,
                "user_id": self.context.user_id,
                "custom_parameters": dict(self.context.custom_parameters),
            },
        )
        await self._send_session_started()
        greeting_task = self._track_task(
            asyncio.create_task(self._play_greeting(), name=f"greeting:{self.session_id}")
        )
        self._response_tasks.add(greeting_task)

        await self.stt_client.start(session_id=self.session_id, sample_rate=self.config.stt_sample_rate)
        self._stt_task = asyncio.create_task(self._consume_stt_events(), name=f"stt-events:{self.session_id}")

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
        await self.transcript_store.end_session(self.session_id)
        await self._send_completed_transcript()

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
            text = str(event.get("text", "")).strip()
            if event_type != "final":
                continue

            if not text:
                continue

            task = self._track_task(
                asyncio.create_task(self._run_response_pipeline(text), name=f"response:{self.context.call_id}")
            )
            self._response_tasks.add(task)

    async def _run_response_pipeline(self, transcript: str) -> None:
        async with self.response_lock:
            self.turn_index += 1
            turn_index = self.turn_index
            logger.info(
                "llm_request call_id=%s turn=%s transcript=%r",
                self.context.call_id,
                turn_index,
                transcript[:200],
            )
            await self.transcript_store.append_event(
                self.session_id,
                source="llm",
                kind="request",
                text=transcript,
                turn=turn_index,
            )
            self.sentence_buffer = ""
            token_started = False
            sentences: list[SentenceSynthesis] = []
            response_chunks: list[str] = []

            try:
                async for token in self.llm_client.stream_tokens(transcript):
                    token_started = True
                    response_chunks.append(token)
                    ready_sentences = self._take_ready_sentences(token)
                    if not ready_sentences:
                        continue

                    self._enqueue_sentence_tasks(ready_sentences, sentences)

                if not token_started:
                    return

                response_text = "".join(response_chunks).strip()
                if response_text:
                    await self.transcript_store.append_event(
                        self.session_id,
                        source="llm",
                        kind="response",
                        text=response_text,
                        turn=turn_index,
                    )

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

    async def _send_session_started(self) -> None:
        if not self.config.backend_session_start_url:
            return

        phone_number = self._normalize_phone_number(
            str(self.context.custom_parameters.get("from", ""))
        )
        call_sid = self.context.call_id.strip()
        if not phone_number or not call_sid:
            logger.info(
                "session_start_skipped session_id=%s phone_number=%r call_sid=%r",
                self.session_id,
                phone_number,
                call_sid,
            )
            return

        payload = {
            "phoneNumber": phone_number,
            "callSid": call_sid,
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(self.config.backend_session_start_url, json=payload)
                response.raise_for_status()
                data = response.json()
            backend_session_id = str(data.get("id", "")).strip() if isinstance(data, dict) else ""
            if not backend_session_id:
                logger.warning(
                    "session_started_missing_id session_id=%s url=%s payload=%s",
                    self.session_id,
                    self.config.backend_session_start_url,
                    data,
                )
                return
            self.backend_session_id = backend_session_id
            logger.info(
                "session_started_uploaded session_id=%s backend_session_id=%s url=%s phone_number=%s call_sid=%s",
                self.session_id,
                self.backend_session_id,
                self.config.backend_session_start_url,
                phone_number,
                call_sid,
            )
        except Exception:
            logger.exception(
                "session_start_upload_failed session_id=%s url=%s",
                self.session_id,
                self.config.backend_session_start_url,
            )

    async def _send_completed_transcript(self) -> None:
        if not self.backend_session_id:
            logger.info("transcript_upload_skipped session_id=%s reason=missing_backend_session_id", self.session_id)
            return

        if not self.config.backend_session_end_url_template:
            return

        session = await self.transcript_store.get_session(self.session_id)
        if session is None:
            return

        transcript_text = self.transcript_store.render_transcript_text(session)
        if not transcript_text:
            logger.info("transcript_upload_skipped session_id=%s reason=empty", self.session_id)
            return

        payload = {
            "status": "completed",
            "transcript": transcript_text,
        }
        end_url = self.config.backend_session_end_url_template.format(session_id=self.backend_session_id)

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(end_url, json=payload)
                response.raise_for_status()
            logger.info(
                "transcript_uploaded session_id=%s backend_session_id=%s url=%s chars=%s",
                self.session_id,
                self.backend_session_id,
                end_url,
                len(transcript_text),
            )
        except Exception:
            logger.exception(
                "transcript_upload_failed session_id=%s backend_session_id=%s url=%s",
                self.session_id,
                self.backend_session_id,
                end_url,
            )

    def _normalize_phone_number(self, value: str) -> str:
        return re.sub(r"[-\s]", "", value).strip()

    def _track_task(self, task: asyncio.Task[None]) -> asyncio.Task[None]:
        def _on_done(done_task: asyncio.Task[None]) -> None:
            self._response_tasks.discard(done_task)
            try:
                done_task.result()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception(
                    "background_task_failed call_id=%s task=%s",
                    self.context.call_id,
                    done_task.get_name(),
                )

        task.add_done_callback(_on_done)
        return task

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
                self._produce_sentence_audio(index, cleaned, frame_queue),
                name=f"tts:{self.context.call_id}:{index}",
            )
            task_bucket.append(
                SentenceSynthesis(
                    index=index,
                    text=cleaned,
                    frame_queue=frame_queue,
                    task=task,
                )
            )

    async def _play_sentence_batch(self, sentences: list[SentenceSynthesis]) -> None:
        async with self.playback_lock:
            for sentence in sentences:
                playback_started_at: float | None = None
                frames_sent = 0
                while True:
                    frame = await sentence.frame_queue.get()
                    if frame is None:
                        break
                    if playback_started_at is None:
                        playback_started_at = time.perf_counter()
                        logger.info(
                            "tts_playback_start call_id=%s sentence=%s text=%r",
                            self.context.call_id,
                            sentence.index,
                            sentence.text[:80],
                        )
                    await self._send_audio_frame(frame)
                    frames_sent += 1
                await sentence.task
                if playback_started_at is not None:
                    logger.info(
                        "tts_playback_done call_id=%s sentence=%s frames=%s playback_ms=%s text=%r",
                        self.context.call_id,
                        sentence.index,
                        frames_sent,
                        int((time.perf_counter() - playback_started_at) * 1000),
                        sentence.text[:80],
                    )

    async def _produce_sentence_audio(self, index: int, text: str, frame_queue: asyncio.Queue[bytes | None]) -> None:
        if not text:
            await frame_queue.put(None)
            return
        request_started_at = time.perf_counter()
        logger.info(
            "tts_request call_id=%s sentence=%s text=%r",
            self.context.call_id,
            index,
            text[:200],
        )

        try:
            first_frame_logged = False
            async for frame in self._stream_tts_frames(index, text, request_started_at):
                if not first_frame_logged:
                    first_frame_logged = True
                    logger.info(
                        "tts_frame_ready call_id=%s sentence=%s ttfb_to_frame_ms=%s text=%r",
                        self.context.call_id,
                        index,
                        int((time.perf_counter() - request_started_at) * 1000),
                        text[:80],
                    )
                await frame_queue.put(frame)
        except httpx.ReadError:
            if self.closed:
                logger.info(
                    "tts_stream_closed call_id=%s sentence=%s text=%r",
                    self.context.call_id,
                    index,
                    text[:80],
                )
                return
            logger.warning(
                "tts_stream_read_error call_id=%s sentence=%s elapsed_ms=%s text=%r",
                self.context.call_id,
                index,
                int((time.perf_counter() - request_started_at) * 1000),
                text[:80],
            )
        finally:
            await frame_queue.put(None)

    async def _stream_tts_frames(self, index: int, text: str, request_started_at: float) -> AsyncIterator[bytes]:
        wav_buffer = WavStreamBuffer()
        frame_size = self.config.outbound_frame_bytes
        pending_mulaw = bytearray()
        first_chunk_logged = False

        async for wav_chunk in self.tts_client.stream_wav(
            text=text,
            voice=self.config.tts_voice,
            lang_code=self.config.tts_lang_code,
            sample_rate=self.config.tts_sample_rate,
        ):
            if not first_chunk_logged:
                first_chunk_logged = True
                logger.info(
                    "tts_first_chunk call_id=%s sentence=%s ttfb_ms=%s chunk_bytes=%s text=%r",
                    self.context.call_id,
                    index,
                    int((time.perf_counter() - request_started_at) * 1000),
                    len(wav_chunk),
                    text[:80],
                )
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
