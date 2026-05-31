from __future__ import annotations

import asyncio
from collections import deque
from difflib import SequenceMatcher
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
    pcm16_bytes_to_wav,
    pcm16_to_bytes,
    pcm16_to_mulaw,
    resample_pcm16,
)
from clients import LlmHttpClient, SttWsClient, TtsHttpClient
from config import OrchestratorConfig
from storage import S3AudioStorage
from transcript_store import ConversationTurn, TranscriptStore


SENTENCE_END_RE = re.compile(r"(.+?[.!?]+(?:\s+|$))", re.DOTALL)
GREETING_TEXT = "Hi. this is MollaAI English. welcome to here !"
TIMEOUT_NOTICE_TEXT = "오늘 잔여시간이 모두 소진되었습니다. 통화가 자동으로 종료됩니다. 감사합니다."
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
    HISTORY_TURN_LIMIT = 5

    def __init__(
        self,
        *,
        websocket: WebSocket,
        config: OrchestratorConfig,
        stt_client: SttWsClient,
        llm_client: LlmHttpClient,
        tts_client: TtsHttpClient,
        transcript_store: TranscriptStore,
        audio_storage: S3AudioStorage | None,
    ) -> None:
        self.websocket = websocket
        self.config = config
        self.stt_client = stt_client
        self.llm_client = llm_client
        self.tts_client = tts_client
        self.transcript_store = transcript_store
        self.audio_storage = audio_storage
        self.context = CallContext()
        self.sequence_number = 1
        self.response_lock = asyncio.Lock()
        self.playback_lock = asyncio.Lock()
        self.closed = False
        self.sentence_buffer = ""
        self.session_id = "call-session"
        self.backend_session_id = ""
        self.turn_index = 0
        self._remaining_minutes: int | None = None
        self._timeout_task: asyncio.Task[None] | None = None
        self._accepting_user_audio = True
        self._timeout_notice_sent = False
        self._stt_task: asyncio.Task[None] | None = None
        self._response_tasks: set[asyncio.Task[None]] = set()
        self._audio_upload_tasks: set[asyncio.Task[None]] = set()
        self._pending_user_audio = bytearray()
        self._last_completed_response_turn_index = 0
        self._assistant_echo_texts: deque[tuple[float, str]] = deque(maxlen=24)

    async def open(self) -> None:
        await self.stt_client.connect()

    async def start(self, payload: dict[str, Any]) -> None:
        start = payload.get("start", {})
        self._remaining_minutes = self._coerce_remaining_minutes(start.get("remainingMinutes"))
        self._accepting_user_audio = True
        self._timeout_notice_sent = False
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
                "remaining_minutes": self._remaining_minutes,
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
        self._schedule_timeout_task()

    async def handle_media(self, payload: dict[str, Any]) -> None:
        if self.closed or not self._accepting_user_audio:
            return
        media = payload.get("media", {})
        encoded = media.get("payload")
        if not isinstance(encoded, str):
            return

        mulaw_audio = decode_base64_payload(encoded)
        pcm8k = mulaw_to_pcm16(mulaw_audio)
        pcm_stt = resample_pcm16(pcm8k, 8000, self.config.stt_sample_rate)
        pcm_stt_bytes = pcm16_to_bytes(pcm_stt)
        if pcm_stt_bytes:
            self._pending_user_audio.extend(pcm_stt_bytes)
        await self.stt_client.send_audio(pcm_stt_bytes)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._accepting_user_audio = False
        self._cancel_timeout_task()

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

        for task in list(self._audio_upload_tasks):
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

            if not self._accepting_user_audio or self.closed:
                continue
            if not text:
                continue
            if self._is_likely_assistant_echo(text):
                logger.info(
                    "stt_echo_ignored call_id=%s text=%r",
                    self.context.call_id,
                    text[:200],
                )
                continue

            turn = await self._store_conversation_turn(text)
            if self._has_active_response():
                await self._interrupt_active_responses(turn)

            task = self._track_task(
                asyncio.create_task(self._run_response_pipeline(turn), name=f"response:{self.context.call_id}")
            )
            self._response_tasks.add(task)

    async def _run_response_pipeline(self, turn: ConversationTurn) -> None:
        async with self.response_lock:
            self.turn_index += 1
            llm_turn_index = self.turn_index
            llm_started_at = time.perf_counter()
            self.sentence_buffer = ""
            token_started = False
            first_sentence_logged = False
            token_count = 0
            sentences: list[SentenceSynthesis] = []
            response_chunks: list[str] = []

            try:
                user_query, history_before_turn_index = await self._pending_user_query(turn)
                history = await self._recent_llm_history(history_before_turn_index)
                logger.info(
                    "llm_request call_id=%s turn=%s transcript=%r history_before_turn=%s",
                    self.context.call_id,
                    llm_turn_index,
                    user_query[:300],
                    history_before_turn_index,
                )
                async for token in self.llm_client.stream_tokens(
                    user_query,
                    user_id=self.context.user_id or self.context.call_id or self.session_id,
                    conversation_id=self.session_id,
                    history=history,
                ):
                    token_count += 1
                    if not token_started:
                        logger.info(
                            "llm_first_token call_id=%s turn=%s latency_ms=%s token=%r",
                            self.context.call_id,
                            llm_turn_index,
                            int((time.perf_counter() - llm_started_at) * 1000),
                            token[:80],
                        )
                    token_started = True
                    response_chunks.append(token)
                    ready_sentences = self._take_ready_sentences(token)
                    if not ready_sentences:
                        continue

                    if not first_sentence_logged:
                        first_sentence_logged = True
                        logger.info(
                            "llm_first_sentence call_id=%s turn=%s latency_ms=%s sentence=%r",
                            self.context.call_id,
                            llm_turn_index,
                            int((time.perf_counter() - llm_started_at) * 1000),
                            ready_sentences[0][:120],
                        )
                    self._enqueue_sentence_tasks(ready_sentences, sentences)

                if not token_started:
                    logger.warning(
                        "llm_empty_response call_id=%s turn=%s elapsed_ms=%s",
                        self.context.call_id,
                        llm_turn_index,
                        int((time.perf_counter() - llm_started_at) * 1000),
                    )
                    return

                response_text = "".join(response_chunks).strip()
                if response_text:
                    await self.transcript_store.set_assistant_response(
                        self.session_id,
                        turn_index=turn.index,
                        text=response_text,
                        llm_turn=llm_turn_index,
                    )

                tail = self._flush_sentence_buffer()
                if tail:
                    if not first_sentence_logged:
                        first_sentence_logged = True
                        logger.info(
                            "llm_first_sentence call_id=%s turn=%s latency_ms=%s sentence=%r",
                            self.context.call_id,
                            llm_turn_index,
                            int((time.perf_counter() - llm_started_at) * 1000),
                            tail[:120],
                        )
                    self._enqueue_sentence_tasks([tail], sentences)

                logger.info(
                    "llm_stream_done call_id=%s turn=%s elapsed_ms=%s tokens=%s chars=%s sentences=%s",
                    self.context.call_id,
                    llm_turn_index,
                    int((time.perf_counter() - llm_started_at) * 1000),
                    token_count,
                    len(response_text),
                    len(sentences),
                )

                if sentences:
                    await self._play_sentence_batch(sentences)
                self._last_completed_response_turn_index = max(
                    self._last_completed_response_turn_index,
                    turn.index,
                )
            except asyncio.CancelledError:
                for sentence in sentences:
                    sentence.task.cancel()
                for sentence in sentences:
                    try:
                        await sentence.task
                    except asyncio.CancelledError:
                        pass
                raise
            except Exception:
                logger.exception(
                    "llm_pipeline_failed call_id=%s turn=%s elapsed_ms=%s tokens=%s",
                    self.context.call_id,
                    llm_turn_index,
                    int((time.perf_counter() - llm_started_at) * 1000),
                    token_count,
                )
                raise

    async def _play_greeting(self) -> None:
        async with self.response_lock:
            await self._play_system_message(GREETING_TEXT)

    async def _play_system_message(self, text: str) -> None:
        sentences: list[SentenceSynthesis] = []
        self._enqueue_sentence_tasks([text], sentences)
        if sentences:
            await self._play_sentence_batch(sentences)

    def _has_active_response(self) -> bool:
        return any(not task.done() for task in self._response_tasks)

    async def _interrupt_active_responses(self, turn: ConversationTurn) -> None:
        tasks = [task for task in list(self._response_tasks) if not task.done()]
        if not tasks:
            return
        logger.info(
            "barge_in_detected call_id=%s turn=%s transcript=%r active_tasks=%s",
            self.context.call_id,
            turn.index,
            turn.user_text[:200],
            len(tasks),
        )
        for task in tasks:
            task.cancel()
        done, pending = await asyncio.wait(tasks, timeout=1.0)
        for task in done:
            try:
                task.result()
            except asyncio.CancelledError:
                pass
        if pending:
            logger.warning(
                "barge_in_cancel_timeout call_id=%s pending_tasks=%s",
                self.context.call_id,
                len(pending),
            )

    async def _pending_user_query(self, current_turn: ConversationTurn) -> tuple[str, int]:
        session = await self.transcript_store.get_session(self.session_id)
        if session is None:
            return current_turn.user_text, current_turn.index

        pending_turns: list[dict[str, Any]] = []
        for raw_turn in session.get("turns", []):
            try:
                turn_index = int(raw_turn.get("index", 0))
            except (TypeError, ValueError):
                continue
            if turn_index <= self._last_completed_response_turn_index:
                continue
            if turn_index > current_turn.index:
                continue
            user_text = str(raw_turn.get("user_text", "")).strip()
            if not user_text:
                continue
            pending_turns.append({"index": turn_index, "user_text": user_text})

        if not pending_turns:
            return current_turn.user_text, current_turn.index

        combined_query = "\n".join(str(turn["user_text"]) for turn in pending_turns).strip()
        first_pending_index = int(pending_turns[0]["index"])
        if len(pending_turns) > 1:
            logger.info(
                "barge_in_query_combined call_id=%s from_turn=%s to_turn=%s parts=%s text=%r",
                self.context.call_id,
                first_pending_index,
                current_turn.index,
                len(pending_turns),
                combined_query[:300],
            )
        return combined_query or current_turn.user_text, first_pending_index

    def _coerce_remaining_minutes(self, value: Any) -> int | None:
        if value is None:
            return None
        try:
            minutes = int(value)
        except (TypeError, ValueError):
            return None
        return max(0, minutes)

    def _schedule_timeout_task(self) -> None:
        self._cancel_timeout_task()
        if self._remaining_minutes is None:
            return
        timeout_seconds = self._remaining_minutes * 60
        self._timeout_task = asyncio.create_task(
            self._run_timeout_after_delay(timeout_seconds),
            name=f"timeout:{self.session_id}",
        )

    def _cancel_timeout_task(self) -> None:
        if self._timeout_task is None:
            return
        self._timeout_task.cancel()
        self._timeout_task = None

    async def _run_timeout_after_delay(self, timeout_seconds: int) -> None:
        try:
            await asyncio.sleep(max(0, timeout_seconds))
            await self._handle_remaining_minutes_expired()
        except asyncio.CancelledError:
            raise

    async def _handle_remaining_minutes_expired(self) -> None:
        if self.closed or self._timeout_notice_sent:
            return
        self._accepting_user_audio = False
        self._timeout_notice_sent = True
        logger.info(
            "session_remaining_time_exhausted session_id=%s remaining_minutes=%s",
            self.session_id,
            self._remaining_minutes,
        )
        async with self.response_lock:
            if self.closed:
                return
            await self._play_system_message(TIMEOUT_NOTICE_TEXT)
        if self.closed:
            return
        await self.websocket.close()

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
            backend_session_id = ""
            if isinstance(data, dict):
                nested = data.get("data", {})
                backend_session_id = str(
                    data.get("id", nested.get("id", "") if isinstance(nested, dict) else "")
                ).strip()
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

        payload = self.transcript_store.build_completed_payload(session)
        if not payload["turns"]:
            logger.info("transcript_upload_skipped session_id=%s reason=empty", self.session_id)
            return

        request_payload = {
            "status": "completed",
            **payload,
        }
        end_url = self.config.backend_session_end_url_template.format(session_id=self.backend_session_id)

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.patch(end_url, json=request_payload)
                response.raise_for_status()
            logger.info(
                "transcript_uploaded session_id=%s backend_session_id=%s url=%s turns=%s",
                self.session_id,
                self.backend_session_id,
                end_url,
                len(payload["turns"]),
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

    async def _store_conversation_turn(self, text: str) -> ConversationTurn:
        cleaned = text.strip()
        if not cleaned:
            self._pending_user_audio.clear()
            raise ValueError("Conversation turn text must not be empty.")

        audio_bytes = bytes(self._pending_user_audio)
        self._pending_user_audio.clear()
        if not audio_bytes:
            logger.warning("conversation_turn_empty_audio session_id=%s text=%r", self.session_id, cleaned[:120])

        turn = await self.transcript_store.append_turn(
            self.session_id,
            user_text=cleaned,
            sample_rate=self.config.stt_sample_rate,
        )
        if audio_bytes and self.audio_storage is not None:
            task = self._track_audio_upload_task(
                asyncio.create_task(
                    self._upload_turn_audio(turn.index, audio_bytes),
                    name=f"audio-upload:{self.context.call_id}:{turn.index}",
                )
            )
            self._audio_upload_tasks.add(task)
        return turn

    def _track_audio_upload_task(self, task: asyncio.Task[None]) -> asyncio.Task[None]:
        def _on_done(done_task: asyncio.Task[None]) -> None:
            self._audio_upload_tasks.discard(done_task)
            try:
                done_task.result()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception(
                    "audio_upload_task_failed call_id=%s task=%s",
                    self.context.call_id,
                    done_task.get_name(),
                )

        task.add_done_callback(_on_done)
        return task

    async def _upload_turn_audio(self, turn_index: int, audio_bytes: bytes) -> None:
        if self.audio_storage is None or not audio_bytes:
            return

        call_id = self.context.call_id.strip() or self.session_id
        wav_bytes = pcm16_bytes_to_wav(audio_bytes, self.config.stt_sample_rate)
        key = await self.audio_storage.upload_turn_audio(
            call_id=call_id,
            turn_index=turn_index,
            wav_bytes=wav_bytes,
        )
        await self.transcript_store.set_turn_audio_key(
            self.session_id,
            turn_index=turn_index,
            audio_key=key,
        )
        logger.info(
            "turn_audio_uploaded call_id=%s turn=%s audio_key=%s bytes=%s",
            self.context.call_id,
            turn_index,
            key,
            len(wav_bytes),
        )

    async def _recent_llm_history(self, current_turn_index: int) -> list[dict[str, str]]:
        session = await self.transcript_store.get_session(self.session_id)
        if session is None:
            return []

        raw_turns = session.get("turns", [])
        prior_turns: list[dict[str, Any]] = []
        for raw_turn in raw_turns:
            try:
                turn_index = int(raw_turn.get("index", 0))
            except (TypeError, ValueError):
                continue
            if turn_index >= current_turn_index:
                continue
            prior_turns.append(raw_turn)

        history: list[dict[str, str]] = []
        for raw_turn in prior_turns[-self.HISTORY_TURN_LIMIT :]:
            user_text = str(raw_turn.get("user_text", "")).strip()
            if user_text:
                history.append({"role": "user", "content": user_text})

            assistant_text = str(raw_turn.get("assistant_text", "")).strip()
            if assistant_text:
                history.append({"role": "assistant", "content": assistant_text})

        return history

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
                        self._remember_assistant_echo_text(sentence.text)
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

    def _remember_assistant_echo_text(self, text: str) -> None:
        normalized = self._normalize_echo_text(text)
        if normalized:
            self._assistant_echo_texts.append((time.monotonic(), normalized))

    def _is_likely_assistant_echo(self, text: str) -> bool:
        normalized = self._normalize_echo_text(text)
        if not normalized:
            return False

        now = time.monotonic()
        while self._assistant_echo_texts and now - self._assistant_echo_texts[0][0] > 12.0:
            self._assistant_echo_texts.popleft()

        for _, assistant_text in self._assistant_echo_texts:
            if not assistant_text:
                continue
            if normalized in assistant_text or assistant_text in normalized:
                return True
            if SequenceMatcher(None, normalized, assistant_text).ratio() >= 0.72:
                return True
        return False

    def _normalize_echo_text(self, text: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^0-9A-Za-z가-힣\s]+", " ", text.lower())).strip()

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
