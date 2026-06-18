from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger("molla.orchestrator.realtime_media")

SendJson = Callable[[dict[str, Any]], Awaitable[None] | None]
PeerConnectionFactory = Callable[[], Any]
SessionDescriptionFactory = Callable[..., Any]


@dataclass(slots=True)
class RealtimeCall:
    call_id: str
    session_id: str
    realtime_session_id: str
    peer_connection: Any
    media_tasks: set[asyncio.Task[None]]


class RealtimeMediaManager:
    def __init__(
        self,
        *,
        peer_connection_factory: PeerConnectionFactory | None = None,
        session_description_factory: SessionDescriptionFactory | None = None,
    ) -> None:
        self.peer_connection_factory = peer_connection_factory or self._create_peer_connection
        self.session_description_factory = session_description_factory or self._create_session_description
        self.calls: dict[str, RealtimeCall] = {}

    async def handle_command(self, payload: dict[str, Any], send_json: SendJson) -> None:
        message_type = str(payload.get("type", "")).strip()
        if message_type == "join_call":
            await self.join_call(payload, send_json=send_json)
            return
        if message_type == "webrtc_answer":
            await self.apply_answer(payload)
            return
        if message_type == "webrtc_renegotiate":
            await self.answer_renegotiation_offer(payload, send_json=send_json)
            return
        if message_type == "agent_control_error":
            logger.warning(
                "agent_control_error call_id=%s message=%s",
                payload.get("callId", ""),
                payload.get("message", ""),
            )
            return
        if message_type in {"end_call", "call_ended"}:
            await self.end_call(str(payload.get("callId", "")))
            return
        logger.info("realtime_media_ignored_command type=%s", message_type)

    async def join_call(self, payload: dict[str, Any], *, send_json: SendJson) -> None:
        call_id = str(payload.get("callId", "")).strip()
        session_id = str(payload.get("sessionId", "")).strip()
        realtime = payload.get("realtime") if isinstance(payload.get("realtime"), dict) else {}
        realtime_session_id = str(realtime.get("sessionId", "")).strip()
        tracks = realtime.get("tracks") if isinstance(realtime.get("tracks"), dict) else {}

        if not call_id:
            logger.warning("realtime_join_missing_call_id")
            return

        await self.end_call(call_id)

        peer_connection = self.peer_connection_factory()
        peer_connection.addTransceiver("audio", direction="sendrecv")
        self._register_track_handler(call_id, peer_connection)

        offer = await peer_connection.createOffer()
        await peer_connection.setLocalDescription(offer)

        self.calls[call_id] = RealtimeCall(
            call_id=call_id,
            session_id=session_id,
            realtime_session_id=realtime_session_id,
            peer_connection=peer_connection,
            media_tasks=set(),
        )
        await self._send_json(
            send_json,
            {
                "type": "agent_webrtc_offer",
                "callId": call_id,
                "sessionId": session_id,
                "realtimeSessionId": realtime_session_id,
                "tracks": tracks,
                "sessionDescription": {
                    "type": peer_connection.localDescription.type,
                    "sdp": peer_connection.localDescription.sdp,
                },
            },
        )
        logger.info("realtime_offer_sent call_id=%s realtime_session_id=%s", call_id, realtime_session_id)

    async def apply_answer(self, payload: dict[str, Any]) -> None:
        call_id = str(payload.get("callId", "")).strip()
        call = self.calls.get(call_id)
        if call is None:
            logger.warning("realtime_answer_unknown_call call_id=%s", call_id)
            return

        description = payload.get("sessionDescription")
        if not isinstance(description, dict):
            logger.warning("realtime_answer_missing_description call_id=%s", call_id)
            return

        remote = self.session_description_factory(
            sdp=str(description.get("sdp", "")),
            type=str(description.get("type", "")),
        )
        await call.peer_connection.setRemoteDescription(remote)
        logger.info("realtime_answer_applied call_id=%s", call_id)

    async def answer_renegotiation_offer(self, payload: dict[str, Any], *, send_json: SendJson) -> None:
        call_id = str(payload.get("callId", "")).strip()
        call = self.calls.get(call_id)
        if call is None:
            logger.warning("realtime_renegotiate_unknown_call call_id=%s", call_id)
            return

        description = payload.get("sessionDescription")
        if not isinstance(description, dict):
            logger.warning("realtime_renegotiate_missing_description call_id=%s", call_id)
            return

        realtime_session_id = str(payload.get("realtimeSessionId", call.realtime_session_id)).strip()
        remote = self.session_description_factory(
            sdp=str(description.get("sdp", "")),
            type=str(description.get("type", "")),
        )
        await call.peer_connection.setRemoteDescription(remote)
        answer = await call.peer_connection.createAnswer()
        await call.peer_connection.setLocalDescription(answer)
        await self._send_json(
            send_json,
            {
                "type": "agent_webrtc_renegotiation_answer",
                "callId": call_id,
                "sessionId": call.session_id,
                "realtimeSessionId": realtime_session_id,
                "sessionDescription": {
                    "type": call.peer_connection.localDescription.type,
                    "sdp": call.peer_connection.localDescription.sdp,
                },
            },
        )
        logger.info("realtime_renegotiation_answer_sent call_id=%s realtime_session_id=%s", call_id, realtime_session_id)

    async def end_call(self, call_id: str) -> None:
        if not call_id:
            return
        call = self.calls.pop(call_id, None)
        if call is None:
            return
        for task in list(call.media_tasks):
            task.cancel()
        for task in list(call.media_tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass
        await call.peer_connection.close()
        logger.info("realtime_call_closed call_id=%s", call_id)

    def _register_track_handler(self, call_id: str, peer_connection: Any) -> None:
        @peer_connection.on("track")
        def on_track(track: Any) -> None:
            if getattr(track, "kind", "") != "audio":
                return
            logger.info("realtime_audio_track_received call_id=%s kind=%s", call_id, getattr(track, "kind", ""))
            call = self.calls.get(call_id)
            if call is None:
                return
            task = asyncio.create_task(self._consume_audio_track(call_id, track), name=f"realtime-audio:{call_id}")
            call.media_tasks.add(task)
            task.add_done_callback(call.media_tasks.discard)

    async def _consume_audio_track(self, call_id: str, track: Any) -> None:
        frame_count = 0
        while True:
            frame = await track.recv()
            frame_count += 1
            if frame_count == 1 or frame_count % 100 == 0:
                logger.info(
                    "realtime_audio_frame_received call_id=%s frame_count=%s frame=%s",
                    call_id,
                    frame_count,
                    type(frame).__name__,
                )
            logger.debug("realtime_audio_frame call_id=%s frame=%s", call_id, type(frame).__name__)

    async def _send_json(self, send_json: SendJson, payload: dict[str, Any]) -> None:
        result = send_json(payload)
        if result is not None:
            await result

    def _create_peer_connection(self) -> Any:
        from aiortc import RTCPeerConnection

        return RTCPeerConnection()

    def _create_session_description(self, *, sdp: str, type: str) -> Any:
        from aiortc import RTCSessionDescription

        return RTCSessionDescription(sdp=sdp, type=type)
