from __future__ import annotations

import unittest
from unittest.mock import AsyncMock

from realtime_media import RealtimeMediaManager


class FakeDescription:
    def __init__(self, *, sdp: str, type: str) -> None:
        self.sdp = sdp
        self.type = type


class FakePeerConnection:
    def __init__(self) -> None:
        self.localDescription = FakeDescription(sdp="local-sdp", type="offer")
        self.remote_descriptions: list[FakeDescription] = []
        self.transceivers: list[tuple[str, str]] = []
        self.handlers: dict[str, object] = {}
        self.closed = False

    def addTransceiver(self, kind: str, direction: str) -> None:
        self.transceivers.append((kind, direction))

    def on(self, event: str):
        def decorator(handler):
            self.handlers[event] = handler
            return handler

        return decorator

    async def createOffer(self) -> FakeDescription:
        return FakeDescription(sdp="local-sdp", type="offer")

    async def createAnswer(self) -> FakeDescription:
        return FakeDescription(sdp="local-answer-sdp", type="answer")

    async def setLocalDescription(self, description: FakeDescription) -> None:
        self.localDescription = description

    async def setRemoteDescription(self, description: FakeDescription) -> None:
        self.remote_descriptions.append(description)

    async def close(self) -> None:
        self.closed = True


class RealtimeMediaManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_join_call_sends_webrtc_offer_to_backend(self) -> None:
        sent: list[dict] = []
        peer = FakePeerConnection()
        manager = RealtimeMediaManager(
            peer_connection_factory=lambda: peer,
            session_description_factory=lambda *, sdp, type: FakeDescription(sdp=sdp, type=type),
        )

        await manager.handle_command(
            {
                "type": "join_call",
                "callId": "call-1",
                "sessionId": "backend-session-1",
                "userId": "user-1",
                "realtime": {
                    "sessionId": "cf-session-1",
                    "tracks": {
                        "subscribe": "user_audio",
                        "publish": "assistant_audio",
                    },
                },
            },
            send_json=sent.append,
        )

        self.assertEqual(peer.transceivers, [("audio", "sendrecv")])
        self.assertEqual(
            sent,
            [
                {
                    "type": "agent_webrtc_offer",
                    "callId": "call-1",
                    "sessionId": "backend-session-1",
                    "realtimeSessionId": "cf-session-1",
                    "tracks": {
                        "subscribe": "user_audio",
                        "publish": "assistant_audio",
                    },
                    "sessionDescription": {
                        "type": "offer",
                        "sdp": "local-sdp",
                    },
                }
            ],
        )

    async def test_handle_command_accepts_agent_control_positional_send_json(self) -> None:
        sent: list[dict] = []
        peer = FakePeerConnection()
        manager = RealtimeMediaManager(
            peer_connection_factory=lambda: peer,
            session_description_factory=lambda *, sdp, type: FakeDescription(sdp=sdp, type=type),
        )

        await manager.handle_command(
            {
                "type": "join_call",
                "callId": "call-1",
                "sessionId": "backend-session-1",
                "realtime": {"sessionId": "cf-session-1"},
            },
            sent.append,
        )

        self.assertEqual(sent[0]["type"], "agent_webrtc_offer")

    async def test_webrtc_answer_applies_remote_description(self) -> None:
        peer = FakePeerConnection()
        manager = RealtimeMediaManager(
            peer_connection_factory=lambda: peer,
            session_description_factory=lambda *, sdp, type: FakeDescription(sdp=sdp, type=type),
        )
        await manager.handle_command(
            {
                "type": "join_call",
                "callId": "call-1",
                "sessionId": "backend-session-1",
                "realtime": {"sessionId": "cf-session-1"},
            },
            send_json=AsyncMock(),
        )

        await manager.handle_command(
            {
                "type": "webrtc_answer",
                "callId": "call-1",
                "sessionDescription": {
                    "type": "answer",
                    "sdp": "remote-sdp",
                },
            },
            send_json=AsyncMock(),
        )

        self.assertEqual(len(peer.remote_descriptions), 1)
        self.assertEqual(peer.remote_descriptions[0].type, "answer")
        self.assertEqual(peer.remote_descriptions[0].sdp, "remote-sdp")

    async def test_webrtc_renegotiate_answers_remote_offer(self) -> None:
        sent: list[dict] = []
        peer = FakePeerConnection()
        manager = RealtimeMediaManager(
            peer_connection_factory=lambda: peer,
            session_description_factory=lambda *, sdp, type: FakeDescription(sdp=sdp, type=type),
        )
        await manager.handle_command(
            {
                "type": "join_call",
                "callId": "call-1",
                "sessionId": "backend-session-1",
                "realtime": {"sessionId": "cf-session-1"},
            },
            send_json=AsyncMock(),
        )

        await manager.handle_command(
            {
                "type": "webrtc_renegotiate",
                "callId": "call-1",
                "realtimeSessionId": "cf-session-1",
                "sessionDescription": {
                    "type": "offer",
                    "sdp": "cloudflare-renegotiation-offer-sdp",
                },
            },
            send_json=sent.append,
        )

        self.assertEqual(len(peer.remote_descriptions), 1)
        self.assertEqual(peer.remote_descriptions[0].type, "offer")
        self.assertEqual(peer.remote_descriptions[0].sdp, "cloudflare-renegotiation-offer-sdp")
        self.assertEqual(
            sent,
            [
                {
                    "type": "agent_webrtc_renegotiation_answer",
                    "callId": "call-1",
                    "sessionId": "backend-session-1",
                    "realtimeSessionId": "cf-session-1",
                    "sessionDescription": {
                        "type": "answer",
                        "sdp": "local-answer-sdp",
                    },
                }
            ],
        )

    async def test_agent_control_error_is_handled_without_response(self) -> None:
        sent: list[dict] = []
        manager = RealtimeMediaManager()

        await manager.handle_command(
            {
                "type": "agent_control_error",
                "callId": "call-1",
                "message": "renegotiation failed",
            },
            send_json=sent.append,
        )

        self.assertEqual(sent, [])

    async def test_end_call_closes_peer_connection(self) -> None:
        peer = FakePeerConnection()
        manager = RealtimeMediaManager(
            peer_connection_factory=lambda: peer,
            session_description_factory=lambda *, sdp, type: FakeDescription(sdp=sdp, type=type),
        )
        await manager.handle_command(
            {
                "type": "join_call",
                "callId": "call-1",
                "sessionId": "backend-session-1",
                "realtime": {"sessionId": "cf-session-1"},
            },
            send_json=AsyncMock(),
        )

        await manager.handle_command({"type": "end_call", "callId": "call-1"}, send_json=AsyncMock())

        self.assertTrue(peer.closed)


if __name__ == "__main__":
    unittest.main()
