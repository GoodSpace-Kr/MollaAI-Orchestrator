from __future__ import annotations

import asyncio
import base64
import unittest
from unittest.mock import AsyncMock, Mock, patch

from config import OrchestratorConfig
from session import CallSession


class CallSessionTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.websocket = Mock()
        self.websocket.send_json = AsyncMock(return_value=None)
        self.websocket.close = AsyncMock(return_value=None)

        self.stt_client = Mock()
        self.stt_client.connect = AsyncMock(return_value=None)
        self.stt_client.start = AsyncMock(return_value=None)
        self.stt_client.send_audio = AsyncMock(return_value=None)
        self.stt_client.close = AsyncMock(return_value=None)

        self.llm_client = Mock()
        self.llm_client.close = AsyncMock(return_value=None)

        self.tts_client = Mock()
        self.tts_client.close = AsyncMock(return_value=None)

        self.transcript_store = Mock()
        self.transcript_store.start_session = AsyncMock(return_value=None)
        self.transcript_store.end_session = AsyncMock(return_value=None)
        self.transcript_store.get_session = AsyncMock(return_value={"turns": []})

        self.session = CallSession(
            websocket=self.websocket,
            config=OrchestratorConfig(),
            stt_client=self.stt_client,
            llm_client=self.llm_client,
            tts_client=self.tts_client,
            transcript_store=self.transcript_store,
            audio_storage=None,
        )

    async def asyncTearDown(self) -> None:
        await self.session.close()

    async def test_start_schedules_timeout_from_remaining_minutes(self) -> None:
        with (
            patch.object(self.session, "_play_greeting", new=AsyncMock(return_value=None)),
            patch.object(self.session, "_consume_stt_events", new=AsyncMock(return_value=None)),
            patch.object(self.session, "_send_session_started", new=AsyncMock(return_value=None)),
        ):
            await self.session.start(
                {
                    "start": {
                        "streamId": "stream-1",
                        "callId": "call-1",
                        "remainingMinutes": 3,
                    }
                }
            )

        self.assertEqual(self.session._remaining_minutes, 3)
        self.assertIsNotNone(self.session._timeout_task)
        self.assertFalse(self.session._timeout_task.done())

    async def test_timeout_announces_and_closes_websocket(self) -> None:
        self.session.context.stream_id = "stream-1"
        self.session.context.call_id = "call-1"
        self.session.session_id = "call-1"

        with patch.object(self.session, "_play_system_message", new=AsyncMock(return_value=None)) as play_message:
            await self.session._handle_remaining_minutes_expired()

        self.assertFalse(self.session._accepting_user_audio)
        play_message.assert_awaited_once_with(
            "오늘 잔여시간이 모두 소진되었습니다. 통화가 자동으로 종료됩니다. 감사합니다."
        )
        self.websocket.close.assert_awaited_once()

        await self.session.handle_media(
            {
                "media": {
                    "payload": base64.b64encode(b"\xff").decode("ascii"),
                }
            }
        )
        self.stt_client.send_audio.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
