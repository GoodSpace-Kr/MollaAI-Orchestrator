from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import AsyncMock, Mock

sys.modules.setdefault("fastapi", types.SimpleNamespace(WebSocket=object))
sys.modules.setdefault("httpx", types.SimpleNamespace(ReadError=RuntimeError, AsyncClient=object))
sys.modules.setdefault("numpy", types.SimpleNamespace())
sys.modules.setdefault("websockets", types.SimpleNamespace(ClientConnection=object))

from config import OrchestratorConfig
from session import CallSession
from transcript_store import ConversationTurn


class CallSessionHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.websocket = Mock()
        self.stt_client = Mock()
        self.llm_client = Mock()
        self.tts_client = Mock()
        self.transcript_store = Mock()
        self.transcript_store.get_session = AsyncMock(
            return_value={
                "turns": [
                    {"index": 1, "user_text": "u1", "assistant_text": "a1"},
                    {"index": 2, "user_text": "u2", "assistant_text": "a2"},
                    {"index": 3, "user_text": "u3", "assistant_text": "a3"},
                    {"index": 4, "user_text": "u4", "assistant_text": "a4"},
                    {"index": 5, "user_text": "u5", "assistant_text": "a5"},
                    {"index": 6, "user_text": "u6", "assistant_text": "a6"},
                    {"index": 7, "user_text": "current", "assistant_text": ""},
                ]
            }
        )

        self.session = CallSession(
            websocket=self.websocket,
            config=OrchestratorConfig(),
            stt_client=self.stt_client,
            llm_client=self.llm_client,
            tts_client=self.tts_client,
            transcript_store=self.transcript_store,
            audio_storage=None,
        )
        self.session.session_id = "call-1"

    async def test_recent_llm_history_returns_last_five_completed_turns(self) -> None:
        history = await self.session._recent_llm_history(7)

        self.assertEqual(
            history,
            [
                {"role": "user", "content": "u2"},
                {"role": "assistant", "content": "a2"},
                {"role": "user", "content": "u3"},
                {"role": "assistant", "content": "a3"},
                {"role": "user", "content": "u4"},
                {"role": "assistant", "content": "a4"},
                {"role": "user", "content": "u5"},
                {"role": "assistant", "content": "a5"},
                {"role": "user", "content": "u6"},
                {"role": "assistant", "content": "a6"},
            ],
        )

    async def test_pending_user_query_combines_turns_after_last_completed_response(self) -> None:
        self.session._last_generated_response_turn_index = 4
        current_turn = ConversationTurn(
            index=7,
            created_at="2026-05-31T00:00:00+00:00",
            user_text="What?",
            sample_rate=16000,
        )

        query, history_before_turn_index = await self.session._pending_user_query(current_turn)

        self.assertEqual(query, "u5\nu6\ncurrent")
        self.assertEqual(history_before_turn_index, 5)

    async def test_pending_user_query_excludes_generated_turn(self) -> None:
        self.session._last_generated_response_turn_index = 6
        current_turn = ConversationTurn(
            index=7,
            created_at="2026-05-31T00:00:00+00:00",
            user_text="current",
            sample_rate=16000,
        )

        query, history_before_turn_index = await self.session._pending_user_query(current_turn)

        self.assertEqual(query, "current")
        self.assertEqual(history_before_turn_index, 7)

    async def test_assistant_echo_text_is_ignored(self) -> None:
        self.session._remember_assistant_echo_text("I'm doing well, thanks for asking.")

        self.assertTrue(self.session._is_likely_assistant_echo("I am doing well thanks for asking"))
        self.assertFalse(self.session._is_likely_assistant_echo("Actually, I wanted to ask something else"))

    async def test_user_acknowledgement_is_not_treated_as_echo(self) -> None:
        self.session._remember_assistant_echo_text("Great to meet you!")

        self.assertFalse(self.session._is_likely_assistant_echo("Yeah great to meet you too."))
        self.assertTrue(self.session._is_likely_assistant_echo("Great to meet you."))

    async def test_tts_text_sanitization_removes_emoji(self) -> None:
        self.assertEqual(
            self.session._sanitize_tts_text("😊 Stephen Curry 🏀⚽ is great."),
            "Stephen Curry is great.",
        )


if __name__ == "__main__":
    unittest.main()
