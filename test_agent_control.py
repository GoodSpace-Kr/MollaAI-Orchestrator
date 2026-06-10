from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import ANY, AsyncMock, patch

from agent_control import AgentControlClient, build_agent_control_url
from config import OrchestratorConfig


class FakeWebSocket:
    def __init__(self, messages: list[str]) -> None:
        self.messages = messages
        self.sent: list[str] = []

    async def __aenter__(self) -> "FakeWebSocket":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def __aiter__(self) -> "FakeWebSocket":
        return self

    async def __anext__(self) -> str:
        if not self.messages:
            raise StopAsyncIteration
        return self.messages.pop(0)

    async def send(self, message: str) -> None:
        self.sent.append(message)


class AgentControlTests(unittest.IsolatedAsyncioTestCase):
    def test_build_agent_control_url_appends_agent_token(self) -> None:
        url = build_agent_control_url(
            "wss://api.example.com/api/v1/agents/control",
            "agent secret",
        )

        self.assertEqual(
            url,
            "wss://api.example.com/api/v1/agents/control?token=agent+secret",
        )

    def test_build_agent_control_url_preserves_existing_query(self) -> None:
        url = build_agent_control_url(
            "wss://api.example.com/api/v1/agents/control?agentId=school-1",
            "agent-token",
        )

        self.assertEqual(
            url,
            "wss://api.example.com/api/v1/agents/control?agentId=school-1&token=agent-token",
        )

    def test_config_loads_agent_control_env(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ORCH_AGENT_CONTROL_WSS_URL": "wss://api.example.com/api/v1/agents/control",
                "ORCH_AGENT_TOKEN": "agent-token",
                "ORCH_AGENT_RECONNECT_DELAY_SECS": "0.25",
                "ORCH_STT_WS_URL": "ws://stt.local:8000/stt/ws",
                "ORCH_LLM_HTTP_URL": "http://llm.local:8001",
                "ORCH_TTS_HTTP_URL": "http://tts.local:8002",
            },
            clear=False,
        ):
            config = OrchestratorConfig.from_env()

        self.assertEqual(config.agent_control_wss_url, "wss://api.example.com/api/v1/agents/control")
        self.assertEqual(config.agent_token, "agent-token")
        self.assertEqual(config.agent_reconnect_delay_secs, 0.25)

    async def test_connect_once_handles_ping_and_join_call(self) -> None:
        websocket = FakeWebSocket(
            [
                '{"type":"ping"}',
                '{"type":"join_call","callId":"call-1","realtime":{"sessionId":"rt-1"}}',
            ]
        )
        connect = AsyncMock(return_value=websocket)
        handler = AsyncMock(return_value=None)
        client = AgentControlClient(
            url="wss://api.example.com/api/v1/agents/control",
            token="agent-token",
            connect=connect,
            command_handler=handler,
        )

        await client.connect_once()

        connect.assert_awaited_once_with(
            "wss://api.example.com/api/v1/agents/control?token=agent-token",
            max_size=None,
        )
        self.assertEqual(websocket.sent, ['{"type": "pong"}'])
        handler.assert_awaited_once_with(
            {"type": "join_call", "callId": "call-1", "realtime": {"sessionId": "rt-1"}},
            ANY,
        )

    async def test_run_forever_reconnects_after_connect_failure(self) -> None:
        connect = AsyncMock(side_effect=[OSError("network down"), asyncio.CancelledError()])
        sleep = AsyncMock(return_value=None)
        client = AgentControlClient(
            url="wss://api.example.com/api/v1/agents/control",
            token="agent-token",
            connect=connect,
            sleep=sleep,
            reconnect_delay_secs=0.01,
        )

        with self.assertRaises(asyncio.CancelledError):
            await client.run_forever()

        self.assertEqual(connect.await_count, 2)
        sleep.assert_awaited_once_with(0.01)


if __name__ == "__main__":
    unittest.main()
