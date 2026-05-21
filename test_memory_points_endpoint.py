from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, Mock, patch

from fastapi.testclient import TestClient

import main


class MemoryPointsIngressTests(unittest.TestCase):
    def test_forwards_points_to_llm_memory_endpoint(self) -> None:
        client = TestClient(main.app)
        with patch("main.LlmHttpClient") as llm_client_cls:
            llm_client = Mock()
            llm_client.upsert_memory_points = AsyncMock(return_value={"status": "ok", "count": 2})
            llm_client.close = AsyncMock(return_value=None)
            llm_client_cls.return_value = llm_client

            response = client.post(
                "/memory/points",
                json={
                    "points": [
                        {
                            "id": "uuid-1",
                            "vector": [0.1, 0.2],
                            "payload": {
                                "userId": "user-123",
                                "phoneNumber": "01012345678",
                                "userText": "I received the wrong item.",
                                "assistantText": "I'm sorry to hear that. What item did you expect?",
                                "createdAt": "2026-05-20T07:08:33.742000Z",
                                "audioKey": "calls/a/turns/5.wav",
                            },
                        },
                        {
                            "id": "uuid-2",
                            "vector": [0.3, 0.4],
                            "payload": {
                                "userId": "user-123",
                                "phoneNumber": "01012345678",
                                "userText": "I want to get an exchange.",
                                "assistantText": "Sure. Do you still have the receipt?",
                                "createdAt": "2026-05-20T07:09:10.000000Z",
                                "audioKey": "calls/a/turns/6.wav",
                            },
                        },
                    ]
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "count": 2})
        llm_client.upsert_memory_points.assert_awaited_once()
        forwarded_payload = llm_client.upsert_memory_points.call_args.args[0]
        self.assertEqual(len(forwarded_payload["points"]), 2)
        self.assertEqual(forwarded_payload["points"][0]["payload"]["userText"], "I received the wrong item.")
        self.assertEqual(forwarded_payload["points"][1]["payload"]["audioKey"], "calls/a/turns/6.wav")

    def test_allows_nullable_user_id_and_assistant_text(self) -> None:
        client = TestClient(main.app)
        with patch("main.LlmHttpClient") as llm_client_cls:
            llm_client = Mock()
            llm_client.upsert_memory_points = AsyncMock(return_value={"status": "ok", "count": 1})
            llm_client.close = AsyncMock(return_value=None)
            llm_client_cls.return_value = llm_client

            response = client.post(
                "/memory/points",
                json={
                    "points": [
                        {
                            "id": "uuid-1",
                            "vector": [0.1, 0.2],
                            "payload": {
                                "userId": None,
                                "phoneNumber": "01012345678",
                                "userText": "hello",
                                "assistantText": None,
                                "createdAt": "2026-05-20T07:08:33.742000Z",
                                "audioKey": "calls/a/turns/5.wav",
                            },
                        }
                    ]
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok", "count": 1})
        forwarded_payload = llm_client.upsert_memory_points.call_args.args[0]
        self.assertIsNone(forwarded_payload["points"][0]["payload"]["userId"])
        self.assertIsNone(forwarded_payload["points"][0]["payload"]["assistantText"])


if __name__ == "__main__":
    unittest.main()
