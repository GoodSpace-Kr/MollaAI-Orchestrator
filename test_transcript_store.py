from __future__ import annotations

import base64
import json
from pathlib import Path
import tempfile
import unittest

from transcript_store import TranscriptStore


class TranscriptStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_payload_includes_session_timing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TranscriptStore(tmpdir)
            session_payload = {
                "session_id": "timed-session",
                "started_at": "2026-05-20T00:00:00+00:00",
                "ended_at": "2026-05-20T00:02:05+00:00",
                "metadata": {},
                "turns": [],
            }
            Path(tmpdir, "timed-session.json").write_text(json.dumps(session_payload), encoding="utf-8")

            session = await store.get_session("timed-session")
            self.assertIsNotNone(session)
            payload = store.build_completed_payload(session)

            self.assertEqual(payload["startedAt"], "2026-05-20T00:00:00+00:00")
            self.assertEqual(payload["endedAt"], "2026-05-20T00:02:05+00:00")
            self.assertEqual(payload["durationMinutes"], 2)

    async def test_completed_payload_uses_structured_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = TranscriptStore(tmpdir)
            await store.start_session("session-1", metadata={"call_id": "call-1"})

            first_turn = await store.append_turn(
                "session-1",
                user_text="hello there",
                sample_rate=16000,
                audio_key="calls/call-1/turns/1.wav",
            )
            await store.set_assistant_response(
                "session-1",
                turn_index=first_turn.index,
                text="hi",
                llm_turn=1,
            )

            second_turn = await store.append_turn(
                "session-1",
                user_text="second question",
                sample_rate=16000,
                audio_key=None,
            )
            await store.set_assistant_response(
                "session-1",
                turn_index=second_turn.index,
                text="second answer",
                llm_turn=2,
            )

            session = await store.get_session("session-1")
            self.assertIsNotNone(session)
            payload = store.build_completed_payload(session)

            self.assertEqual(len(payload["turns"]), 2)
            self.assertEqual(payload["turns"][0]["index"], 1)
            self.assertEqual(payload["turns"][0]["user"]["text"], "hello there")
            self.assertEqual(payload["turns"][0]["user"]["audioKey"], "calls/call-1/turns/1.wav")
            self.assertEqual(payload["turns"][0]["assistant"]["text"], "hi")
            self.assertEqual(payload["turns"][1]["index"], 2)
            self.assertEqual(payload["turns"][1]["user"]["text"], "second question")
            self.assertNotIn("audio", payload["turns"][1]["user"])
            self.assertNotIn("audioKey", payload["turns"][1]["user"])
            self.assertEqual(payload["turns"][1]["assistant"]["text"], "second answer")

    async def test_loads_legacy_entries_as_turns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            legacy_session = {
                "session_id": "legacy-session",
                "started_at": "2026-05-20T00:00:00+00:00",
                "ended_at": None,
                "metadata": {},
                "entries": [
                    {
                        "index": 1,
                        "created_at": "2026-05-20T00:00:01+00:00",
                        "source": "llm",
                        "kind": "request",
                        "text": "legacy user",
                        "turn": 1,
                    },
                    {
                        "index": 2,
                        "created_at": "2026-05-20T00:00:02+00:00",
                        "source": "llm",
                        "kind": "response",
                        "text": "legacy assistant",
                        "turn": 1,
                    },
                ],
            }
            Path(tmpdir, "legacy-session.json").write_text(json.dumps(legacy_session), encoding="utf-8")

            store = TranscriptStore(tmpdir)
            session = await store.get_session("legacy-session")
            self.assertIsNotNone(session)
            payload = store.build_completed_payload(session)

            self.assertEqual(len(payload["turns"]), 1)
            self.assertEqual(payload["turns"][0]["user"]["text"], "legacy user")
            self.assertNotIn("audio", payload["turns"][0]["user"])
            self.assertNotIn("audioKey", payload["turns"][0]["user"])
            self.assertEqual(payload["turns"][0]["assistant"]["text"], "legacy assistant")


if __name__ == "__main__":
    unittest.main()
