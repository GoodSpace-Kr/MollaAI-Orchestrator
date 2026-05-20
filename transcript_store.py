from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
from pathlib import Path
import re
from typing import Any


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _safe_session_id(session_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", session_id).strip("._")
    return cleaned or "call-session"


@dataclass(slots=True)
class ConversationTurn:
    index: int
    created_at: str
    user_text: str
    sample_rate: int
    user_audio_key: str | None = None
    assistant_text: str = ""
    assistant_created_at: str | None = None
    llm_turn: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "created_at": self.created_at,
            "user_text": self.user_text,
            "sample_rate": self.sample_rate,
            "user_audio_key": self.user_audio_key,
            "assistant_text": self.assistant_text,
            "assistant_created_at": self.assistant_created_at,
            "llm_turn": self.llm_turn,
        }

    def utterance_payload(self) -> dict[str, Any]:
        payload = {
            "text": self.user_text,
            "sampleRate": self.sample_rate,
        }
        if self.user_audio_key:
            payload["audioKey"] = self.user_audio_key
        return payload

    def payload(self) -> dict[str, Any]:
        data = {
            "index": self.index,
            "createdAt": self.created_at,
            "user": self.utterance_payload(),
        }
        if self.assistant_text.strip():
            assistant = {
                "text": self.assistant_text,
            }
            if self.assistant_created_at:
                assistant["createdAt"] = self.assistant_created_at
            data["assistant"] = assistant
        return data


@dataclass(slots=True)
class TranscriptSession:
    session_id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    started_at: str = field(default_factory=_utc_now)
    ended_at: str | None = None
    turns: list[ConversationTurn] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "metadata": deepcopy(self.metadata),
            "turns": [turn.to_dict() for turn in self.turns],
        }


class TranscriptStore:
    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._sessions: dict[str, TranscriptSession] = {}
        self._lock = asyncio.Lock()

    async def start_session(self, session_id: str, metadata: dict[str, Any] | None = None) -> None:
        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                session = self._load_session(session_id) or TranscriptSession(session_id=session_id)
                self._sessions[session_id] = session

            session.metadata = deepcopy(metadata or {})
            if session.ended_at is not None:
                session.ended_at = None
            self._persist_session(session)

    async def append_turn(
        self,
        session_id: str,
        *,
        user_text: str,
        sample_rate: int,
        audio_key: str | None = None,
    ) -> ConversationTurn:
        cleaned = user_text.strip()
        if not cleaned:
            raise ValueError("Conversation turn text must not be empty.")

        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                session = self._load_session(session_id) or TranscriptSession(session_id=session_id)
                self._sessions[session_id] = session

            turn = ConversationTurn(
                index=len(session.turns) + 1,
                created_at=_utc_now(),
                user_text=cleaned,
                sample_rate=sample_rate,
                user_audio_key=audio_key,
            )
            session.turns.append(turn)
            self._persist_session(session)
            return turn

    async def set_turn_audio_key(
        self,
        session_id: str,
        *,
        turn_index: int,
        audio_key: str,
    ) -> None:
        if not audio_key.strip():
            return

        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                session = self._load_session(session_id)
                if session is None:
                    return
                self._sessions[session_id] = session

            for turn in session.turns:
                if turn.index != turn_index:
                    continue
                turn.user_audio_key = audio_key.strip()
                self._persist_session(session)
                return

            raise ValueError(f"Conversation turn {turn_index} not found for session {session_id}.")

    async def set_assistant_response(
        self,
        session_id: str,
        *,
        turn_index: int,
        text: str,
        llm_turn: int | None = None,
    ) -> None:
        cleaned = text.strip()
        if not cleaned:
            return

        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                session = self._load_session(session_id)
                if session is None:
                    return
                self._sessions[session_id] = session

            for turn in session.turns:
                if turn.index != turn_index:
                    continue
                turn.assistant_text = cleaned
                turn.assistant_created_at = _utc_now()
                turn.llm_turn = llm_turn
                self._persist_session(session)
                return

            raise ValueError(f"Conversation turn {turn_index} not found for session {session_id}.")

    async def end_session(self, session_id: str) -> None:
        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                session = self._load_session(session_id)
                if session is None:
                    return
                self._sessions[session_id] = session

            if session.ended_at is None:
                session.ended_at = _utc_now()
                self._persist_session(session)

    async def get_session(self, session_id: str) -> dict[str, Any] | None:
        async with self._lock:
            session = self._sessions.get(session_id)
            if session is not None:
                return session.to_dict()

            loaded = self._load_session(session_id)
            if loaded is None:
                return None
            self._sessions[session_id] = loaded
            return loaded.to_dict()

    async def list_sessions(self, limit: int = 50) -> list[dict[str, Any]]:
        async with self._lock:
            sessions: list[TranscriptSession] = list(self._sessions.values())
            seen = {session.session_id for session in sessions}

            for path in sorted(self.base_dir.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
                loaded = self._load_session(path.stem)
                if loaded is not None:
                    if loaded.session_id in seen:
                        continue
                    sessions.append(loaded)
                    seen.add(loaded.session_id)

            sessions.sort(key=lambda item: item.started_at, reverse=True)
            return [
                {
                    "session_id": session.session_id,
                    "started_at": session.started_at,
                    "ended_at": session.ended_at,
                    "entry_count": len(session.turns),
                    "turn_count": len(session.turns),
                    "metadata": deepcopy(session.metadata),
                }
                for session in sessions[:limit]
            ]

    def build_completed_payload(self, session: dict[str, Any]) -> dict[str, Any]:
        turns = [self._turn_from_dict(item) for item in session.get("turns", [])]
        return {
            "turns": [turn.payload() for turn in turns],
        }

    def render_transcript_text(self, session: dict[str, Any]) -> str:
        lines: list[str] = []
        for turn_data in session.get("turns", []):
            turn = self._turn_from_dict(turn_data)
            lines.append(f"User: {turn.user_text}")
            if turn.assistant_text.strip():
                lines.append(f"Assistant: {turn.assistant_text.strip()}")
        return "\n".join(lines)

    def _session_path(self, session_id: str) -> Path:
        return self.base_dir / f"{_safe_session_id(session_id)}.json"

    def _persist_session(self, session: TranscriptSession) -> None:
        self._session_path(session.session_id).write_text(
            json.dumps(session.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_session(self, session_id: str) -> TranscriptSession | None:
        path = self._session_path(session_id)
        if not path.exists():
            return None

        data = json.loads(path.read_text(encoding="utf-8"))
        return TranscriptSession(
            session_id=str(data.get("session_id", session_id)),
            started_at=str(data.get("started_at", _utc_now())),
            ended_at=data.get("ended_at"),
            metadata=dict(data.get("metadata", {}) or {}),
            turns=self._load_turns(data),
        )

    def _turn_from_dict(self, data: dict[str, Any]) -> ConversationTurn:
        return ConversationTurn(
            index=int(data.get("index", 0) or 0),
            created_at=str(data.get("created_at", _utc_now())),
            user_text=str(data.get("user_text", "")).strip(),
            sample_rate=int(data.get("sample_rate", 0) or 0),
            user_audio_key=self._coerce_audio_key(data),
            assistant_text=str(data.get("assistant_text", "")).strip(),
            assistant_created_at=data.get("assistant_created_at"),
            llm_turn=int(data["llm_turn"]) if data.get("llm_turn") is not None else None,
        )

    def _load_turns(self, data: dict[str, Any]) -> list[ConversationTurn]:
        if "turns" in data:
            return [self._turn_from_dict(item) for item in data.get("turns", [])]

        turns: list[ConversationTurn] = []
        pending_user: dict[str, Any] | None = None
        for entry in data.get("entries", []):
            source = str(entry.get("source", "")).strip().lower()
            kind = str(entry.get("kind", "")).strip().lower()
            text = str(entry.get("text", "")).strip()
            if not text:
                continue

            if source == "llm" and kind == "request":
                pending_user = entry
                continue

            if source == "llm" and kind == "response" and pending_user is not None:
                turns.append(
                    ConversationTurn(
                        index=len(turns) + 1,
                        created_at=str(pending_user.get("created_at", _utc_now())),
                        user_text=str(pending_user.get("text", "")).strip(),
                        sample_rate=0,
                        assistant_text=text,
                        assistant_created_at=str(entry.get("created_at", _utc_now())),
                        llm_turn=entry.get("turn"),
                    )
                )
                pending_user = None

        if pending_user is not None:
            turns.append(
                ConversationTurn(
                    index=len(turns) + 1,
                    created_at=str(pending_user.get("created_at", _utc_now())),
                    user_text=str(pending_user.get("text", "")).strip(),
                    sample_rate=0,
                    llm_turn=pending_user.get("turn"),
                )
            )

        return turns

    def _coerce_audio_key(self, data: dict[str, Any]) -> str | None:
        raw_audio_key = data.get("user_audio_key")
        audio_key = str(raw_audio_key).strip() if raw_audio_key is not None else ""
        if audio_key:
            return audio_key
        raw_legacy_key = data.get("audio_key")
        legacy_key = str(raw_legacy_key).strip() if raw_legacy_key is not None else ""
        return legacy_key or None
