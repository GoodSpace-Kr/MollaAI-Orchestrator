from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
import json
import re
from typing import Any


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _safe_session_id(session_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", session_id).strip("._")
    return cleaned or "call-session"


@dataclass(slots=True)
class TranscriptSession:
    session_id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    started_at: str = field(default_factory=_utc_now)
    ended_at: str | None = None
    entries: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "metadata": deepcopy(self.metadata),
            "entries": deepcopy(self.entries),
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

    async def append_event(
        self,
        session_id: str,
        *,
        source: str,
        kind: str,
        text: str,
        **extra: Any,
    ) -> None:
        cleaned = text.strip()
        if not cleaned:
            return

        async with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                session = self._load_session(session_id) or TranscriptSession(session_id=session_id)
                self._sessions[session_id] = session

            entry = {
                "index": len(session.entries) + 1,
                "created_at": _utc_now(),
                "source": source,
                "kind": kind,
                "text": cleaned,
            }
            entry.update({key: value for key, value in extra.items() if value is not None})
            session.entries.append(entry)
            self._persist_session(session)

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
                    "entry_count": len(session.entries),
                    "metadata": deepcopy(session.metadata),
                }
                for session in sessions[:limit]
            ]

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
            entries=list(data.get("entries", []) or []),
        )
