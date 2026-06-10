from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


logger = logging.getLogger("molla.orchestrator.agent_control")

SendJson = Callable[[dict[str, Any]], Awaitable[None]]
CommandHandler = Callable[[dict[str, Any], SendJson], Awaitable[None]]
ConnectCallable = Callable[..., Awaitable[Any]]
SleepCallable = Callable[[float], Awaitable[Any]]


def build_agent_control_url(url: str, token: str) -> str:
    parts = urlsplit(url)
    query = parse_qsl(parts.query, keep_blank_values=True)
    query = [(key, value) for key, value in query if key != "token"]
    query.append(("token", token))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


class AgentControlClient:
    def __init__(
        self,
        *,
        url: str,
        token: str,
        connect: ConnectCallable | None = None,
        sleep: SleepCallable | None = None,
        command_handler: CommandHandler | None = None,
        reconnect_delay_secs: float = 5.0,
    ) -> None:
        self.url = url
        self.token = token
        self.control_url = build_agent_control_url(url, token)
        self.connect = connect or self._connect
        self.sleep = sleep or asyncio.sleep
        self.command_handler = command_handler or self._default_command_handler
        self.reconnect_delay_secs = reconnect_delay_secs

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        while stop_event is None or not stop_event.is_set():
            try:
                await self.connect_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("agent_control_disconnected error=%s", exc)

            if stop_event is not None and stop_event.is_set():
                break
            await self.sleep(self.reconnect_delay_secs)

    async def connect_once(self) -> None:
        logger.info("agent_control_connecting url=%s", self.url)
        websocket = await self.connect(self.control_url, max_size=None)
        async with websocket:
            logger.info("agent_control_connected url=%s", self.url)
            async for raw_message in websocket:
                if isinstance(raw_message, bytes):
                    continue
                await self._handle_message(websocket, raw_message)

    async def send_json(self, websocket: Any, payload: dict[str, Any]) -> None:
        await websocket.send(json.dumps(payload, ensure_ascii=False))

    async def _handle_message(self, websocket: Any, raw_message: str) -> None:
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            logger.warning("agent_control_invalid_json message=%r", raw_message[:200])
            return

        if not isinstance(payload, dict):
            logger.warning("agent_control_invalid_payload payload=%r", payload)
            return

        message_type = str(payload.get("type", "")).strip()
        if message_type == "ping":
            await self.send_json(websocket, {"type": "pong"})
            return

        await self.command_handler(payload, lambda response: self.send_json(websocket, response))

    async def _default_command_handler(self, payload: dict[str, Any], send_json: SendJson) -> None:
        logger.info(
            "agent_control_command type=%s call_id=%s session_id=%s",
            payload.get("type", ""),
            payload.get("callId", ""),
            payload.get("sessionId", ""),
        )

    async def _connect(self, url: str, **kwargs: Any) -> Any:
        import websockets

        return await websockets.connect(url, **kwargs)
