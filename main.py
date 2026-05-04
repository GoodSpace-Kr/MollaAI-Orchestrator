from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn

from clients import LlmHttpClient, SttWsClient, TtsHttpClient
from config import OrchestratorConfig
from session import CallSession


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.config = OrchestratorConfig.from_env()
    yield


app = FastAPI(title="Molla Orchestrator", lifespan=lifespan)


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.websocket("/orchestrator/ws")
async def orchestrator_websocket(websocket: WebSocket) -> None:
    await websocket.accept()

    config: OrchestratorConfig = websocket.app.state.config
    session = CallSession(
        websocket=websocket,
        config=config,
        stt_client=SttWsClient(config.stt_ws_url),
        llm_client=LlmHttpClient(config.llm_http_url),
        tts_client=TtsHttpClient(config.tts_http_url),
    )
    await session.open()

    try:
        while True:
            payload = await websocket.receive_json()
            await _handle_event(session, payload)
    except WebSocketDisconnect:
        pass
    finally:
        await session.close()


async def _handle_event(session: CallSession, payload: dict[str, Any]) -> None:
    event = str(payload.get("event", "")).strip().lower()

    if event == "connected":
        return
    if event == "start":
        await session.start(payload)
        return
    if event == "media":
        await session.handle_media(payload)
        return
    if event == "stop":
        await session.close()
        return


def main() -> None:
    config = OrchestratorConfig.from_env()
    uvicorn.run(app, host=config.host, port=config.port)


if __name__ == "__main__":
    main()
