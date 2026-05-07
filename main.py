from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from xml.sax.saxutils import escape

from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
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


@app.api_route("/voice", methods=["GET", "POST"])
async def clawops_voice_webhook(request: Request) -> Response:
    config: OrchestratorConfig = request.app.state.config
    params = await _get_voice_params(request)
    stream_url = _build_stream_url(request, config)
    xml = _build_voiceml_response(stream_url, params)
    return Response(content=xml, media_type="application/xml")


@app.websocket("/orchestrator/ws")
async def orchestrator_websocket(websocket: WebSocket) -> None:
    await _run_orchestrator_session(websocket)


@app.websocket("/stream")
async def clawops_stream_websocket(websocket: WebSocket) -> None:
    await _run_orchestrator_session(websocket)


async def _run_orchestrator_session(websocket: WebSocket) -> None:
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


async def _get_voice_params(request: Request) -> dict[str, str]:
    if request.method == "POST":
        form = await request.form()
        return {key: value for key, value in form.items() if isinstance(value, str)}
    return {key: value for key, value in request.query_params.items()}


def _build_stream_url(request: Request, config: OrchestratorConfig) -> str:
    public_base_url = config.public_base_url or str(request.base_url).rstrip("/")
    parts = urlsplit(public_base_url)
    scheme = "wss" if parts.scheme == "https" else "ws"
    path = config.stream_websocket_path if config.stream_websocket_path.startswith("/") else f"/{config.stream_websocket_path}"
    return urlunsplit((scheme, parts.netloc, path, "", ""))


def _build_voiceml_response(stream_url: str, params: dict[str, str]) -> str:
    call_id = escape(params.get("CallId", ""))
    from_number = escape(params.get("From", ""))
    to_number = escape(params.get("To", ""))
    parameter_lines = [
        f'      <Parameter name="callId" value="{call_id}"/>',
        f'      <Parameter name="from" value="{from_number}"/>',
        f'      <Parameter name="to" value="{to_number}"/>',
    ]
    parameter_block = "\n".join(parameter_lines)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<Response>\n"
        "  <Connect>\n"
        f'    <Stream url="{escape(stream_url)}" track="inbound">\n'
        f"{parameter_block}\n"
        "    </Stream>\n"
        "  </Connect>\n"
        "</Response>"
    )


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
