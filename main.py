from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from typing import Any
from urllib.parse import parse_qsl
from urllib.parse import urlsplit, urlunsplit
from xml.sax.saxutils import escape

from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
import uvicorn

from clients import LlmHttpClient, SttWsClient, TtsHttpClient
from config import OrchestratorConfig
from session import CallSession
from transcript_store import TranscriptStore


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("molla.orchestrator")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.config = OrchestratorConfig.from_env()
    app.state.transcript_store = TranscriptStore(app.state.config.transcript_dir)
    logger.info(
        "orchestrator_started host=%s port=%s public_base_url=%s transcript_dir=%s stt_ws_url=%s llm_http_url=%s tts_http_url=%s",
        app.state.config.host,
        app.state.config.port,
        app.state.config.public_base_url,
        app.state.config.transcript_dir,
        app.state.config.stt_ws_url,
        app.state.config.llm_http_url,
        app.state.config.tts_http_url,
    )
    yield


app = FastAPI(title="Molla Orchestrator", lifespan=lifespan)


@app.get("/healthz")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/transcripts")
async def list_transcripts(request: Request, limit: int = 20) -> dict[str, Any]:
    transcript_store: TranscriptStore = request.app.state.transcript_store
    return {"sessions": await transcript_store.list_sessions(limit=max(1, min(limit, 100)))}


@app.get("/transcripts/{session_id}")
async def get_transcript(session_id: str, request: Request) -> dict[str, Any]:
    transcript_store: TranscriptStore = request.app.state.transcript_store
    session = await transcript_store.get_session(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Transcript not found")
    return session


@app.api_route("/voice", methods=["GET", "POST"])
async def clawops_voice_webhook(request: Request) -> Response:
    config: OrchestratorConfig = request.app.state.config
    params = await _get_voice_params(request)
    stream_url = _build_stream_url(request, config)
    xml = _build_voiceml_response(stream_url, params)
    logger.info(
        "voice_webhook method=%s client=%s call_id=%s from=%s to=%s stream_url=%s",
        request.method,
        request.client.host if request.client else "",
        params.get("CallId", ""),
        params.get("From", ""),
        params.get("To", ""),
        stream_url,
    )
    return Response(content=xml, media_type="application/xml")


@app.websocket("/orchestrator/ws")
async def orchestrator_websocket(websocket: WebSocket) -> None:
    await _run_orchestrator_session(websocket)


@app.websocket("/stream")
async def clawops_stream_websocket(websocket: WebSocket) -> None:
    await _run_orchestrator_session(websocket)


async def _run_orchestrator_session(websocket: WebSocket) -> None:
    client_host = websocket.client.host if websocket.client else ""
    client_port = websocket.client.port if websocket.client else ""
    logger.info(
        "websocket_connect path=%s client=%s:%s",
        websocket.url.path,
        client_host,
        client_port,
    )
    await websocket.accept()

    config: OrchestratorConfig = websocket.app.state.config
    session = CallSession(
        websocket=websocket,
        config=config,
        stt_client=SttWsClient(config.stt_ws_url),
        llm_client=LlmHttpClient(config.llm_http_url),
        tts_client=TtsHttpClient(config.tts_http_url),
        transcript_store=websocket.app.state.transcript_store,
    )
    await session.open()

    try:
        while True:
            payload = await websocket.receive_json()
            await _handle_event(session, payload)
    except WebSocketDisconnect:
        logger.info(
            "websocket_disconnect path=%s client=%s:%s",
            websocket.url.path,
            client_host,
            client_port,
        )
    except Exception:
        logger.exception(
            "websocket_error path=%s client=%s:%s",
            websocket.url.path,
            client_host,
            client_port,
        )
        raise
    finally:
        await session.close()


async def _get_voice_params(request: Request) -> dict[str, str]:
    if request.method == "POST":
        body = await request.body()
        content_type = request.headers.get("content-type", "")
        if "application/x-www-form-urlencoded" in content_type:
            return dict(parse_qsl(body.decode("utf-8"), keep_blank_values=True))
        logger.warning("voice_webhook_unsupported_content_type content_type=%s", content_type)
        return {}
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
    stream_sid = ""

    if isinstance(payload.get("streamSid"), str):
        stream_sid = payload["streamSid"]
    elif isinstance(payload.get("start"), dict):
        stream_sid = str(payload["start"].get("streamSid", ""))
    elif isinstance(payload.get("stop"), dict):
        stream_sid = str(payload["stop"].get("streamSid", ""))

    if event == "media":
        logger.debug("clawops_event event=media stream_sid=%s", stream_sid)
    else:
        logger.info("clawops_event event=%s stream_sid=%s payload=%s", event, stream_sid, payload)

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
