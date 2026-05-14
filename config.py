from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class OrchestratorConfig:
    host: str = "0.0.0.0"
    port: int = 8010
    public_base_url: str | None = None
    voice_webhook_path: str = "/voice"
    stream_websocket_path: str = "/stream"
    transcript_dir: str = "./transcripts"
    backend_session_start_url: str | None = None
    backend_session_end_url_template: str | None = None
    stt_ws_url: str = "ws://3.36.166.43:8000/stt/ws"
    llm_http_url: str = "http://internal-molla-llm-lb-563483486.ap-northeast-2.elb.amazonaws.com"
    tts_http_url: str = "http://3.36.184.67:8002"
    stt_sample_rate: int = 16000
    tts_sample_rate: int = 24000
    tts_voice: str = "af_heart"
    tts_lang_code: str = "a"
    outbound_frame_bytes: int = 160
    outbound_frame_ms: float = 0.02
    llm_sentence_soft_limit: int = 48
    llm_sentence_hard_limit: int = 96

    @classmethod
    def from_env(cls) -> "OrchestratorConfig":
        return cls(
            host=os.getenv("ORCH_HOST", "0.0.0.0"),
            port=int(os.getenv("ORCH_PORT", "8010")),
            public_base_url=os.getenv("ORCH_PUBLIC_BASE_URL"),
            voice_webhook_path=os.getenv("ORCH_VOICE_WEBHOOK_PATH", "/voice"),
            stream_websocket_path=os.getenv("ORCH_STREAM_WEBSOCKET_PATH", "/stream"),
            transcript_dir=os.getenv("ORCH_TRANSCRIPT_DIR", "./transcripts"),
            backend_session_start_url=os.getenv(
                "ORCH_BACKEND_SESSION_START_URL",
                "http://43.202.22.150:8080/api/v1/internal/sessions/start",
            ),
            backend_session_end_url_template=os.getenv(
                "ORCH_BACKEND_SESSION_END_URL_TEMPLATE",
                "http://43.202.22.150:8080/api/v1/internal/sessions/{session_id}/end",
            ),
            stt_ws_url=os.getenv("ORCH_STT_WS_URL", "ws://172.31.33.2:8000/stt/ws"),
            llm_http_url=os.getenv("ORCH_LLM_HTTP_URL", "http://internal-molla-llm-lb-563483486.ap-northeast-2.elb.amazonaws.com"),
            tts_http_url=os.getenv("ORCH_TTS_HTTP_URL", "http://172.31.33.2:8002"),
            stt_sample_rate=int(os.getenv("ORCH_STT_SAMPLE_RATE", "16000")),
            tts_sample_rate=int(os.getenv("ORCH_TTS_SAMPLE_RATE", "24000")),
            tts_voice=os.getenv("ORCH_TTS_VOICE", "af_heart"),
            tts_lang_code=os.getenv("ORCH_TTS_LANG_CODE", "a"),
            outbound_frame_bytes=int(os.getenv("ORCH_OUTBOUND_FRAME_BYTES", "160")),
            outbound_frame_ms=float(os.getenv("ORCH_OUTBOUND_FRAME_MS", "0.02")),
            llm_sentence_soft_limit=int(os.getenv("ORCH_LLM_SENTENCE_SOFT_LIMIT", "48")),
            llm_sentence_hard_limit=int(os.getenv("ORCH_LLM_SENTENCE_HARD_LIMIT", "96")),
        )
