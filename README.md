# Molla Orchestrator

ClawOps websocket과 `molla-stt`, `molla-llm`, `molla-tts`를 연결하는 오케스트레이션 서버입니다.

## 역할

- ClawOps `media.payload`의 base64 G.711 mu-law 8kHz mono 오디오 수신
- mu-law 디코드 후 PCM16으로 변환
- STT 입력 샘플레이트로 리샘플링 후 `molla-stt` websocket 전달
- STT `final` 이벤트 수신 시 `molla-llm` 토큰 스트림 호출
- 생성 텍스트를 문장 단위로 잘라 `molla-tts`에 앞당겨 요청
- 반환된 WAV PCM을 8kHz mono mu-law로 변환 후 ClawOps websocket으로 전송

## 엔드포인트

- WebSocket: `/orchestrator/ws`
- Health: `/healthz`

## 기본 연결 대상

- STT: `ws://127.0.0.1:8000/stt/ws`
- LLM: `http://127.0.0.1:8001`
- TTS: `http://127.0.0.1:8002`

환경 변수로 변경할 수 있습니다.

## 실행

```bash
uvicorn main:app --host 0.0.0.0 --port 8010
```

## 주요 환경 변수

- `ORCH_STT_WS_URL`
- `ORCH_LLM_HTTP_URL`
- `ORCH_TTS_HTTP_URL`
- `ORCH_STT_SAMPLE_RATE`
- `ORCH_TTS_SAMPLE_RATE`
- `ORCH_TTS_VOICE`
- `ORCH_TTS_LANG_CODE`
