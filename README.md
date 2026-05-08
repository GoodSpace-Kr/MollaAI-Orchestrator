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

- Voice webhook: `/voice`
- ClawOps stream WebSocket: `/stream`
- WebSocket: `/orchestrator/ws`
- Health: `/healthz`

`/voice`는 ClawOps 인바운드 Voice webhook용 HTTP 엔드포인트입니다. 이 경로는 VoiceML을 반환하며, 내부적으로 `/stream` WebSocket을 가리킵니다.

## 기본 연결 대상

- STT: `ws://127.0.0.1:8000/stt/ws`
- LLM: `http://127.0.0.1:8001`
- TTS: `http://127.0.0.1:8002`

환경 변수로 변경할 수 있습니다.

## 실행

```bash
uvicorn main:app --host 0.0.0.0 --port 8010
```

ClawOps를 외부에서 연결할 때는 다음 환경 변수를 함께 설정하는 편이 안전합니다.

```bash
export ORCH_PUBLIC_BASE_URL="https://orchestrator.example.com"
```

그러면 `/voice` 응답의 VoiceML이 아래처럼 외부용 WebSocket 주소를 생성합니다.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://orchestrator.example.com/stream" track="inbound">
      <Parameter name="callId" value="CA..."/>
      <Parameter name="from" value="010..."/>
      <Parameter name="to" value="070..."/>
    </Stream>
  </Connect>
</Response>
```

## 주요 환경 변수

- `ORCH_PUBLIC_BASE_URL`
- `ORCH_STT_WS_URL`
  - 예시: `ws://3.36.166.43:8000/stt/ws`
- `ORCH_LLM_HTTP_URL`
- `ORCH_TTS_HTTP_URL`
- `ORCH_STT_SAMPLE_RATE`
- `ORCH_TTS_SAMPLE_RATE`
- `ORCH_TTS_VOICE`
- `ORCH_TTS_LANG_CODE`
