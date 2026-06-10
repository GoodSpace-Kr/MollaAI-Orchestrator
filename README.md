# Molla Orchestrator

ClawOps websocket과 `molla-stt`, `molla-llm`, `molla-tts`를 연결하는 오케스트레이션 서버입니다.

## 역할

- ClawOps `media.payload`의 base64 G.711 mu-law 8kHz mono 오디오 수신
- mu-law 디코드 후 PCM16으로 변환
- STT 입력 샘플레이트로 리샘플링 후 `molla-stt` websocket 전달
- STT `final` 이벤트 수신 시 `molla-llm` 토큰 스트림 호출
- STT `final` 이벤트 직전까지의 사용자 오디오와 확정 텍스트를 발화 단위로 메모리에 보관
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
- Backend agent control WSS: 설정 시 서버 시작과 함께 상시 outbound 연결

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

### 설정 소유 구분

`config.py` 는 코드가 가져도 되는 안전한 기본값만 가집니다. 서버 주소와 토큰처럼 환경마다 달라지는 값은 코드나 Dockerfile에 넣지 않습니다.

로컬 홈서버의 `docker-compose.yml` 은 `molla-orchestrator/.env` 에서 아래 값만 받습니다.

- `ORCH_AGENT_CONTROL_WSS_URL`
- `ORCH_AGENT_TOKEN`
- `ORCH_STT_WS_URL`
- `ORCH_LLM_HTTP_URL`
- `ORCH_TTS_HTTP_URL`

`ORCH_AGENT_TOKEN` 은 백엔드의 `AI_AGENT_TOKEN` 과 같은 값이어야 합니다.

GitHub Actions는 Docker 이미지를 Docker Hub에 빌드/푸시까지만 합니다. 운영 서버에 SSH 접속하거나 `docker run`으로 배포하지 않습니다. 로컬 홈서버 실행은 `docker-compose.yml` 과 `.env` 로 관리합니다.

- `ORCH_PUBLIC_BASE_URL`
- `ORCH_STT_WS_URL`
  - 필수. 예시: `ws://molla-stt:8000/stt/ws`
- `ORCH_LLM_HTTP_URL`
  - 필수. 예시: `http://molla-llm:8001`
- `ORCH_TTS_HTTP_URL`
  - 필수. 예시: `http://molla-tts:8002`
- `ORCH_STT_SAMPLE_RATE`
- `ORCH_TTS_SAMPLE_RATE`
- `ORCH_TTS_VOICE`
- `ORCH_TTS_LANG_CODE`
- `ORCH_AGENT_CONTROL_WSS_URL`
  - 예시: `wss://api.example.com/api/v1/agents/control`
- `ORCH_AGENT_TOKEN`
  - 백엔드와 오케스트레이터가 공유하는 agent 인증 토큰입니다. Git에 커밋하지 말고 운영 환경변수로만 설정합니다.
- `ORCH_AGENT_RECONNECT_DELAY_SECS`
  - agent control WSS 연결이 끊겼을 때 재연결까지 대기할 초 단위 시간입니다. 기본값은 `5.0`입니다.

`ORCH_AGENT_CONTROL_WSS_URL` 과 `ORCH_AGENT_TOKEN` 이 모두 설정되면 오케스트레이터는 부팅 시 백엔드 agent control WSS에 상시 연결합니다. 백엔드가 `join_call` 명령을 보내면 이 연결로 수신하고, 실제 통화 미디어는 이후 Cloudflare Realtime WebRTC 연결로 처리합니다.

agent control WSS에서 수신하는 `join_call` 은 Cloudflare Realtime 세션 정보를 포함해야 합니다. 오케스트레이터는 `join_call` 수신 시 WebRTC audio peer를 만들고 백엔드로 `agent_webrtc_offer` 를 응답합니다. 백엔드는 이 offer를 Cloudflare Realtime SFU HTTPS API에 전달하고, 반환된 answer를 `webrtc_answer` 명령으로 다시 오케스트레이터에 보내야 합니다.

## 시작 payload

ClawOps `start` 이벤트 payload에 `remainingMinutes` 정수를 포함하면 오케스트레이터가 통화 잔여시간을 분 단위로 추적합니다.

- `start.remainingMinutes`가 없으면 자동 종료 타이머를 걸지 않습니다
- `start.remainingMinutes`가 `0`이면 시작 직후 종료 안내 음성을 재생하고 통화를 닫습니다
- 잔여시간이 만료되면 새 사용자 오디오는 더 이상 STT로 전달하지 않습니다
- 만료 시 `"오늘 잔여시간이 모두 소진되었습니다. 통화가 자동으로 종료됩니다. 감사합니다."` 안내 음성을 송출한 뒤 websocket을 종료합니다

## 세션 종료 업로드

세션 종료 시 backend end API payload에는 `startedAt`, `endedAt`, `durationMinutes`, `turns`가 포함됩니다.

- `startedAt`과 `endedAt`은 오케스트레이터가 추적한 통화 세션 시작/종료 시각입니다
- `durationMinutes`는 `endedAt - startedAt` 기준의 총 통화 시간을 분 단위로 내림 계산한 값입니다
- 각 turn은 하나의 사용자 발화와 그에 대한 assistant 응답을 함께 담습니다
- `turn.user.text`와 `turn.user.audio`는 같은 conversation turn에서 함께 저장됩니다
- `turn.user`는 `text`, `audio`, `sampleRate`, `encoding` 네 필드를 포함합니다
- `turn.user.audio`는 해당 발화 직전까지 누적한 16kHz `pcm16le/base64` 오디오입니다
- `turn.assistant`는 응답이 존재할 때 `text`와 `createdAt` 필드를 포함합니다
