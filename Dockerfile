FROM goodspace/molla-orchestrator-base:py311

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    ORCH_HOST=0.0.0.0 \
    ORCH_PORT=8010 \
    ORCH_STT_WS_URL=ws://172.31.39.65:8000/stt/ws \
    ORCH_LLM_HTTP_URL=http://internal-molla-llm-lb-563483486.ap-northeast-2.elb.amazonaws.com \
    ORCH_TTS_HTTP_URL=http://172.31.33.2:8002 \
    ORCH_PUBLIC_BASE_URL=https://orch.mollatalk.com \
    AWS_REGION=ap-northeast-2 \
    ORCH_S3_AUDIO_BUCKET=molla-call-audio-prod \
    ORCH_S3_AUDIO_PREFIX=calls \

WORKDIR /app

COPY . /app

EXPOSE 8010

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8010"]
