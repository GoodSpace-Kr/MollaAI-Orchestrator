FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    ORCH_HOST=0.0.0.0 \
    ORCH_PORT=8010 \
    ORCH_STT_WS_URL=ws://molla-stt:8000/stt/ws \
    ORCH_LLM_HTTP_URL=http://molla-llm:8001 \
    ORCH_TTS_HTTP_URL=http://molla-tts:8002

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --upgrade pip setuptools wheel && \
    pip install -r /app/requirements.txt

COPY . /app

EXPOSE 8010

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8010"]
