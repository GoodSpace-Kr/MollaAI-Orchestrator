FROM goodspace/molla-orchestrator-base:py311

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8010

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8010"]
