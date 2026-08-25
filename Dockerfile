FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY loom_v2 ./loom_v2
COPY migrations ./migrations
RUN pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

CMD ["uvicorn", "loom_v2.observer.app:app", "--host", "0.0.0.0", "--port", "8080"]
