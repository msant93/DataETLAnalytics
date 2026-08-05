# Multi-stage build: small, reproducible, non-root runtime image.
FROM python:3.12-slim AS base

# System hardening + no pyc/buffered output.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install deps first (layer caching): only re-runs when requirements change.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source.
COPY . .

# Run as an unprivileged user (never root in production containers).
RUN useradd --create-home --uid 10001 appuser \
    && mkdir -p /app/output \
    && chown -R appuser:appuser /app
USER appuser

# Default profile is prod inside the container; override at `docker run` time.
ENV APP_ENV=prod

# The pipeline is the entrypoint; `run` is the default command.
ENTRYPOINT ["python", "-m", "etl.cli"]
CMD ["run"]
