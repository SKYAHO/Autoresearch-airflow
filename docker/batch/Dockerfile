ARG UV_VERSION=0.7.13

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:3.12-slim AS source

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

ARG AUTORESEARCH_REPO=https://github.com/SKYAHO/Autoresearch.git
ARG AUTORESEARCH_REF=main

RUN git clone --depth 1 "${AUTORESEARCH_REPO}" /opt/autoresearch \
    && cd /opt/autoresearch \
    && git fetch --depth 1 origin "${AUTORESEARCH_REF}" \
    && git checkout FETCH_HEAD \
    && rm -rf .git


FROM python:3.12-slim AS deps

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --from=uv /uv /uvx /bin/
COPY --from=source /opt/autoresearch /opt/autoresearch

WORKDIR /opt/autoresearch

RUN uv venv /opt/venv \
    && uv pip install --python /opt/venv/bin/python --no-cache -r requirements.txt \
    && uv pip install --python /opt/venv/bin/python --no-cache "datasets>=2.19" "openai>=1.0"


FROM python:3.12-slim AS runtime

ARG AUTORESEARCH_REF=main

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH=/opt/venv/bin:$PATH
ENV PYTHONPATH=/opt:/opt/autoresearch

LABEL org.opencontainers.image.source="https://github.com/SKYAHO/Autoresearch" \
      org.opencontainers.image.revision="${AUTORESEARCH_REF}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=deps /opt/venv /opt/venv
COPY --from=source /opt/autoresearch /opt/autoresearch
COPY autoresearch_airflow_jobs /opt/autoresearch_airflow_jobs

WORKDIR /opt/autoresearch

RUN adduser --disabled-password --gecos "" appuser
USER appuser

ENTRYPOINT ["python", "-m", "autoresearch.action_logs.cli"]
