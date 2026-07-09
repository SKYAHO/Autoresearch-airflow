ARG UV_VERSION=0.7.13

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --from=uv /uv /uvx /bin/

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

ARG AUTORESEARCH_REPO=https://github.com/SKYAHO/Autoresearch.git
ARG AUTORESEARCH_REF=main

RUN git clone --depth 1 "${AUTORESEARCH_REPO}" /opt/autoresearch \
    && cd /opt/autoresearch \
    && git fetch --depth 1 origin "${AUTORESEARCH_REF}" \
    && git checkout FETCH_HEAD

WORKDIR /opt/autoresearch

RUN uv pip install --system --no-cache -r requirements.txt \
    && uv pip install --system --no-cache "datasets>=2.19" "openai>=1.0"

COPY autoresearch_airflow_jobs /opt/autoresearch_airflow_jobs

ENV PYTHONPATH=/opt:/opt/autoresearch

RUN adduser --disabled-password --gecos "" appuser
USER appuser

ENTRYPOINT ["python", "-m", "autoresearch_airflow_jobs.daily_action_log"]
