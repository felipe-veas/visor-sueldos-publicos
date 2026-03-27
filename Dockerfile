# Build Stage
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install dependencies from uv lock
COPY uv.lock pyproject.toml ./
RUN uv sync --frozen --no-install-project --no-dev

# Final Stage
FROM python:3.12-slim-bookworm

WORKDIR /app

# Copy the environment
COPY --from=builder /app/.venv /app/.venv

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Copy application source
COPY . /app/

# Create a non-root user and change ownership
RUN useradd -m appuser && chown -R appuser:appuser /app

USER appuser

EXPOSE 8501

HEALTHCHECK CMD python -c 'import urllib.request; urllib.request.urlopen("http://localhost:8501/_stcore/health")' || exit 1

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
