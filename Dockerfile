FROM python:3.12-slim

ENV UV_PROJECT_ENVIRONMENT=/usr/local

RUN pip install --no-cache-dir uv

WORKDIR /workspace

# Copy dependency files first (for layer caching)
COPY pyproject.toml /workspace/

# Install dependencies into system environment
RUN uv sync

# Verify installation
RUN python --version && dbt --version

CMD ["bash"]
