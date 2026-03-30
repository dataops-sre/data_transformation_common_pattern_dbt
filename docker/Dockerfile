FROM python:3.12-slim

ENV UV_PROJECT_ENVIRONMENT=/usr/local

RUN pip install --no-cache-dir uv

WORKDIR /workspace

# Copy dependency files first (for layer caching)
COPY pyproject.toml /workspace/

# Install dependencies into system environment
RUN uv sync

# Copy application files
COPY docker/ducklake_ui_server.py /workspace/

# Install DuckDB extensions during build
RUN python ducklake_ui_server.py install

# Verify installation
RUN python --version && dbt --version

CMD ["bash"]
