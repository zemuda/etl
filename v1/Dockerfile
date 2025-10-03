# Alternative Dockerfile without entrypoint script
FROM python:3.11-slim AS builder

# Set working directory
WORKDIR /etl

# Install system dependencies for building
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Create virtual environment and install dependencies
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip and install requirements
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Production stage
FROM python:3.11-slim AS production

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    PIPELINE_HOME="/etl"

# Create non-root user
RUN groupadd -r pipeline && useradd -r -g pipeline pipeline

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Set working directory
WORKDIR /etl

# Copy application code
COPY excel_parquet_pipeline.py .

# Create necessary directories
RUN mkdir -p /etl/data/input /etl/data/output /etl/logs /etl/state

# Set permissions
RUN chown -R pipeline:pipeline /etl

# Switch to non-root user
USER pipeline

# Expose volume mount points
VOLUME ["/etl/data/input", "/etl/data/output", "/etl/logs", "/etl/state"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Direct Python entry point
ENTRYPOINT ["python", "-m", "excel_parquet_pipeline"]
# CMD ["--help"]