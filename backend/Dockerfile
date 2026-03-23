FROM python:3.11-alpine
LABEL org.opencontainers.image.source=https://github.com/nkz-os/catastro-sp-module-nekazari

WORKDIR /app

# Install system dependencies
RUN apk add --no-cache \
    postgresql-client \
    gcc \
    musl-dev \
    postgresql-dev

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ /app/app/

# Create non-root user
RUN adduser -D -u 1000 cadastral && chown -R cadastral:cadastral /app
USER cadastral

EXPOSE 5000

CMD ["python", "-m", "app.cadastral_api"]

