FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Make startup script executable
RUN chmod +x /app/startup.sh

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Use startup.sh to bootstrap the agent with proper validation
ENTRYPOINT ["/app/startup.sh"]
