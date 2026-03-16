FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default: streaming backtest for BTC, 5 minutes, with DB logging
CMD ["python", "-m", "brain_agent.backtesting.simple_replay", \
     "--coin", "BTC", \
     "--duration-seconds", "300", \
     "--db-log"]
