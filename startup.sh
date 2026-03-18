#!/bin/bash
# startup.sh — Brain Agent container bootstrap script
# Validates dependencies, waits for services, then starts the trading agent

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         Brain Agent - ML Trading System Startup             ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# 1. ENVIRONMENT VALIDATION
# ══════════════════════════════════════════════════════════════════════════════

echo -e "${YELLOW}[SETUP]${NC} Validating environment variables..."
echo ""

MISSING_VARS=()
REQUIRED_VARS=(
    "REDIS_HOST"
    "REDIS_PORT"
    "REDIS_PASSWORD"
    "POSTGRES_HOST"
    "POSTGRES_PORT"
    "POSTGRES_USER"
    "POSTGRES_PASSWORD"
    "POSTGRES_DB"
)

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var:-}" ]; then
        MISSING_VARS+=("$var")
    else
        echo -e "  ${GREEN}✓${NC} $var = ${!var}"
    fi
done

if [ ${#MISSING_VARS[@]} -gt 0 ]; then
    echo ""
    echo -e "${RED}✗ ERROR: Missing required environment variables:${NC}"
    for var in "${MISSING_VARS[@]}"; do
        echo -e "  ${RED}  - $var${NC}"
    done
    echo ""
    echo "Make sure .env file is loaded: docker-compose uses env_file: .env"
    exit 1
fi

echo ""
echo -e "${GREEN}All required variables present${NC}"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# 2. REDIS CONNECTIVITY CHECK
# ══════════════════════════════════════════════════════════════════════════════

echo -e "${YELLOW}[REDIS]${NC} Waiting for Redis at ${REDIS_HOST}:${REDIS_PORT}..."

MAX_RETRIES=30
RETRY=0

while [ $RETRY -lt $MAX_RETRIES ]; do
    RETRY=$((RETRY + 1))

    # Use Python to test Redis connection with proper auth
    if python3 << PYTHON_REDIS 2>/dev/null
import redis
import sys

try:
    r = redis.Redis(
        host='${REDIS_HOST}',
        port=${REDIS_PORT},
        password='${REDIS_PASSWORD}',
        db=0,
        socket_connect_timeout=2,
        socket_keepalive=True
    )
    r.ping()
    print("OK")
except Exception as e:
    sys.exit(1)
PYTHON_REDIS
    then
        echo -e "  ${GREEN}✓${NC} Redis is reachable and authenticated"
        break
    fi

    if [ $RETRY -lt $MAX_RETRIES ]; then
        WAIT=$((RETRY * 2))
        if [ $WAIT -gt 10 ]; then WAIT=10; fi
        echo -e "  Attempt $RETRY/$MAX_RETRIES — Redis not ready, retrying in ${WAIT}s..."
        sleep $WAIT
    fi
done

if [ $RETRY -eq $MAX_RETRIES ]; then
    echo -e "${RED}✗ Failed to connect to Redis after $MAX_RETRIES attempts${NC}"
    echo "Check:"
    echo "  1) Redis is running: docker-compose ps"
    echo "  2) REDIS_HOST and REDIS_PORT are correct"
    echo "  3) REDIS_PASSWORD matches docker-compose.yml"
    exit 1
fi

echo ""

# ══════════════════════════════════════════════════════════════════════════════
# 3. POSTGRES CONNECTIVITY CHECK
# ══════════════════════════════════════════════════════════════════════════════

echo -e "${YELLOW}[POSTGRES]${NC} Waiting for PostgreSQL at ${POSTGRES_HOST}:${POSTGRES_PORT}..."

RETRY=0

while [ $RETRY -lt $MAX_RETRIES ]; do
    RETRY=$((RETRY + 1))

    # Use Python to test Postgres connection with asyncpg
    if python3 << PYTHON_PG 2>/dev/null
import asyncio
import asyncpg
import sys

async def check_db():
    try:
        conn = await asyncpg.connect(
            host='${POSTGRES_HOST}',
            port=${POSTGRES_PORT},
            user='${POSTGRES_USER}',
            password='${POSTGRES_PASSWORD}',
            database='${POSTGRES_DB}',
            timeout=5.0
        )
        await conn.close()
        print("OK")
    except Exception as e:
        sys.exit(1)

asyncio.run(check_db())
PYTHON_PG
    then
        echo -e "  ${GREEN}✓${NC} PostgreSQL is reachable and authenticated"
        break
    fi

    if [ $RETRY -lt $MAX_RETRIES ]; then
        WAIT=$((RETRY * 2))
        if [ $WAIT -gt 10 ]; then WAIT=10; fi
        echo -e "  Attempt $RETRY/$MAX_RETRIES — PostgreSQL not ready, retrying in ${WAIT}s..."
        sleep $WAIT
    fi
done

if [ $RETRY -eq $MAX_RETRIES ]; then
    echo -e "${RED}✗ Failed to connect to PostgreSQL after $MAX_RETRIES attempts${NC}"
    echo "Check:"
    echo "  1) PostgreSQL is running: docker-compose ps"
    echo "  2) POSTGRES_HOST and POSTGRES_PORT are correct"
    echo "  3) POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB match docker-compose.yml"
    exit 1
fi

echo ""

# ══════════════════════════════════════════════════════════════════════════════
# 4. HYPERLIQUID CONFIGURATION CHECK
# ══════════════════════════════════════════════════════════════════════════════

echo -e "${YELLOW}[HYPERLIQUID]${NC} Checking trading configuration..."

if [ -z "${HYPERLIQUID_ADDRESS:-}" ] && [ -z "${HYPERLIQUID_TESTNET_ADDRESS:-}" ]; then
    echo -e "  ${YELLOW}⚠${NC}  No Hyperliquid credentials configured"
    echo "     The system will run in data-only mode (no trading execution)"
    echo "     To enable trading, set HYPERLIQUID_ADDRESS and HYPERLIQUID_PRIVATE_KEY"
else
    if [ -n "${HYPERLIQUID_ADDRESS:-}" ]; then
        echo -e "  ${GREEN}✓${NC} Mainnet credentials configured"
    fi
    if [ -n "${HYPERLIQUID_TESTNET_ADDRESS:-}" ]; then
        echo -e "  ${GREEN}✓${NC} Testnet credentials configured"
    fi
fi

echo ""

# ══════════════════════════════════════════════════════════════════════════════
# 5. EXECUTION MODE SELECTION
# ══════════════════════════════════════════════════════════════════════════════

# Determine execution mode from environment or default
EXECUTION_MODE="${EXECUTION_MODE:-backtest}"
COIN="${COIN:-BTC}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"

# For Docker, we run streaming backtest (duration=0 = infinite until SIGTERM)
# Use environment variables to override:
#   DURATION_SECONDS=0   (0 = infinite streaming)
#   DURATION_SECONDS=300 (5 min burst)
DURATION_SECONDS="${DURATION_SECONDS:-0}"

# Allow explicit script selection via ENTRYPOINT_MODE
# Values: "simple_replay" (default) or "main"
ENTRYPOINT_MODE="${ENTRYPOINT_MODE:-simple_replay}"

echo -e "${YELLOW}[CONFIG]${NC} Execution parameters:"
echo "  Entrypoint: $ENTRYPOINT_MODE"
echo "  Coin: $COIN"
echo "  Mode: $EXECUTION_MODE"
echo "  Duration: $([ "$DURATION_SECONDS" = "0" ] && echo "infinite (streaming)" || echo "${DURATION_SECONDS}s")"
echo "  Log Level: $LOG_LEVEL"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# 6. STARTUP BANNER
# ══════════════════════════════════════════════════════════════════════════════

echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}Starting Brain Agent Trading System${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S UTC')"
echo "Python: $(python3 --version)"
echo "PID: $$"
echo ""

# ══════════════════════════════════════════════════════════════════════════════
# 7. EXECUTE ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════

# Use 'exec' to replace this shell process with the Python process.
# This ensures SIGTERM (docker stop) reaches Python directly.

if [ "$ENTRYPOINT_MODE" = "main" ]; then
    # Main agent mode (paper/live trading)
    exec python -m brain_agent.main \
        --mode "$EXECUTION_MODE" \
        --symbol "$COIN" \
        --log-level "$LOG_LEVEL"
else
    # Streaming backtest mode (default)
    # Always runs with --db-log to persist to PostgreSQL
    exec python -m brain_agent.backtesting.simple_replay \
        --coin "$COIN" \
        --duration-seconds "$DURATION_SECONDS" \
        --log-level "$LOG_LEVEL" \
        --db-log
fi
