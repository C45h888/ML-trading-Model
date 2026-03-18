"""
PostgreSQL logging for Brain Agent.
Three tables: trade_ticks, engine_features, signals.
Signals track outcome (WIN/LOSS) after a forward evaluation window.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


class Database:
    """Async PostgreSQL client. All write errors are caught and logged — never crash the trading loop."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=5)
        await self._create_tables()
        logger.info("PostgreSQL connected and tables ready")

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL disconnected")

    async def _create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_ticks (
                    id          BIGSERIAL PRIMARY KEY,
                    ts          TIMESTAMPTZ NOT NULL,
                    session_id  TEXT NOT NULL,
                    coin        TEXT NOT NULL,
                    price       DOUBLE PRECISION NOT NULL,
                    size        DOUBLE PRECISION NOT NULL,
                    side        TEXT NOT NULL,
                    hash        TEXT
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tt_ts_coin ON trade_ticks (ts, coin)"
            )

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS engine_features (
                    id                  BIGSERIAL PRIMARY KEY,
                    ts                  TIMESTAMPTZ NOT NULL,
                    session_id          TEXT NOT NULL,
                    coin                TEXT NOT NULL,
                    cvd                 DOUBLE PRECISION,
                    absorption_score    DOUBLE PRECISION,
                    bid_ask_imbalance   DOUBLE PRECISION,
                    volume_imbalance    DOUBLE PRECISION,
                    delta_divergence    DOUBLE PRECISION,
                    total_buy_volume    DOUBLE PRECISION,
                    total_sell_volume   DOUBLE PRECISION,
                    trade_count         INTEGER
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ef_ts_coin ON engine_features (ts, coin)"
            )

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id          BIGSERIAL PRIMARY KEY,
                    ts          TIMESTAMPTZ NOT NULL,
                    session_id  TEXT NOT NULL,
                    coin        TEXT NOT NULL,
                    direction   TEXT NOT NULL,
                    confidence  DOUBLE PRECISION,
                    reason      TEXT,
                    entry_price DOUBLE PRECISION,
                    outcome     TEXT DEFAULT 'PENDING',
                    exit_price  DOUBLE PRECISION,
                    pnl_pct     DOUBLE PRECISION
                )
            """)

    # ------------------------------------------------------------------
    # Write helpers — each wraps its own try/except so errors are silent
    # ------------------------------------------------------------------

    async def log_trade_tick(self, tick: dict, session_id: str):
        if not self.pool:
            return
        try:
            ts = tick.get("timestamp")
            if isinstance(ts, (int, float)):
                # Hyperliquid returns timestamps in nanoseconds (19 digits)
                # Convert nanoseconds to seconds if needed
                if ts > 1_000_000_000_000_000_000:  # > 1 quintillion = nanoseconds
                    ts = ts / 1_000_000_000  # Convert nanoseconds to seconds
                elif ts > 1_000_000_000_000:  # > 1 trillion = milliseconds
                    ts = ts / 1_000  # Convert milliseconds to seconds
                ts = datetime.fromtimestamp(ts, tz=timezone.utc)
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO trade_ticks (ts, session_id, coin, price, size, side, hash)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    ts, session_id, tick["coin"],
                    float(tick["price"]), float(tick["size"]),
                    tick["side"], tick.get("hash"),
                )
        except Exception as e:
            logger.error(f"DB log_trade_tick error: {e}")

    async def log_engine_features(self, snapshot: dict, session_id: str, coin: str, ts: datetime):
        if not self.pool:
            return
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO engine_features
                        (ts, session_id, coin, cvd, absorption_score, bid_ask_imbalance,
                         volume_imbalance, delta_divergence, total_buy_volume, total_sell_volume, trade_count)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                    ts, session_id, coin,
                    float(snapshot.get("cvd", 0)),
                    float(snapshot.get("absorption_score", 0)),
                    float(snapshot.get("bid_ask_imbalance", 0)),
                    float(snapshot.get("volume_imbalance", 0)),
                    float(snapshot.get("delta_divergence", 0)),
                    float(snapshot.get("total_buy_volume", 0)),
                    float(snapshot.get("total_sell_volume", 0)),
                    int(snapshot.get("trade_count", 0)),
                )
        except Exception as e:
            logger.error(f"DB log_engine_features error: {e}")

    async def log_signal(
        self,
        direction: str,
        confidence: float,
        reason: str,
        entry_price: float,
        session_id: str,
        coin: str,
        ts: datetime,
    ) -> Optional[int]:
        """Insert a signal row and return its id (used later to record outcome)."""
        if not self.pool:
            return None
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO signals (ts, session_id, coin, direction, confidence, reason, entry_price)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id
                    """,
                    ts, session_id, coin, direction,
                    float(confidence), reason, float(entry_price),
                )
                return row["id"] if row else None
        except Exception as e:
            logger.error(f"DB log_signal error: {e}")
            return None

    async def update_signal_outcome(
        self, signal_id: int, exit_price: float, entry_price: float, direction: str
    ) -> tuple[str, float]:
        """
        Evaluate signal outcome and persist it.
        Returns (outcome, pnl_pct).
        """
        if direction == "LONG":
            pnl_pct = (exit_price - entry_price) / entry_price * 100
        else:
            pnl_pct = (entry_price - exit_price) / entry_price * 100

        outcome = "WIN" if pnl_pct > 0 else "LOSS"

        if self.pool and signal_id is not None:
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE signals SET outcome=$1, exit_price=$2, pnl_pct=$3 WHERE id=$4",
                        outcome, float(exit_price), float(pnl_pct), signal_id,
                    )
            except Exception as e:
                logger.error(f"DB update_signal_outcome error: {e}")

        return outcome, pnl_pct
