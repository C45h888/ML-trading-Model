# Brain Agent Development Plan - Phase 2

> **Status**: Planning Phase  
> **Version**: 2.0 (Optimized) 
> **Last Updated**: 2026-03-14

---

## Overview

This plan addresses issues found during Phase 1 testing and provides a **production-ready** roadmap with:
- Reconnection logic (tenacity)
- Complete L2 handling
- Error handling
- Real historical replay
- Redis validation

---

## Issues Identified from Phase 1

1. **Settings Loading** - `.env` file has parsing issues, need better error handling
2. **Hyperliquid API** - `info.all_trades` method doesn't exist, need to use correct API
3. **OrderflowEngine** - Returns Pydantic model but strategies expect dict
4. **Type Mismatch** - Features need to be dict-compatible for strategies
5. **No reconnection logic** - Need tenacity with exponential backoff
6. **Incomplete L2 handling** - Need full bid/ask imbalance calculation
7. **Missing error handling** - Need try/except around all API calls

---

## Core Principles

1. **Type Safety**: Everything must be type-checked with Pydantic v2
2. **Modularity**: Single responsibility for each component
3. **Resilience**: Automatic reconnection, proper error handling
4. **Observability**: Structured logging, health checks, metrics
5. **Extensibility**: Easy to add new strategies and data sources

---

# Execution Order (Sequential - Must Complete in Order)

---

## Step 1: Replace `brain_agent/config/settings.py`

**Why**: Single source of truth, validation, env + yaml merging, type safety

### Tasks:

- Use `pydantic_settings.BaseSettings` (supports .env + env vars)
- Fields with defaults & validation:

```python
data_source: Literal["hyperliquid", "sierra_dtc"] = "hyperliquid"
hyperliquid_ws_url: str = "wss://api.hyperliquid.xyz/ws"
hyperliquid_testnet: bool = False
redis_url: str = "redis://localhost:6379"  # Required - no default!
symbols: List[str] = Field(default_factory=lambda: ["BTC", "ETH", "SOL"])
mode: Literal["backtest", "paper", "live"] = "backtest"
log_level: str = "INFO"
feature_window_ticks: int = 500
absorption_threshold: float = 3.0
```

- Add `@model_validator` to check:
  - `data_source` in allowed list
  - `mode` in allowed list
  - Testnet consistency with redis_url

- Export: `settings = Settings(_env_file=".env", _env_file_encoding="utf-8")`
- Add `__repr__` for nice printing

---

## Step 2: Create `brain_agent/core/models.py`

**Why**: Prevents dict chaos, enables type checking, makes engine & strategies predictable

### Models to define:

```python
# TradeTick - normalized trade (Hyperliquid returns prices as strings with 6 decimals)
TradeTick:
  - timestamp: int
  - coin: str
  - price: float  # Divide Hyperliquid px by 1e6
  - size: float
  - side: Literal["B", "A"]  # Bid/Ask
  - hash: Optional[str]

# L2Update - orderbook (top N levels)
L2Update:
  - timestamp: int
  - coin: str
  - bids: List[Dict[str, str | int]]  # price, size as strings from Hyperliquid
  - asks: List[Dict[str, str | int]]
  - top_n: int = 5
  # Add computed properties: best_bid, best_ask, spread, mid_price

# Event - union type
Event:
  - event_type: Literal["trade", "l2_update"]
  - data: Union[TradeTick, L2Update]

# OrderflowFeatures - calculated metrics
OrderflowFeatures:
  - cvd: float
  - absorption_score: float
  - bid_ask_imbalance: float
  - volume_imbalance: float
  - delta_divergence: float
  - volume_profile_poc: float
  - trade_count: int
  - total_buy_volume: float
  - total_sell_volume: float
  - high_price: float
  - low_price: float
  - close_price: float
  # Add to_dict() method for strategy compatibility
```

---

## Step 3: Refactor `brain_agent/core/data_pipeline.py` (Full Production Version)

**Why**: Need reconnection, L2 handling, error handling, Redis publishing

### Must-have improvements:

1. **Use settings for all config**
2. **Add tenacity for WS reconnection**:
   ```python
   @retry(
       stop=stop_after_attempt(10),
       wait=wait_exponential(multiplier=1, min=1, max=30),
       retry=retry_if_exception_type(Exception)
   )
   async def _connect_with_retry(self):
       # WS connection logic
   ```
3. **Support BOTH trades AND l2Book subscriptions**
4. **Normalize Hyperliquid raw data** into TradeTick and L2Update:
   - Hyperliquid returns prices as strings with 6 decimal places
   - Must divide by 1e6
5. **Async generator** `stream_events(coin: str)` yields Event
6. **Publish to Redis** channel "market:data" (JSON serialized)
7. **Graceful shutdown** (stop() method)
8. **Context manager support** (__aenter__ / __aexit__)
9. **Add logging** (use logging module)

### Architecture:

```
DataPipeline
├── HyperliquidSource
│   ├── Info (REST)
│   ├── WebSocket subscriptions
│   └── Normalization (TradeTick, L2Update)
├── SierraDTCSource (stub - commented)
└── Redis publisher
```

---

## Step 4: Build Full `brain_agent/core/orderflow_engine.py`

**Why**: Process both event types, complete feature calculation, async safety

### Must-have features:

1. **Init with window from settings** + absorption_threshold
2. **process_event(event: Event)** → updates internal state
3. **Separate handlers**:
   - `_process_trade(tick: TradeTick)` → update CVD, buy/sell totals, price range
   - `_process_l2(l2: L2Update)` → calculate bid/ask imbalance, absorption at top levels

4. **get_features()** → OrderflowFeatures (returns validated model)
5. **Feature calculations**:
   - **Absorption score**: 
     ```
     absorption_score = (max_level_size / avg_level_size) / (price_range + ε)
     ```
   - **Bid/ask imbalance**:
     ```
     bid_ask_imbalance = sum(bid_sz) / sum(ask_sz) over top N levels
     ```
   - **Delta divergence**: simple z-score or ratio over window
   - **Volume profile POC**: mode of price in window (weighted by size later)

6. **reset()** method for new sessions
7. **Thread/async safe** (use asyncio.Lock if needed)
8. **Regime detection stub**: trending vs ranging based on imbalance volatility

---

## Step 5: Create `brain_agent/backtesting/simple_replay.py`

**Why**: Real historical replay, not just synthetic data

### Requirements:

- Command-line: `python -m brain_agent.backtesting.simple_replay --coin BTC --limit 2000`
- Use `info.recent_trades(coin)` for trade history (fallback to synthetic if empty)
- **Simulate L2 updates**: Random small perturbations around last price for now
- **Replay loop**: feed events → engine → strategy → print signals & summary stats
- **Output format**:
  ```
  [1234] Absorption LONG | conf 0.82 | score 4.91 | CVD +187.3
  [1567] Imbalance SHORT | conf 0.67 | ratio 0.32
  ...
  SUMMARY: Events=2000 | Signals=28 | Final CVD=+412.6 | Max drawdown=-1.8%
  ```

### Implementation:

```python
async def run_backtest(coin: str, limit: int, print_every: int = 100):
    pipeline = DataPipeline()
    engine = OrderflowEngine()
    strategy = AbsorptionStrategy()
    
    await pipeline.connect()
    
    # Try real data first, fallback to synthetic
    trades = await pipeline.get_recent_trades(coin, limit)
    if not trades:
        trades = _generate_synthetic(coin, limit)
    
    signals = 0
    for i, trade in enumerate(trades):
        event = Event(event_type=EventType.TRADE, data=trade)
        features = engine.process_event(event)
        signal = strategy.generate_signal(features.to_dict(), trade.price)
        
        if signal.signal.value != "FLAT":
            signals += 1
            print(f"[{i}] {signal.signal.value} | conf {signal.confidence:.2f} | {signal.reason}")
        
        if (i + 1) % print_every == 0:
            print(f"[{i+1}] CVD: {features.cvd:.2f} | Abs: {features.absorption_score:.2f}")
```

---

## Step 6: Update `brain_agent/main.py`

**Why**: Proper integration, logging, argument handling

### Tasks:

- Parse args: `--mode`, `--coin`, `--limit`, `--log-level`
- Load settings
- If mode == "backtest": run the harness
- If mode == "live" or "paper": run live stream loop → print features/signals
- Add basic logging setup from `settings.log_level`
- Add Ctrl+C graceful shutdown

### Command-line usage:

```bash
# Backtest
python -m brain_agent.main --mode backtest --coin BTC --limit 1000

# Paper trading
python -m brain_agent.main --mode paper --coin BTC

# Live trading
python -m brain_agent.main --mode live --coin BTC --log-level DEBUG
```

---

# Final Acceptance Criteria

Before moving to strategies, verify:

| # | Criterion | Test Command |
|---|-----------|--------------|
| 1 | No crashes for 60+ seconds | `python -m brain_agent.main --mode live --coin BTC` |
| 2 | Shows live trades + L2 updates + features | Check output for absorption > 3.0 |
| 3 | Auto-reconnect on WS drop | Kill network briefly, observe reconnect |
| 4 | Backtest mode works | `--mode backtest --limit 1000` prints signals |
| 5 | Redis receives valid JSON | `redis-cli monitor` shows "market:data" |

---

# Agent Instructions (Copy-Paste Block)

```
Follow this exact sequence. Commit after each numbered task with clear message.

1. Create/replace config/settings.py with pydantic_settings BaseSettings (fields as listed, validators, singleton export)

2. Create core/models.py with TradeTick, L2Update, Event, OrderflowFeatures (Pydantic models)

3. Refactor core/data_pipeline.py:
   - Use settings
   - Add tenacity retry on WS (10x with exp backoff)
   - Subscribe to both trades and l2Book
   - Normalize to TradeTick / L2Update (divide Hyperliquid px by 1e6)
   - stream_events() generator
   - Publish to Redis "market:data"

4. Build full core/orderflow_engine.py:
   - Process both event types
   - Calculate all OrderflowFeatures fields
   - Use rolling deque + pandas/numpy
   - absorption_score, bid_ask_imbalance, delta_divergence, POC
   - Add reset() method for new sessions

5. Create backtesting/simple_replay.py (backtest loop with recent_trades + synthetic L2 fallback)

6. Update main.py to support --mode, --coin, --limit and run appropriate flow

After step 6: run live mode for 60 seconds and paste 10–15 lines of output (features + any signals).
Once complete: paste the output here (or say "cycle complete — features look good") and we'll move straight to strategy refactoring.
```

---

# File Structure After Implementation

```
brain_agent/
├── __init__.py
├── main.py                           # Updated entry point
├── config/
│   ├── __init__.py
│   ├── settings.py                   # REPLACED - pydantic BaseSettings
│   └── data_sources.yaml
├── core/
│   ├── __init__.py
│   ├── models.py                     # REPLACED - complete Pydantic models
│   ├── data_pipeline.py              # REPLACED - full production version
│   └── orderflow_engine.py           # REPLACED - full implementation
├── strategies/
│   ├── __init__.py
│   ├── base_strategy.py
│   └── absorption.py
└── backtesting/
    ├── __init__.py
    └── simple_replay.py              # NEW - backtest harness
```

---

# Key Differences from Original Plan

| Original Issue | Fix in Optimized Plan |
|----------------|----------------------|
| No reconnection logic | Added tenacity with 10x retry + exp backoff |
| Incomplete L2 handling | Full _process_l2 with imbalance calculation |
| Synthetic data fallback | Try real data first, fallback only if empty |
| Missing error handling | Try/except around all API calls |
| No real historical replay | Uses info.all_trades or proper WS subscription |
| No Redis validation | Publish to "market:data" channel verification |
| Missing validators | Added @model_validator for config validation |

---

# Running the System

### Test Backtest:
```bash
cd brain_agent
python -m brain_agent.main --mode backtest --coin BTC --limit 500
```

### Direct Backtest:
```bash
python -m brain_agent.backtesting.simple_replay --coin BTC --limit 2000
```

### Live Mode:
```bash
python -m brain_agent.main --mode live --coin BTC
```

---

# Expected Output

```
============================================================
BACKTEST: BTC | Limit: 1000
============================================================

⚠️ Generating synthetic data
✅ Processing 1000 trades...

[100] CVD:    124.50 | Abs:  1.23 | Imb:  0.12
[200] CVD:    -87.30 | Abs:  2.45 | Imb: -0.08
...

============================================================
SUMMARY: Events=1000 | Signals=15 | Final CVD=45.20
============================================================
```

---

*End of Plan - Ready for Implementation*
