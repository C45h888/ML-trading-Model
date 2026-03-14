# Brain Agent Development Plan

> **Status**: Planning Phase  
> **Version**: 1.0  
> **Last Updated**: 2026-03-14

---

## Overview

This document outlines the production-ready development plan for the Brain Agent ML trading system. The plan builds upon the initial implementation and transforms it into a robust, maintainable, and production-grade system.

---

## Core Principles

1. **Type Safety**: Everything must be type-checked with Pydantic v2
2. **Modularity**: Single responsibility for each component
3. **Resilience**: Automatic reconnection, proper error handling
4. **Observability**: Structured logging, health checks, metrics
5. **Extensibility**: Easy to add new strategies and data sources

---

## Phase 1: Foundation & Configuration

### 1.1 Settings Management

**File**: `brain_agent/config/settings.py`

**Why**: Raw YAML is error-prone (typos, missing keys, no type checking, no env override). This becomes the single source of truth for the entire agent.

**Technical Changes**:

- Use `pydantic_settings` + YAML loader
- Auto-merge `.env` + `data_sources.yaml`
- Validation + defaults + secret handling

**Sub-Steps**:

1. Create `settings.py` with `BaseSettings` class
2. Fields: `data_source`, `hyperliquid` dict, `sierra_dtc` dict, `redis_url`, `symbols` list, `mode` (backtest/paper/live), `log_level`
3. Add `model_validator` for cross-field checks (e.g., testnet + mainnet conflict)
4. Expose as `settings = Settings()` singleton

**Benefits**:

- Type-safe everywhere (VSCode autocomplete)
- `settings.data_source` used in every file
- Future-proof for CrewAI/LSTM config later

---

## Phase 2: Data Pipeline Refactor

### 2.1 Split Data Pipeline Architecture

**File**: `brain_agent/core/data_pipeline.py` (refactored)

**Why**: Current monolithic file is hard to debug, test, or extend (mixed WS + Redis + historical + Sierra stub).

**Technical Changes**:

Split into 3 internal classes:

- `HyperliquidSource` — WS connection with reconnection (using tenacity)
- `SierraDTCSource` — Stub → full socket later
- `DataPipeline` — Orchestrator + unified async stream

Introduce `Tick` and `OrderbookSnapshot` Pydantic models

Add Tenacity retry + exponential backoff for WS disconnects

Publish to Redis only after validation

### 2.2 New File: `brain_agent/core/models.py`

**Contents**:

```python
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class Tick(BaseModel):
    """Normalized tick data from any source"""
    timestamp: int
    coin: str
    price: float
    size: float
    side: str  # "B" or "S"
    is_trade: bool

class OrderbookSnapshot(BaseModel):
    """L2 orderbook snapshot"""
    timestamp: int
    coin: str
    bids: list[tuple[float, float]]  # (price, size)
    asks: list[tuple[float, float]]
    spread: float = 0.0
    mid_price: float = 0.0

class OrderflowFeatures(BaseModel):
    """Calculated orderflow features"""
    cvd: float
    absorption_score: float
    bid_ask_imbalance: float
    volume_imbalance: float
    delta_divergence: float
    volume_profile_poc: float
    trade_count: int
    high_price: float
    low_price: float
    close_price: float
```

### 2.3 Data Pipeline Implementation

**Sub-Steps**:

1. Add `models.py` inside `core/` for `Tick`, `Features`, `OrderbookSnapshot`
2. Refactor `data_pipeline.py` (~120 lines total after split)
3. Implement `async def stream_ticks(self)` generator (works for both sources)
4. Add `get_historical_replay()` for backtesting
5. Unit-test hooks (mock WS later)

**Benefits**:

- Latency < 5ms on Hyperliquid ticks
- Easy to swap sources (one line in settings)
- Reconnects automatically (critical for 24/7 paper/live)

---

## Phase 3: Orderflow Engine

### 3.1 Full Orderflow Engine Implementation

**File**: `brain_agent/core/orderflow_engine.py` (enhanced)

**Why**: Currently minimal/empty — this is the brain that feeds Absorption, Delta Divergence, and Imbalance. Must be fast, stateful, and reusable.

**Technical Changes**:

- Use `deque` + rolling pandas for real-time calc
- Pre-compute all features every tick:
  - `cvd` — Cumulative Volume Delta
  - `absorption_score` — Detection of absorbing liquidity
  - `imbalance_ratio` — From L2 data
  - `delta_divergence_zscore` — Statistical divergence
  - `volume_profile_poc/vah/val` — Point of Control, Value Area High/Low
  - `footprint_nodes` — Stub for footprint charts

Configurable windows (default 500 ticks)
Thread-safe + async-friendly

**Sub-Steps**:

1. `__init__` with windows + thresholds from settings
2. `process_tick(tick: Tick) -> Features` method (returns typed dict)
3. Private helpers: `_calc_absorption()`, `_calc_delta_divergence()`, `_calc_imbalance()`
4. Reset logic on new session or regime change
5. Add `get_current_state()` for MCP health/status

**Benefits**:

- Every strategy receives identical clean features
- 10x easier to add 4th/5th strategy later
- Ready for LSTM input later (features vector)

---

## Phase 4: Main Entry Point

### 4.1 Updated Main Runner

**File**: `brain_agent/main.py` (refactored)

**Why**: Current main is just a stub — needs to orchestrate everything and support all modes.

**Technical Changes**:

- Use `asyncio` + `argparse` for `--mode backtest/paper/live`
- Proper structured logging (`structlog`)
- MCP client stub (subscribe control + publish signal)
- Graceful shutdown on kill-switch or Ctrl+C

**Sub-Steps**:

1. Load settings
2. Instantiate Pipeline + Engine + StrategyRegistry
3. If mode == "backtest": `run_backtest()` else `run_live_stream()`
4. Add basic MCP hooks (will expand later)
5. Print nice status table on startup

**Command Usage**:

```bash
# Backtest
python -m brain_agent.main --mode backtest --symbol BTC

# Paper trading
python -m brain_agent.main --mode paper --symbol BTC

# Live trading
python -m brain_agent.main --mode live --symbol BTC
```

**Benefits**:

- One command runs anything
- Clean logs + metrics ready for Grafana

---

## Phase 5: Dependencies & Documentation

### 5.1 Update Requirements

**File**: `requirements.txt`

**Add**:

```
pydantic-settings>=2.5.0
tenacity>=9.0.0
structlog>=25.1.0
pyyaml>=6.0.2
```

### 5.2 Update README

Add section: "Current Status — Optimized Core Complete"

---

## Rollout Sequence

| Step | Task | Estimated Time |
|------|------|----------------|
| 1 | Settings.py (foundation) | 5 min |
| 2 | Data pipeline refactor (reliable data) | 15 min |
| 3 | Orderflow engine full (features ready) | 12 min |
| 4 | Main.py update (everything runnable) | 8 min |
| 5 | Requirements + README | 2 min |

**Total**: ~40 minutes

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Brain Agent                              │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│  │   Settings   │  │    Main      │  │   MCP Client        │ │
│  │  (Pydantic)  │  │   (Runner)   │  │   (Stub)            │ │
│  └──────────────┘  └──────────────┘  └──────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Data Pipeline                         │  │
│  │  ┌─────────────────┐  ┌─────────────────────────────┐   │  │
│  │  │ HyperliquidSource│  │     SierraDTCSource        │   │  │
│  │  │  (WS + tenacity) │  │       (Stub)               │   │  │
│  │  └─────────────────┘  └─────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              ▼                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                 Orderflow Engine                         │  │
│  │  • CVD calculation    • Absorption detection           │  │
│  │  • Imbalance ratio    • Delta divergence                │  │
│  │  • Volume profile     • Footprint (stub)                │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              ▼                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  Strategy Registry                       │  │
│  │  ┌───────────────┐  ┌────────────────┐  ┌────────────┐ │  │
│  │  │  Absorption   │  │ DeltaDivergence│  │ Imbalance  │ │  │
│  │  └───────────────┘  └────────────────┘  └────────────┘ │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Future Enhancements (Post-Phase 1)

1. **CrewAI Integration**: Add strategy selector using CrewAI
2. **DeepSeek-V3**: LLM for signal interpretation
3. **LSTM Model**: ML-based prediction on features
4. **Backtrader Integration**: Full backtesting with broker simulation
5. **Live Execution**: Hyperliquid order execution

---

## Notes

- All code must follow PEP 8 + project linting rules
- Use type hints everywhere
- Add docstrings to all public methods
- Keep functions under 50 lines where possible

---

*End of Plan*
