# Brain Agent Core

## Project Purpose
This repository contains the **core Python Brain Agent** for the Orderflow Trading System.

It is the foundation that:
- Ingests high-resolution orderflow data
- Computes features (CVD, absorption score, delta divergence, imbalances, footprint nodes)
- Runs backtests with Backtrader
- Publishes clean `agent:signal` messages to the MCP server
- Respects `agent:control` kill-switch

Later phases (CrewAI + DeepSeek-V3 decision layer, LSTM training, live VPS) will plug directly on top of this core — zero rework.

**Current Phase (Phase 0 — Right Now):**  
Nail the pluggable data pipeline + centralized orderflow engine.  
No CrewAI, no LSTM training, no live trading yet.

## Core Data Sources (Locked to These Two Only)
We have narrowed to the only two viable options that give us **secure, high-resolution orderflow data** suitable for a production Python agent:

1. **Hyperliquid** (primary recommendation — see RESEARCH.md)
2. **Sierra Chart DTC Protocol** (secondary option)

The entire `core/data_pipeline.py` is designed to be **fully pluggable**. Switching between them is a single config change.

## Repository Structure (Phase 0)