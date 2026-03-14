
## Immediate Next Steps (Development Process)
1. Implement pluggable `data_pipeline.py` for both sources
2. Build `orderflow_engine.py` (features used by ALL strategies)
3. Add first two strategies (Absorption + Delta Divergence)
4. Validate with local backtests on BTC/USDT or SOL/USDT
5. Integrate MCP client (subscribe market:sentiment + agent:control)

Once this core is solid and backtests show edge, we move to Phase 1 (CrewAI selector + DeepSeek-V3).

**LLM Context Note for Agent Development**  
Use DeepSeek-V3 (local via Ollama) for all code generation.  
Prioritize: clean, modular, fully typed, testable code.  
Always keep data sources swappable.

Start by creating `core/data_pipeline.py` with Hyperliquid as default (easiest integration).

---

### 📊 Separate Expanded Research & Comparison (March 2026)

**Core Data Source Decision — Hyperliquid vs Sierra Chart DTC**  
(Strictly for orderflow trading in a Python Brain Agent)

| Criteria                      | Hyperliquid                                      | Sierra Chart DTC Protocol                          | Winner for Our Python Brain Agent |
|-------------------------------|--------------------------------------------------|----------------------------------------------------|-----------------------------------|
| **Orderflow Data Quality**    | Excellent — fully on-chain L2 orderbook + raw trades. Perfect visibility of absorption, icebergs, large limit orders. | Excellent inside Sierra (Number Bars, footprint), but DTC relay has limitations on crypto live data. | Hyperliquid |
| **Python Integration**        | Native official SDK (hyperliquid-python-sdk). Full REST + WebSocket examples. Zero extra server. | Custom socket/JSON client required (forum examples exist). Needs running Sierra Chart instance as DTC server. | Hyperliquid |
| **Real-time Streaming**       | WebSocket subscriptions for trades, l2Book, liquidations — instant & reliable. | Possible via DTC but live data relay restricted for many symbols (crypto especially). | Hyperliquid |
| **Historical Data**           | Full historical via SDK/WS replay (easy for backtesting). | Strong via DTC historical requests, but requires SC server. | Tie |
| **Execution / Trading**       | Same SDK — place/cancel orders natively. | DTC supports order submission (paper + live via Teton), but more complex. | Hyperliquid |
| **Crypto Perps Focus**        | Built for it (BTC, ETH, SOL, etc.). No KYC.     | Supported (Binance, Bitfinex, etc. feeds), but not primary strength. | Hyperliquid |
| **Cost**                      | Near-zero (on-chain gas only).                   | Sierra subscription (~$36/mo) + data fees.         | Hyperliquid |
| **Setup Complexity**          | One `pip install` + API key.                     | Run Sierra Chart + build/maintain DTC client.      | Hyperliquid |
| **Transparency & Security**   | Fully on-chain — no hidden aggregation.          | Depends on data feed provider.                     | Hyperliquid |
| **Scalability for Agent**     | Designed for bots (high throughput WS).          | Works but adds overhead of SC instance.            | Hyperliquid |

**Recommendation (March 2026)**  
**Start with Hyperliquid as the default core data source.**  
It gives us the cleanest, most reliable Python integration with true on-chain orderflow visibility — exactly what we need for accurate CVD, absorption detection, and imbalances without reconstruction hacks.

Use **Sierra Chart DTC as secondary/optional** only for:
- Visual validation of our signals (run Sierra alongside to see footprint)
- If we ever need specific non-crypto futures data later

This matches your goal of “secure data which we actually need” while keeping the Brain Agent pure Python and production-ready.

We will implement **both** in the pluggable pipeline so you can switch instantly and compare results yourself.

---

Just say **“Generate core/data_pipeline.py with Hyperliquid default + Sierra DTC stub”** and I’ll drop the full working code next (ready to run and backtest).

Or tell me any tweak to the README before you commit it.

We are now locked and ready to build the actual core. Your move.