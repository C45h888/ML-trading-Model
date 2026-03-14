# brain_agent/test_l2.py  (or overwrite test-hyper-liquid.py)
import asyncio
import json
from hyperliquid.info import Info
from hyperliquid.utils import constants

async def test_with_l2():
    print("Hyperliquid Public Data Test + L2 Book")
    
    info = Info(constants.MAINNET_API_URL, skip_ws=False)
    
    # Mid prices (quick check)
    mids = info.all_mids()
    print("\nBTC mid price:", mids.get("BTC", "N/A"))
    
    # Trades subscription
    def trades_cb(msg):
        if msg.get("channel") == "trades":
            for trade in msg.get("data", []):
                print(f"Trade: {trade['side']} {trade['sz']} @ {trade['px']}  ({trade['time']})")
    
    info.subscribe({"type": "trades", "coin": "BTC"}, trades_cb)
    
    # L2 Book subscription (depth)
    def l2_cb(msg):
        if msg.get("channel") == "l2Book":
            data = msg.get("data", {})
            coin = data.get("coin")
            levels = data.get("levels", [[], []])  # [bids, asks]
            top_bids = levels[0][:3] if levels[0] else []
            top_asks = levels[1][:3] if levels[1] else []
            print(f"\nL2 Update ({coin}):")
            print("  Top bids:", [f"{l['px']} ({l['sz']})" for l in top_bids])
            print("  Top asks:", [f"{l['px']} ({l['sz']})" for l in top_asks])
            print("  Timestamp:", data.get("time"))
    
    info.subscribe({"type": "l2Book", "coin": "BTC"}, l2_cb)
    
    print("\nListening for trades + L2 updates (30 seconds)...")
    await asyncio.sleep(30)
    print("Test done.")

if __name__ == "__main__":
    asyncio.run(test_with_l2())