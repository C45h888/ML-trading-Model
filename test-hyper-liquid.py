# test_hyperliquid.py - Enhanced with L2 book subscription
import asyncio
from hyperliquid.info import Info
from hyperliquid.utils import constants

async def test_hyperliquid_with_l2():
    print("Connecting to Hyperliquid mainnet (public data only)...")
    
    info = Info(constants.MAINNET_API_URL, skip_ws=False)
    
    # 1. Mid prices snapshot
    mids = info.all_mids()
    print("\nCurrent mids (sample):")
    print({k: v for k, v in list(mids.items())[:10]})  # first 10 for brevity
    
    # 2. Trades subscription (already working)
    print("\nSubscribing to BTC trades...")
    def trades_cb(msg):
        print("→ Trade:", msg.get('data', [{}])[0])  # simplified print
    
    info.subscribe({"type": "trades", "coin": "BTC"}, trades_cb)
    
    # 3. NEW: L2 book subscription (order book depth)
    print("\nSubscribing to BTC L2 book updates...")
    def l2_cb(msg):
        if msg.get("channel") == "l2Book":
            data = msg.get("data", {})
            coin = data.get("coin")
            levels = data.get("levels", [[], []])
            bids = levels[0][:3] if levels[0] else []  # top 3 bids
            asks = levels[1][:3] if levels[1] else []  # top 3 asks
            print(f"→ L2 Update ({coin}):")
            print("  Top bids:", bids)
            print("  Top asks:", asks)
            print("  Timestamp:", data.get("time"))
    
    info.subscribe({"type": "l2Book", "coin": "BTC"}, l2_cb)
    
    # Run for 30 seconds to collect data
    await asyncio.sleep(30)
    print("\nTest complete. Check for trades + L2 updates above.")

if __name__ == "__main__":
    asyncio.run(test_hyperliquid_with_l2())