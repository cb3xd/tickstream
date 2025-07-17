import websockets, asyncio, json, argparse
from datetime import datetime
import pandas as pd
import keyboard
URL = "wss://wbs.mexc.com/ws"


async def kline_stream(symbol: str, interval: str):
    async with websockets.connect(URL) as websocket:
        print(f"[+] Connected to WebSocket at {URL}")

        # Construct and send subscription message
        message = {
            "method": "SUBSCRIPTION",
            "params": [
                f"spot@public.kline.v3.api@{symbol}@{interval}"
            ]
        }
        await websocket.send(json.dumps(message))
        print(f"[+] Subscribed to {symbol} @ {interval}")

        try:
            while True:
                msg = await websocket.recv()
                print(json.dumps(json.loads(msg), indent=2)) 
        except websockets.exceptions.ConnectionClosed:
            print("[-] WebSocket connection closed.")
        except Exception as e:
            print(f"[!] Error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Minimal WebSocket data stream client.")
    parser.add_argument("symbol", type=str, help="Trading pair (e.g., BTCUSDT)")
    parser.add_argument("--interval", type=str, default="Min1", help="Kline interval")

    args = parser.parse_args()

    asyncio.run(main(args.symbol.upper(), args.interval))