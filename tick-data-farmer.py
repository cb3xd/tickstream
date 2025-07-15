import websockets, asyncio, json, argparse


async def main(symbol: str, interval: str):
    url = "wss://wbs.mexc.com/ws"

    async with websockets.connect(url) as websocket:
        print(f"[+] Connected to WebSocket at {url}")

        # Construct and send subscription message
        message = {
            "method": "SUBSCRIPTION",
            "params": [
                f"spot@public.kline.v3.api@{symbol}@{interval}"
            ]
        }
        await websocket.send(json.dumps(message))
        print(f"[+] Subscribed to {symbol} @ {interval}")

        # Receive and print raw messages
        try:
            while True:
                msg = await websocket.recv()
                print(json.dumps(json.loads(msg), indent=2))  # Pretty-print JSON
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