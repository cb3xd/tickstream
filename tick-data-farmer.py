import asyncio
import json
import websockets
import pandas as pd
from datetime import datetime
import sys

WS_URL = "wss://stream.binance.com:9443/ws" 
SYMBOL = "btcusdt"

async def trade_stream(symbol: str):
    symbol = symbol.lower()
    stream_name = f"{symbol}@aggTrade"
    
    trade_data = []
    cvd = 0.0
    
    print(f"[INFO] Attempting to connect to: {WS_URL}")
    print(f"[INFO] Stream: {stream_name}")
    print("[INFO] Press Ctrl+C to stop and save data.")


    try:
        async with websockets.connect(WS_URL, open_timeout=30, ping_interval=None) as websocket:
            print("[SUCCESS] Connected!")

            subscribe_msg = {
                "method": "SUBSCRIBE",
                "params": [stream_name],
                "id": 1
            }
            await websocket.send(json.dumps(subscribe_msg))
            
            while True:
                try:
                    # Wait for message
                    msg = await websocket.recv()
                    data = json.loads(msg)

                    if 'e' not in data or data['e'] != 'aggTrade':
                        continue

                    # 3. PARSE DATA
                    price = float(data['p'])
                    quantity = float(data['q'])
                    timestamp = data['T']
                    is_buyer_maker = data['m']

                    if is_buyer_maker:
                        direction_string = 'SELL'
                        cvd -= quantity
                    else:
                        direction_string = 'BUY'
                        cvd += quantity

                    readable_time = datetime.fromtimestamp(timestamp / 1000).strftime('%H:%M:%S.%f')[:-3]
                    
                    print(f"{readable_time} | {symbol.upper()} | {price:.2f} | {direction_string} | Vol: {quantity:.4f} | CVD: {cvd:.4f}")

                    trade_data.append({
                        'Time': timestamp,
                        'HumanTime': readable_time,
                        'Price': price,
                        'Volume': quantity,
                        'Direction': direction_string,
                        'CVD': cvd
                    })

                except asyncio.TimeoutError:
                    print("[WARN] No data received for a while...")
                    continue

    except asyncio.TimeoutError:
        print("\n[ERROR] Connection Timed Out. \nPossible causes:\n1. You are in a restricted region (USA) -> Use a VPN or change URL to binance.us\n2. Firewall/ISP blocking Port 9443.")
    except websockets.exceptions.ConnectionClosed as e:
        print(f"\n[ERROR] Connection Closed: {e}")
    except KeyboardInterrupt:
        print("\n[INFO] User stopped stream.")
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
    finally:
        # 4. SAVE DATA
        if trade_data:
            filename = f"trades_{symbol}_{int(datetime.now().timestamp())}.csv"
            print(f"[INFO] Saving {len(trade_data)} trades to {filename}...")
            pd.DataFrame(trade_data).to_csv(filename, index=False)
            print("[SUCCESS] File saved.")
        else:
            print("[INFO] No data collected to save.")

async def main():
    await trade_stream(SYMBOL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass