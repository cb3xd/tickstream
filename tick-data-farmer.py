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


async def trade_stream(symbol: str):
    df = pd.DataFrame(columns=['Time', 'Price', 'Volume', 'CVD'])
    cvd = 0.0  # initialize CVD accumulator

    async with websockets.connect(URL) as websocket:
        print(f"[+] Connected to WebSocket at {URL}")

        # Subscribe to trade stream
        message = {
            "method": "SUBSCRIPTION",
            "params": [
                f"spot@public.deals.v3.api@{symbol}",
            ]
        }
        await websocket.send(json.dumps(message))
        print(f"[+] Subscribed to {symbol} Trade Stream")

        try:
            while True:
                if keyboard.is_pressed('esc'):
                    print('\n[!] Saving file, exiting...')
                    break
                msg = await websocket.recv()
                data = json.loads(msg)

                trades = data.get('d', {}).get('deals', [])
                pair = data.get('s', '')

                for trade in trades:
                    price = float(trade.get('p'))
                    volume = float(trade.get('v'))
                    direction = trade.get('S')  # 1 = buy, 2 = sell
                    timestamp = trade.get('t')


                    if direction == 1:
                        cvd += volume
                        direction_string = 'BUY'
                    elif direction == 2:
                        cvd -= volume
                        direction_string = 'SELL'

                    time_string = datetime.fromtimestamp(timestamp / 1000).strftime('%H:%M:%S')

                    print(f'{time_string} {pair} {price} {direction_string} {volume:.4f} | CVD: {cvd:.2f}')

                    # Add row to DataFrame
                    df.loc[len(df)] = [timestamp, price, volume, cvd]
        except asyncio.TimeoutError:
            pass
        except KeyboardInterrupt:
            print("[x] Interrupted")
            print(df.tail())
        except websockets.exceptions.ConnectionClosed:
            print("[-] WebSocket connection closed.")
        except Exception as e:
            print(f"[!] Error: {e}")
        finally:
            last_row_time = df['Time'].iloc[-1]
            df.to_csv(f'td_{pair}_{last_row_time}.csv', index=False)
    


async def main():
    await trade_stream('BTCUSDT')
    

if __name__ == "__main__":
    asyncio.run(main())