import requests
import pandas as pd
import aiohttp
import asyncio
import time
from datetime import datetime, timezone

apiUrl = "https://api-gcp.binance.com/api/v3/aggTrades"
symbol = "BTCUSDT"
startTime = 1730800800000
endTime = 1730887200000
limit = 1000

updatedTime = startTime
updatedTimesArr = []
#the step is one minute in milliseconds
timeStep = 60 * 1000

#update the time for one minute untill it reaches the endTime
while updatedTime < endTime: 
    updatedTimesArr.append((updatedTime, min(updatedTime + timeStep, endTime)))
    updatedTime += timeStep

async def get_rounds_per_step(session, step_start, step_end):

    trades = []
    start = step_start

    while start < step_end:
        params = {
            "symbol" : symbol,
            "startTime" : start,
            "endTime" : step_end,
            "limit" : limit
        }

        async with session.get(apiUrl, params=params) as response:
            data = await response.json()         
            if not data: 
                break

            if not isinstance(data, list):
                print("Unexpected data format:", type(data), data)
                break
            trades.extend(data)
            if len(data) < limit:
                break

            start = data[-1]["T"] + 1
            await asyncio.sleep(0.1)

    start_date = datetime.fromtimestamp(step_start / 1000, timezone.utc)
    end_date = datetime.fromtimestamp(step_end / 1000, timezone.utc)
    print(f"For period {start_date.strftime('%Y-%m-%d %H:%M:%S')} to {end_date.strftime('%Y-%m-%d %H:%M:%S')} : {len(trades)} trades fetched")

    return (step_start, step_end, trades)

async def main():

    all_trades = []

    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        events = []
        for steps in updatedTimesArr:
            event = get_rounds_per_step(session, steps[0], steps[1])
            events.append(event)

        results = await asyncio.gather(*events)

        for result in results:
            trades = result[2]
            all_trades.extend(trades)

        return all_trades

start = time.time()
all_trades = asyncio.run(main())
end = time.time()

print(f"\nFetched a total of {len(all_trades)} trades in {int(end - start)} seconds.")

df = pd.DataFrame(all_trades)
df["price"] = df["p"].astype(float)
df["quantity"] = df["q"].astype(float)

df["amountUSDT"] = df["price"] * df["quantity"]

#deciding a side
df["isTheMaker"] = df["m"].map({True: "SELL", False: "BUY"})

#milliseconds to microseconds
df["timestamp"] = df["T"] * 1000

#grouping by the timestamp and the side
groupedTrades = df.groupby(by=["timestamp", "isTheMaker"]).agg(
    amountBtc=("quantity", "sum"),
    amountUsdt=("amountUSDT", "sum")
).reset_index()

#top5 values returned
top5trades = groupedTrades.sort_values(by="amountUsdt", ascending=False).head(5)

print("\nTimestamp (μs)       Amount (BTC)    Amount (USDT)    Side (BUY/SELL)")
for _, row in top5trades.iterrows():
    print(f"{int(row['timestamp']):<20} {row['amountBtc']:<14.3f} {row['amountUsdt']:<16.3f} {row['isTheMaker']}")