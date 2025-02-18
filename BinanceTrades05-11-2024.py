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
#the step to call the API is one minute in milliseconds
timeStep = 60 * 1000

#update the time for one minute until it reaches the endTime
while updatedTime < endTime: 
    updatedTimesArr.append((updatedTime, min(updatedTime + timeStep, endTime)))
    updatedTime += timeStep

async def get_rounds_per_step(session, step_start, step_end):

    #list to store all the trades
    trades = []
    start = step_start

    while start < step_end:
        params = {
            "symbol" : symbol,
            "startTime" : start,
            "endTime" : step_end,
            "limit" : limit
        }

        #asynchronous request to fetch data from the API
        async with session.get(apiUrl, params=params) as response:
            data = await response.json()         
            if not data: 
                break
            
            #check if data in response is in the expected format
            if not isinstance(data, list):
                print("Unexpected data format:", type(data), data)
                break
            #add data to the list
            trades.extend(data)
            
            #if the data returned is less than the limit (1000) break the loop
            if len(data) < limit:
                break
            
            #update the start time of the next step by one millisec after the Timestamp of the last transaction
            start = data[-1]["T"] + 1
            await asyncio.sleep(0.1)

    #printing the amount of trades fetched for each minute
    start_date = datetime.fromtimestamp(step_start / 1000, timezone.utc)
    end_date = datetime.fromtimestamp(step_end / 1000, timezone.utc)
    print(f"For period {start_date.strftime('%Y-%m-%d %H:%M:%S')} to {end_date.strftime('%Y-%m-%d %H:%M:%S')} : {len(trades)} trades fetched")

    return (step_start, step_end, trades)

async def main():

    #list of all trades fetched
    all_trades = []

    #ste up a connection with a limit of 10 concurrent connections asynchronously
    conn = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        events = [] 
        for steps in updatedTimesArr:
            event = get_rounds_per_step(session, steps[0], steps[1])
            events.append(event)

        #gather all the results from the asynchronous tasks
        results = await asyncio.gather(*events)

        # put the trades from all the steps into one list
        for result in results:
            trades = result[2]
            all_trades.extend(trades)

        return all_trades

#track the timing of the execution
start = time.time()
all_trades = asyncio.run(main())
end = time.time()

print(f"\nFetched a total of {len(all_trades)} trades in {int(end - start)} seconds.")

#working with data
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