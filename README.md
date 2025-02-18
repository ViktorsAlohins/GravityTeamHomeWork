**This project was created specifically for Gravity Team as part of the home task. The goal of this task is to fetch and process the biggest BTC/USDT taker trades on Binance Spot for the 24 hours following Trump's re-election as President of the USA for a second term.**

**Task Breakdown**

   Fetch and process trades for the BTC/USDT pair from the Binance Spot API.
   Focus on the taker trades (buyers and sellers).
   Retrieve data for the 24 hours following Trump’s re-election.
   Find the 5 biggest taker trades based on the amount in USDT.
   The output should be formatted as:

    Timestamp (μs)       Amount (BTC)    Amount (USDT)    Side (BUY/SELL)
    1717171715000000     12.000          1234567.890      BUY
    1717171716000000     11.000          1111111.111      SELL
    1717171717000000     10.000          1000000.000      BUY
    1717171718000000     9.000           987654.321       SELL
    1717171719000000     8.000           888888.888       BUY

**Key Features**

   Binance API Integration: Fetches aggregated trade data for BTC/USDT.
   
   Time Range: Uses a specified time window (24 hours following Trump's re-election).
   
   Taker Trades: Focuses on identifying BUY and SELL trades based on the taker side.
   
   Top 5 Trades by Amount in USDT: Outputs the top 5 trades, sorted by the highest total value in USDT.

**Technical Details**

   Time Range:
        The 24-hour period after Trump’s re-election is defined by the startTime and endTime in milliseconds.

   Pagination:
        Binance’s API may return fewer trades than the limit in a single request. The script handles pagination by updating the start time for each subsequent request.

   Asynchronous Requests:
        Asynchronous calls are used to fetch data for each minute within the 24-hour period, improving efficiency.

   Data Processing:
        The data is parsed to calculate the total amount in USDT for each trade and grouped by timestamp and side.
        The top 5 trades are then sorted by the highest value in USDT.

**Requirements**

    Python 3.6 or higher
    pandas - For data manipulation and analysis
    aiohttp - For making asynchronous HTTP requests
