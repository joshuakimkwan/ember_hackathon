import requests
import urllib
import logging
import time
import csv 

apikey = "USEAPIKEYASMYID"
secret = "S1XP1e3UZj6A7H5fATj0jNhqPxxdSJYdInClVN65XAbvqqMKjVHjA7PZj4W12oep"
api = "https://mock-api.roostoo.com"

headers = {
    "Authorization": f"Bearer {secret}",
    "Accept": "application/json"
}

servertime_url = api + "/v3/serverTime"
exchangeinfo_url = api + "/v3/exchangeInfo"
marketticker_url = api + "/v3/ticker"

mt_params = {}
mt_params["timestamp"] = "1580762734517"
mt_params["pair"] = "EDEN/USD"

response = requests.get(exchangeinfo_url, headers = headers)

data = response.json()

all_tickers = list(data["TradePairs"].keys())

def append_to_csv(file_path, row_data):
    with open(file_path, mode='a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row_data)

def get_data():
    for ticker in all_tickers:
        mt_params["timestamp"] = "1580762734517"
        mt_params["pair"] = ticker
        market_info = requests.get(marketticker_url, headers = headers, params = mt_params)
        file_name = ticker.replace("/", "")
        path = f"{file_name}.csv"
        info = market_info.json()
        append_to_csv(path, info)
        print(f"Successfully added info for ticker: {ticker}")

while True:
    time.sleep(5)
    get_data()

# class Ember():
#     def get_data(ticker): 
#         url = api + "{}".format(ticker)

#         response = requests.get(url) # json object

#         info = response.json()

#         # Get AAPL, add to CSV file 
#         # Ticker Name, Vol, Price, Time
#         # Clean data "AAPL,1000,150.0,DateTime" 

#     def poll():
#         # Trigger point to make trades
#         for ticker in tickers:
#             self.get_data()
#         pass

#     def start():
#         self.poll()


# if __name__ == __init__:
#     bot = Ember()
#     bot.start() 

# Thrown into Git > 
