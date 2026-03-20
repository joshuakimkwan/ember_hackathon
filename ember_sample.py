import requests
import time
import hmac
import hashlib
import csv
import pandas as pd
import os 
import asyncio
import numpy as np
import matplotlib.pyplot as plt
import logging

# Configure the root logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- API Configuration ---
BASE_URL = "https://mock-api.roostoo.com"
# General Portfolio Testing API
API_KEY = "VEl4HZ01sGMbCyr48He5r7CGhTIgaoUj4IeEHpFdb77qyJIOb3YlsdQm681AJs6A"      # Replace with your actual API key
SECRET_KEY = "7ue4oQRfdkGu4bhRXBmkbjA5iO7fTY4Zdaz6l0XGT6mXvNiYQqpiz4mWPVriU4Wo"  # Replace with your actual secret key
# R1 Competition API
# API_KEY = "KPmLBKLYVEmPgRsiyYkN33UY5KlLPv1Qi7ykUJAvqEB5Fj888IiALZncN1YlwmO4"
# SECRET_KEY = "h7rT1aw2MiHgCgOt2Hu0crUZy1kSG6oho4UMMcgRUveMvIQ3H3B7ivla16krAegj"

# ------------------------------
# Helpers 
# ------------------------------
def append_to_csv(file_path, row_data):
    with open(file_path, mode='a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row_data)

# ------------------------------
# Utility Functions
# ------------------------------

def _get_timestamp():
    """Return a 13-digit millisecond timestamp as string."""
    return str(int(time.time() * 1000))


def _get_signed_headers(payload: dict = {}):
    """
    Generate signed headers and totalParams for RCL_TopLevelCheck endpoints.
    """
    payload['timestamp'] = _get_timestamp()
    sorted_keys = sorted(payload.keys())
    total_params = "&".join(f"{k}={payload[k]}" for k in sorted_keys)

    signature = hmac.new(
        SECRET_KEY.encode('utf-8'),
        total_params.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    headers = {
        'RST-API-KEY': API_KEY,
        'MSG-SIGNATURE': signature
    }

    return headers, payload, total_params


# ------------------------------
# Public Endpoints
# ------------------------------

def check_server_time():
    """Check API server time."""
    url = f"{BASE_URL}/v3/serverTime"
    try:
        res = requests.get(url)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error checking server time: {e}")
        return None


def get_exchange_info():
    """Get exchange trading pairs and info."""
    url = f"{BASE_URL}/v3/exchangeInfo"
    try:
        res = requests.get(url)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting exchange info: {e}")
        return None


def get_ticker(pair=None):
    """Get ticker for one or all pairs."""
    url = f"{BASE_URL}/v3/ticker"
    params = {'timestamp': _get_timestamp()}
    if pair:
        params['pair'] = pair
    try:
        res = requests.get(url, params=params)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting ticker: {e}")
        return None


# ------------------------------
# Signed Endpoints
# ------------------------------

def get_balance():
    """Get wallet balances (RCL_TopLevelCheck)."""
    url = f"{BASE_URL}/v3/balance"
    headers, payload, _ = _get_signed_headers({})
    try:
        res = requests.get(url, headers=headers, params=payload)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting balance: {e}")
        print(f"Response text: {e.response.text if e.response else 'N/A'}")
        return None


def get_pending_count():
    """Get total pending order count."""
    url = f"{BASE_URL}/v3/pending_count"
    headers, payload, _ = _get_signed_headers({})
    try:
        res = requests.get(url, headers=headers, params=payload)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error getting pending count: {e}")
        print(f"Response text: {e.response.text if e.response else 'N/A'}")
        return None


def place_order(pair_or_coin, side, quantity, price=None, order_type=None):
    """
    Place a LIMIT or MARKET order.
    """
    url = f"{BASE_URL}/v3/place_order"
    pair = f"{pair_or_coin}/USD" if "/" not in pair_or_coin else pair_or_coin

    if order_type is None:
        order_type = "LIMIT" if price is not None else "MARKET"

    if order_type == 'LIMIT' and price is None:
        print("Error: LIMIT orders require 'price'.")
        return None

    payload = {
        'pair': pair,
        'side': side.upper(),
        'type': order_type.upper(),
        'quantity': str(quantity)
    }
    if order_type == 'LIMIT':
        payload['price'] = str(price)

    headers, _, total_params = _get_signed_headers(payload)
    headers['Content-Type'] = 'application/x-www-form-urlencoded'

    try:
        res = requests.post(url, headers=headers, data=total_params)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error placing order: {e}")
        print(f"Response text: {e.response.text if e.response else 'N/A'}")
        return None


def query_order(order_id=None, pair=None, pending_only=None):
    """Query order history or pending orders."""
    url = f"{BASE_URL}/v3/query_order"
    payload = {}
    if order_id:
        payload['order_id'] = str(order_id)
    elif pair:
        payload['pair'] = pair
        if pending_only is not None:
            payload['pending_only'] = 'TRUE' if pending_only else 'FALSE'

    headers, _, total_params = _get_signed_headers(payload)
    headers['Content-Type'] = 'application/x-www-form-urlencoded'

    try:
        res = requests.post(url, headers=headers, data=total_params)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error querying order: {e}")
        print(f"Response text: {e.response.text if e.response else 'N/A'}")
        return None


def cancel_order(order_id=None, pair=None):
    """Cancel specific or all pending orders."""
    url = f"{BASE_URL}/v3/cancel_order"
    payload = {}
    if order_id:
        payload['order_id'] = str(order_id)
    elif pair:
        payload['pair'] = pair

    headers, _, total_params = _get_signed_headers(payload)
    headers['Content-Type'] = 'application/x-www-form-urlencoded'

    try:
        res = requests.post(url, headers=headers, data=total_params)
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error canceling order: {e}")
        print(f"Response text: {e.response.text if e.response else 'N/A'}")
        return None

# ------------------------------
# Own Functions
# ------------------------------
async def tickers_to_csv(ticker_list):
    # TODO Change this to run asynchronously 
    time_now = _get_timestamp()
    for ticker in ticker_list:
        tdata = get_ticker(ticker)

        if tdata and tdata["Success"] == True:
            info = tdata.get("Data", {}).get(ticker, {})
        else:
            logging.info(f"Failed to get data for {ticker}")
            with open(f"{ticker.replace("/", "_")}.csv", "w") as f:
                pass
            continue

        file_name = ticker.replace("/", "_")
        path = f"./{file_name}.csv"
        try:
            with open(path, 'r') as f:
                pass
        except FileNotFoundError:
            headers = ["Timestamp"]
            headers += list(info.keys())
            append_to_csv(path, headers)
        upload_info = [time_now] 
        upload_info += list(info.values())
        append_to_csv(path, upload_info)

### Metrics Computation

def calculate_double_EMA(df, time_period, column):
    # df refers to the csv being opened at that point in time, which will be accessed via pandas df
    EMA = df[column].ewm(span=time_period, adjust=False).mean()
    DEMA = 2*EMA - EMA.ewm(span=time_period, adjust=False).mean()
    return DEMA

def create_double_EMA_columns(df, column, short_time_period=20, long_time_period=50):
    df["DEMA_Short"] = calculate_double_EMA(df, short_time_period, column)
    df["DEMA_Long"] = calculate_double_EMA(df, long_time_period, column)
    return None

def calculate_MA(df, time_period, column):
    MA = df[column].rolling(window=time_period, min_periods=1).mean()
    return MA

def calculate_ATR(df, time_period, column):
    ATR = df[column].diff().abs().rolling(window=time_period).mean()
    return ATR

async def compute_metrics(ticker_list, short=20, long=50):
    # Compute the DEMA 
    # Convert csv to pandas dataframe for computation of DEMA
    # Remove earliest row if length exceeds 2*long_period - 1
    for ticker in ticker_list: 
        path = f"./{ticker.replace("/", "_")}" + ".csv"
        if os.path.getsize(path) == 0:
            continue
        df = pd.read_csv(path)
        
        df["DEMA_Short"] = calculate_double_EMA(df, short, "LastPrice")
        df["DEMA_Long"] = calculate_double_EMA(df, long, "LastPrice")
        df["MA"] = calculate_MA(df, short, "LastPrice")
        df["ATR"] = calculate_ATR(df, short, "LastPrice")
        while len(df) > 2*long - 1:
            df = df.iloc[1:]
        df.to_csv(path, index=False) # Update the csv

def check_for_trades(df, portfolio, pair_or_coin, curr_cash, buy_expenditure):
    # TODO Iterate through CSV. (Added: CSV should be maintained at (2*long_time_period-1) rows)
    # Check if we take a trade - To check past information in CSV to see if the last price is above / lower EMA bounds
    spread = df["MaxBid"] - df["MinAsk"]
    mid_spread = (df["MaxBid"] + df["MinAsk"]) / 2
    quantity_buy = buy_expenditure / mid_spread

    ### COMBINING SCTIONS
    current_orders = query_order(None, pair_or_coin)
    if current_orders['Success'] == False:
        pending_orders = []
    else:
        pending_orders = [order for order in current_orders["OrderMatched"] if order["Status"] == "PENDING"]
    # pending_orders should contain only 2 entries. For each ticker, [0] entry is BUY, [1] entry is sell.
    # This means that we would not have more than 1 limit order for BUY for any point in time. Similar for SELL.

    ma20 = df["MA"].iloc[-1]
    atr = df["ATR"].iloc[-1]
    quantity_buy_limit = quantity_buy * 0.7  # 70% for limit order
    quantity_buy_market = quantity_buy * 0.3  # 30% for market order

    current_position = portfolio.loc[portfolio["Ticker_name"] == pair_or_coin, "Quantity"].values


    # Example: if mid_spread = 10000, and buy_expenditure is $100, then we buy 100/10000 units

    # NOTE Compare only final row assuming CSV maintained at 2*long_period-1 rows
    # For buying, we check:
    # 1. We are currently NOT in the long position  : df["indicator"][1] == False
    # 2. The short and long DEMA cross each other   : df["DEMA_Short"][-1] > df["DEMA_Long"][-1]
    if df["indicator"][1] == False \
        and df["DEMA_Short"][-1] > df["DEMA_Long"][-1] \
        and curr_cash > buy_expenditure:

        # For now, do market order if spread is < 0.001
        if spread < 0.001:
            # BUY at market order, followed by immediately updating portfolio
            order = place_order(pair_or_coin, "BUY", current_position)
            order_id = order["OrderDetail"]["OrderID"]
            quantity_buy = order["OrderDetail"]["Quantity"]
            price = order["OrderDetail"]["Price"]
            PnL = 0 # Since this is buy, nothing profitted yet
            update_orders(pair_or_coin, "BUY", quantity_buy)
            update_portfolio(order_id, pair_or_coin, "BUY", quantity_buy, "MARKET", price, PnL) # TODO Use API to get current price and compute PnL
            df["indicator"][1] = True
        else:
            # BUY at limit order
            # 如果没有挂单或者现有挂单价格偏离过大，可以挂新的
            if not pending_orders or abs(pending_orders[0]['Price'] - (ma20 - 0.5*atr)) / mid_spread > 0.05:
                limit_price = ma20 - 0.5 * atr
                if abs(limit_price - mid_spread)/mid_spread < 0.10:  # 不偏离现价过多
                    limit_price *= (1 + np.random.uniform(-0.001, 0.001))  # 避免整数关口
                    # 挂单
                    order = place_order(pair_or_coin, "BUY", quantity_buy_limit, price=limit_price, order_type="LIMIT")
                    add_pending_orders(order, "./pending_orders.csv")
                    # Need below??
                    # order_id = order["OrderDetail"]["OrderID"]
                    # df["indicator"][1] = True
                    # PnL = 0 # Since this is buy, nothing profitted yet
                    # update_portfolio(order_id, pair_or_coin, "BUY", quantity_buy_limit, "LIMIT", limit_price, PnL) # TODO Use API to get current price and compute PnL
            # Query order to see if we can place a buy
        
        # Change indicator to True, indicating a position is now buy
        df["indicator"][1] = True

    # NOTE Compare only final row assuming CSV maintained at 2*long_period-1 rows
    # For selling, we check:
    # 1. We are currently in the long position                              :   df["indicator"][1] == True
    # 2. The short and long DEMA cross each other                           :   df["Short_DEMA"][-1] < df["Long_DEMA"][-1]
    # 3. Currently holding a positive quantity of stock in our portfolio    :   current_position > 0               : 
    elif df["indicator"][1] == True \
        and df["DEMA_Short"][-1] < df["DEMA_Long"][-1] \
        and current_position > 0:
        price_bought = portfolio.loc[portfolio["Ticker_name"] == pair_or_coin, "Price"].values
        # For now, do market order if spread is < 0.001
        if spread < 0.001:
            # SELL at market order. SELL will close entire position for simplicity
            order = place_order(pair_or_coin, "SELL", current_position)
            order_id = order["OrderDetail"]["OrderID"]
            quantity_buy = order["OrderDetail"]["Quantity"]
            price = order["OrderDetail"]["Price"]
            PnL = price_bought - price
            update_orders(pair_or_coin, "SELL", quantity_buy)
            update_portfolio(order_id, pair_or_coin, "SELL", quantity_buy, "MARKET", price, PnL) # TODO Use API to get current price and compute PnL
            df["indicator"][1] = False

        else:
            # SELL at limit order. When we limit sell, we close the entire trade.
            if not pending_orders or abs(pending_orders[1]['Price'] - (ma20 + 0.5*atr)) / mid_spread > 0.05:
                limit_price = ma20 + 0.5 * atr
                if abs(limit_price - mid_spread)/mid_spread < 0.10:  # 不偏离现价过多
                    limit_price *= (1 + np.random.uniform(-0.001, 0.001))  # 避免整数关口
                    # 挂单
                    order = place_order(pair_or_coin, "SELL", current_position, price=limit_price, order_type="LIMIT")
                    order_id = order["OrderDetail"]["OrderID"]
                    add_pending_orders(order, "./pending_orders.csv")
    else:
        # Continue to hold
        # Append position as NaN in our portfolio CSV since no BUY/SELL action taken
        current_time = int(time.time() * 1000)  # 当前时间，单位毫秒
        max_pending_duration = 60 * 60 * 1000  # 最大挂单时间 1 小时（可调）

        for order in pending_orders:
            order_age = current_time - order["CreateTimestamp"]
            # 计算目标价格（滚动支撑/阻力）
            if order['Side'] == "BUY":
                target_price = ma20 - 0.5 * atr
            elif order['Side'] == "SELL":
                target_price = ma20 + 0.5 * atr
            else:
                continue
            # 判断是否需要撤单：价格偏离过大 或 存在时间过长
            price_deviation = abs(order['Price'] - mid_spread) / mid_spread
            if price_deviation > 0.05 or order_age > max_pending_duration:
                cancel_order(order_id=order['OrderID'])
                print(f"Cancelled pending {order['Side']} order {order['OrderID']} due to deviation/time")
        pass
    

def update_orders(): # TODO Parameters WIP 
    # TODO New orders to be added to order file 
    # query_order to check if any current orders are still pending
    # cancel_order to remove if the targets have changed
    # if query_order shows it is completed, completed orders to be deleted from order file and added to portfolio
    order_file = "./orders.csv"
    try:
        with open(order_file, 'r') as f:
            pass
    except FileNotFoundError:
        headers = [] # Headers TBD
        append_to_csv(order_file, headers)
    
    if query_order() == "":
        # If this is true, we add/remove to portfolio
        pass
    else:
        # If this is false, we want to check if the target is still valid. If not we will cancel
        pass

def portfolio_file():
    portfolio_file = "./portfolio.csv"
    try:
        with open(portfolio_file, 'r') as f:
            # Check if the file is empty
            first_line = f.readline()
            if not first_line.strip():
                # File is empty, write headers
                headers = ["Timestamp", "OrderID", "Ticker_name", "Transaction_type", "Price", "Quantity", "Side", "PnL", "Commission"]
                append_to_csv(portfolio_file, headers)
            else:
                pass
    except FileNotFoundError:
        headers = ["Timestamp", "OrderID", "Ticker_name", "Transaction_type", "Price", "Quantity", "Side", "PnL", "Commission"]
        append_to_csv(portfolio_file, headers)
    portfolio_df = pd.read_csv(portfolio_file)
    return portfolio_df

def update_portfolio(order_id, pair_or_coin, side, quantity, transaction_type, price, PnL): # TODO Parameters WIP
    # TODO To add/remove successful orders into portfolios
    # Close entire position for simplicity, can advance this next time
    # If a successful order is removed, we want to update our PnL / balance (this might have a function)
    portfolio_file = "./portfolio.csv"

    if transaction_type == "MARKET":
        commission = ((0.1)/100) * price * quantity
    elif transaction_type == "LIMIT":
        commission = ((0.05)/100) * price * quantity

    try:
        with open(portfolio_file, 'r') as f:
            # Check if the file is empty
            first_line = f.readline()
            if not first_line.strip():
                # File is empty, write headers
                headers = ["Timestamp", "OrderID", "Ticker_name", "Transaction_type", "Price", "Quantity", "Side", "PnL", "Commission"]
                append_to_csv(portfolio_file, headers)

                # After headers are written, append the actual data row
                timestamp = _get_timestamp()
                data_row = [timestamp, order_id, pair_or_coin, transaction_type, price, quantity, side, PnL, commission]
                append_to_csv(portfolio_file, data_row)
            else:
                # File is not empty and has headers, append a new row
                timestamp = _get_timestamp()
                data_row = [timestamp, order_id, pair_or_coin, transaction_type, price, quantity, side, PnL, commission]
                append_to_csv(portfolio_file, data_row)
    except FileNotFoundError:
        headers = ["Timestamp", "OrderID", "Ticker_name", "Transaction_type", "Price", "Quantity", "Side", "PnL", "Commission"]
        append_to_csv(portfolio_file, headers)
        timestamp = _get_timestamp()
        data_row = [timestamp, order_id, pair_or_coin, transaction_type, price, quantity, side, PnL, commission]
        append_to_csv(portfolio_file, data_row)

def remove_pending_orders(orders):
    for order in orders["OrderMatched"]:
        order_id = order["OrderID"]
        pair = order["Pair"]
        status = order["Status"]
        if status == "PENDING":
            cancel_order(order_id, pair)


def create_headers():
    headers = ["Pair", "OrderID", "Status", "CreateTimestamp", "FinishTimestamp", "Side", "Type", "Price", "Quantity"]
    files = ["./pending_orders.csv", "./portfolio.csv"]
    for csv_file in files:
        try:
            os.remove(csv_file)
        except FileNotFoundError:
            logging.error(f"{csv_file} not found. Continuing...")
        try:
            with open(csv_file, 'r') as f:
                pass
        except FileNotFoundError:
            append_to_csv(csv_file, headers)

def add_pfo_orders(order, csv_file): 
    df = pd.read_csv(csv_file)
    headers = df.columns
    odata = []
    for head in headers:
        odata.append(order[head])
    if not df[df["OrderID"] == int(order["OrderID"])].empty:
        logging.info(f"Order {order["OrderID"]} has been already added to {csv_file}")
    else:
        side = df.loc[df["Pair"] == order["Pair"], "Side"]
        if side.empty:
            logging.info(f"Added {odata} into {csv_file}")
            df.loc[len(df)] = odata
        else:
            pfo_side = 1 if side.iloc[0] == "BUY" else -1
            order_side = 1 if order["Side"] == "BUY" else -1

            new_quantity = df.loc[df["Pair"] == order["Pair"], "Quantity"].iloc[0] * pfo_side + order["Quantity"] * order_side
            new_price = ( df.loc[df["Pair"] == order["Pair"], "Price"] * df.loc[df["Pair"] == order["Pair"], "Quantity"] * pfo_side + \
                        order["Quantity"] * order["Price"] * order_side ) / \
                        new_quantity
            if new_quantity == 0:
                logging.info(f"Empty quantity found for {order["Pair"]}, removing pair from portfolio")
                drop_index = df[df["Pair"] == order["Pair"]].index
                df.drop(drop_index, inplace = True)
            elif new_quantity < 0:
                logging.info(f"New price {new_price} and quantity {new_quantity} found for {order["Pair"]}")
                df.loc[df["Pair"] == order["Pair"], "Side"] = "SELL"
                df.loc[df["Pair"] == order["Pair"], "Price"] = new_price
                df.loc[df["Pair"] == order["Pair"], "Quantity"] = new_quantity * -1
            else:
                logging.info(f"New price {new_price} and quantity {new_quantity} found for {order["Pair"]}")
                df.loc[df["Pair"] == order["Pair"], "Side"] = "BUY"
                df.loc[df["Pair"] == order["Pair"], "Price"] = new_price
                df.loc[df["Pair"] == order["Pair"], "Quantity"] = new_quantity
        df.to_csv(csv_file, index = False)

def add_pending_orders(order, csv_file):
    pass    

def check_portfolio(orders):
    for order in orders["OrderMatched"]:
        status = order["Status"]
        if status == "PENDING":
            # Add to orders.csv
            add_pending_orders(order, "./pending_orders.csv")
            pass
        elif status == "FILLED":
            # Add to portfolio.csv
            add_pfo_orders(order, "./portfolio.csv")
            pass
        else:
            continue
        


async def main():
    """
    For each ticker, do (while True:)
    1. Open CSV of the ticker + Calculate DEMA 
    2. Populate CSV with data (see function tickers_to_csv or see the csv that has been populated)
    3. Compute the DEMA for that CSV while it is still open
        a. Also, ensure there are exactly 2*long_period - 1 rows in the CSV 
        b. In particular, add the latest data, followed by removing the earliest row, then compute the DEMA columns
     """
    print(f"Started at {time.strftime('%X')}")
    info = get_exchange_info()
    ticker_list = list(info.get('TradePairs', {}).keys())
    await asyncio.gather(
        tickers_to_csv(ticker_list),     # Always keep getting exchange data
        compute_metrics(ticker_list)#,       # Always recompute DEMA        
        #check_for_trades()               # Check for trades
    )
    print(f"Finished at {time.strftime('%X')}")


# ------------------------------
# Quick Demo Section
# ------------------------------
if __name__ == "__main__":
    all_orders = query_order()

    logging.info("--- Creating necessary files if they don't exist ---")
    create_headers()

    logging.info("--- Remove all pending trades ---")
    remove_pending_orders(all_orders)

    logging.info("--- Validate all trades ---")
    check_portfolio(all_orders)

    logging.info("--- Checking Server Time ---")
    logging.info(check_server_time())

    logging.info("--- Getting Exchange Info ---")
    info = get_exchange_info()
    # if info:
    #     print(f"Available Pairs: {list(info.get('TradePairs', {}).keys())}")
    
    logging.info("--- Running query_order ---")
    # logging.info(query_order())

    # print("\n--- Getting Market Ticker (BTC/USD) ---")
    # ticker = get_ticker("BTC/USD")
    # if ticker:
    #     print(ticker.get("Data", {}).get("BTC/USD", {}))

    #print("\n--- Triggering place_order ---")
    #print(place_order("ADA/USD", "SELL", "3.5", "0.34", "LIMIT"))
    #print("\n--- Triggering query_order ---")
    #print([order["Status"] for order in query_order(None, "ADA/USD")["OrderMatched"]])
    # while True:
    #     asyncio.run(main())

    # print("\n--- Getting Account Balance ---")
    # print(get_balance())

    # print("\n--- Checking Pending Orders ---")
    # print(get_pending_count())

    # # Uncomment these to test trading actions:
    # # print(place_order("BTC", "BUY", 0.01, price=95000))  # LIMIT
    # print(place_order("BNB/USD", "BUY", 1))      
    # print(place_order("BNB/USD", "SELL", 1))             # MARKET       
    # print(query_order(pair="BNB/USD", pending_only=False))
    # print(cancel_order(pair="BNB/USD"))

