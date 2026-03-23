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
import glob 
# made change
# Configure the root logger
logging.basicConfig(filename = 'app.log', level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(funcName)s] %(message)s', filemode = 'a')

# --- API Configuration ---
BASE_URL = "https://mock-api.roostoo.com"
# General Portfolio Testing API
API_KEY = "VEl4HZ01sGMbCyr48He5r7CGhTIgaoUj4IeEHpFdb77qyJIOb3YlsdQm681AJs6A"      # Replace with your actual API key
SECRET_KEY = "7ue4oQRfdkGu4bhRXBmkbjA5iO7fTY4Zdaz6l0XGT6mXvNiYQqpiz4mWPVriU4Wo"  # Replace with your actual secret key
# R1 Competition API
# API_KEY = "KPmLBKLYVEmPgRsiyYkN33UY5KlLPv1Qi7ykUJAvqEB5Fj888IiALZncN1YlwmO4"
# SECRET_KEY = "h7rT1aw2MiHgCgOt2Hu0crUZy1kSG6oho4UMMcgRUveMvIQ3H3B7ivla16krAegj"
info = None
balance = None

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
async def process_ticker(ticker):
    time.sleep(0.1)
    time_now = _get_timestamp()

    try:
        # 把同步函数变成异步执行（线程池）
        tdata = await asyncio.to_thread(get_ticker, ticker)

        if tdata and tdata.get("Success") is True:
            info = tdata.get("Data", {}).get(ticker, {})
        else:
            logging.info(f"Failed to get data for {ticker}")
            open(f"./ticker_csv/{ticker.replace('/', '_')}.csv", "w").close()
            return
        file_name = ticker.replace("/", "_")
        path = f"./ticker_csv/{file_name}.csv"
        # 如果文件不存在 → 写header
        if not os.path.exists(path):
            headers = ["Timestamp"] + list(info.keys())
            append_to_csv(path, headers)

        # 写数据
        upload_info = [time_now] + list(info.values())
        append_to_csv(path, upload_info)

    except Exception as e:
        logging.error(f"Error processing {ticker}: {e}")
  
# 主函数（并发执行）
async def tickers_to_csv(ticker_list):
    tasks = [process_ticker(ticker) for ticker in ticker_list]
    # 并发执行
    await asyncio.gather(*tasks)

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

def calculate_MA20(df, column, time_period=20):
    MA = df[column].rolling(window=time_period, min_periods=1).mean()
    return MA

def calculate_MA50(df, column, time_period=50):
    MA = df[column].rolling(window=time_period, min_periods=1).mean()
    return MA

def calculate_MA100(df, column, time_period=100):
    MA = df[column].rolling(window=time_period, min_periods=1).mean()
    return MA

def calculate_RSI(df, column, period):
    # Use 25-75
    delta = df[column].diff()

    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.finfo(float).eps)

    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_EMA(df, column, period):
    EMA = df[column].ewm(span=period, adjust=False).mean()
    return EMA

async def compute_metrics(ticker_list, short=10, long=30):
    # Compute the DEMA 
    # Convert csv to pandas dataframe for computation of DEMA
    # Remove earliest row if length exceeds 2*long_period - 1
    for ticker in ticker_list: 
        path = f"./ticker_csv/{ticker.replace('/', '_')}" + ".csv"
        if os.path.getsize(path) == 0:
            continue
        df = pd.read_csv(path)
        rows = max(10,20,30,7,12,50)
        if len(df) >= 2 * rows - 1:
            df["DEMA_Short"] = calculate_double_EMA(df, 10, "LastPrice")
            df["DEMA_Long"] = calculate_double_EMA(df, 30, "LastPrice")
            df["MA"] = calculate_MA(df, 20, "LastPrice")
            df["ATR"] = calculate_ATR(df, 20, "LastPrice")
            df["RSI"] = calculate_RSI(df, "LastPrice", 7) # RSI<25 BUY, else SELL
            df["EMA12"] = calculate_EMA(df, "LastPrice", 12) # EMA12>EMA50 BUY, else SELL
            df["EMA50"] = calculate_EMA(df, "LastPrice", 50)
        while len(df) > 2*rows - 1:
            df = df.iloc[1:]
        df.to_csv(path, index=False) # Update the csv

def check_for_trades(df, pair_or_coin, curr_cash, buy_expenditure):
    # TODO Iterate through CSV. (Added: CSV should be maintained at (2*long_time_period-1) rows)
    # Check if we take a trade - To check past information in CSV to see if the last price is above / lower EMA bounds
    if len(df) < 99: # check function on compute_metrics for number (2*rows-1)
        return

    spread = df["MaxBid"] - df["MinAsk"]
    mid_spread = (df["MaxBid"] + df["MinAsk"]) / 2
    quantity_buy = buy_expenditure / mid_spread.iloc[-1]

    ### COMBINING SECTIONS
    current_orders = query_order(None, pair_or_coin)
    if current_orders['Success'] == False:
        pending_orders = []
    else:
        pending_orders = [order for order in current_orders["OrderMatched"] if order["Status"] == "PENDING"]
    # pending_orders should contain only 2 entries. For each ticker, [0] entry is BUY, [1] entry is sell.
    # This means that we would not have more than 1 limit order for BUY for any point in time. Similar for SELL.

    ma20 = df["MA"].iloc[-1]
    atr = df["ATR"].iloc[-1]

    coin_info = info['TradePairs'][pair_or_coin]

    quantity_buy_limit = round(quantity_buy * 0.7, coin_info['AmountPrecision'])  # 70% for limit order
    quantity_buy_market = round(quantity_buy * 0.3, coin_info['AmountPrecision'])  # 30% for market order

    balance = get_balance()
    try:
        curr_position = balance['SpotWallet'][pair_or_coin.replace('/USD','')]['Free']
        curr_cash = balance['SpotWallet']["USD"]["Free"]
        current_position = round(curr_position, coin_info['AmountPrecision'])
    except:
        logging.error(f"Failed to get balance: {balance}")
        return None
       
    # Example: if mid_spread = 10000, and buy_expenditure is $100, then we buy 100/10000 units
    # Current_position condition updated to current_position < 0.1 due to floating point error in the get_balance
    if current_position <= 0.1 \
        and df["DEMA_Short"].iloc[-1] > df["DEMA_Long"].iloc[-1] \
        and df["RSI"].iloc[-1] < 25 \
        and df["EMA12"].iloc[-1] > df["EMA50"].iloc[-1] \
        and curr_cash > buy_expenditure:
        logging.info(f"Current_position {current_position} {pair_or_coin}, DEMA_Short: {df['DEMA_Short'].iloc[-1]}, DEMA_Long: {df['DEMA_Long'].iloc[-1]}, curr_cash: {curr_cash}, buy_exp: {buy_expenditure}")

        # For now, do market order if spread is < 0.001
        if spread.iloc[-1] < 0.001:
            # BUY at market order, followed by immediately updating portfolio
            last_price = df["LastPrice"].iloc[-1]
            if last_price*quantity_buy_market >= coin_info['MiniOrder']:
                logging.info(f"Sending BUY Order for {pair_or_coin} with quantity {quantity_buy_market}")
                order = place_order(pair_or_coin, "BUY", quantity_buy_market)
                logging.info(f"Order info: {order}")
                try:
                    update_pfo(order["OrderDetail"]["Pair"])
                except:
                    logging.error(order["ErrMsg"])
                
        else:
            # BUY at limit order
            # 如果没有挂单或者现有挂单价格偏离过大，可以挂新的
            if pending_orders:
                logging.info(f"[BUY] Pending Orders: {pending_orders}")
                for order in pending_orders:
                    if abs(pending_orders[0]['Price'] - (ma20 - 1.5*atr)) / mid_spread > 0.05:
                        logging.info(f"Cancelling order for {pair_or_coin}")
                        cancel_order(order["OrderID"], pair_or_coin)
                        limit_price = ma20 - 1.5 * atr
                        if abs(limit_price - mid_spread)/mid_spread < 0.10:  # 不偏离现价过多
                            limit_price *= (1 + np.random.uniform(-0.001, 0.001))  # 避免整数关口
                            # 挂单
                            if limit_price*quantity_buy_limit >= coin_info['MiniOrder']:
                                order = place_order(pair_or_coin, "BUY", quantity_buy_limit, price=limit_price, order_type="LIMIT")
                                try:
                                    add_pending_orders(order["OrderDetail"], "./pending_orders.csv")
                                except:
                                    logging.error(order["ErrMsg"])
            else:
                logging.info(f"[BUY] Not pending Orders: {pending_orders}")
                limit_price = ma20 - 1.5 * atr
                if abs(limit_price - mid_spread)/mid_spread < 0.10:  # 不偏离现价过多
                    limit_price *= (1 + np.random.uniform(-0.001, 0.001))  # 避免整数关口
                    # 挂单
                    if limit_price*quantity_buy_limit >= coin_info['MiniOrder']:
                        order = place_order(pair_or_coin, "BUY", quantity_buy_limit, price=limit_price, order_type="LIMIT")
                        try:
                            add_pending_orders(order["OrderDetail"], "./pending_orders.csv")
                        except:
                            logging.error(order["ErrMsg"])

    # 1. The short and long DEMA cross each other                           :   df["Short_DEMA"][-1] < df["Long_DEMA"][-1]
    # 2. Currently holding a positive quantity of stock in our portfolio    :   current_position > 0               : 
    elif current_position > 0.1 \
        and df["DEMA_Short"].iloc[-1] < df["DEMA_Long"].iloc[-1] \
        and df["RSI"].iloc[-1] > 75 \
        and df["EMA12"].iloc[-1] < df["EMA50"].iloc[-1]:
        logging.info(f"Current_position {current_position} {pair_or_coin}, DEMA_Short: {df['DEMA_Short'].iloc[-1]}, DEMA_Long: {df['DEMA_Long'].iloc[-1]}")
        # For now, do market order if spread is < 0.001
        if spread.iloc[-1] < 0.001:
            # SELL at market order. SELL will close entire position for simplicity
            last_price = df["LastPrice"].iloc[-1]
            if last_price*current_position >= coin_info['MiniOrder']:
                logging.info(f"Sending SELL Order for {pair_or_coin} with quantity {current_position}")
                order = place_order(pair_or_coin, "SELL", current_position)
                logging.info(f"Order info: {order}")
                try:
                    quantity_buy = order["OrderDetail"]["Quantity"]
                    price = order["OrderDetail"]["Price"]
                    update_pfo(order["OrderDetail"]["Pair"])
                except:
                    logging.error(order["ErrMsg"])
        else:
            if pending_orders:
                logging.info(f"[SELL] Pending Orders: {pending_orders}")
                for order in pending_orders:
                    if abs(pending_orders[0]['Price'] - (ma20 + 1.5*atr)) / mid_spread > 0.05:
                        cancel_order(order["OrderID"], pair_or_coin)
                        limit_price = ma20 + 1.5 * atr
                        if abs(limit_price - mid_spread)/mid_spread < 0.10:  # 不偏离现价过多
                            limit_price *= (1 + np.random.uniform(-0.001, 0.001))  # 避免整数关口
                            # 挂单
                            if limit_price*current_position >= coin_info['MiniOrder']:
                                order = place_order(pair_or_coin, "SELL", current_position, price=limit_price, order_type="LIMIT")
                                try:
                                    add_pending_orders(order["OrderDetail"], "./pending_orders.csv")
                                except:
                                    logging.error(order["ErrMsg"])
            # else:
            #     limit_price = ma20 + 1.5 * atr
            #     if abs(limit_price - mid_spread)/mid_spread < 0.10:  # 不偏离现价过多
            #         limit_price *= (1 + np.random.uniform(-0.001, 0.001))  # 避免整数关口
            #         # 挂单
            #         order = place_order(pair_or_coin, "SELL", current_position, price=limit_price, order_type="LIMIT")
            #         add_pending_orders(order["OrderDetail"], "./pending_orders.csv")


    else:
        pass
        # Continue to hold
        # Append position as NaN in our portfolio CSV since no BUY/SELL action taken
        # current_time = int(time.time() * 1000)  # 当前时间，单位毫秒
        # max_pending_duration = 60 * 60 * 1000  # 最大挂单时间 1 小时（可调）

        # for order in pending_orders:
        #     order_age = current_time - order["CreateTimestamp"]
        #     # 计算目标价格（滚动支撑/阻力）
        #     if order['Side'] == "BUY":
        #         target_price = ma20 - 0.5 * atr
        #     elif order['Side'] == "SELL":
        #         target_price = ma20 + 0.5 * atr
        #     else:
        #         continue
        #     # 判断是否需要撤单：价格偏离过大 或 存在时间过长
        #     price_deviation = abs(order['Price'] - mid_spread) / mid_spread
        #     if price_deviation > 0.05 or order_age > max_pending_duration:
        #         cancel_order(order_id=order['OrderID'])
        #         print(f"Cancelled pending {order['Side']} order {order['OrderID']} due to deviation/time")

def remove_pending_orders(orders):
    if orders["Success"] == False:
        return
    for order in orders["OrderMatched"]:
        order_id = order["OrderID"]
        pair = order["Pair"]
        status = order["Status"]
        if status == "PENDING":
            logging.info(f"Order {order_id} has been cancelled")
            cancel_order(order_id, pair)

def update_pfo(pair = None, csv_file = "./portfolio.csv"):
    balance = get_balance()
    df = pd.read_csv(csv_file)
    all_coins = balance['SpotWallet']
    if pair:
        df.loc[df["Pair"] == pair.replace('/USD',''), "Quantity"] = all_coins[pair.replace('/USD','')]['Free']
    for coin in all_coins:
        curr_coin_bal = all_coins[coin]['Free']
        if curr_coin_bal <= 0:
            continue
        df.loc[len(df)] = [coin, curr_coin_bal]
    df.to_csv(csv_file, index = False)

def create_headers():
    logging.info("Creating necessary files")
    headers = ["Pair", "Quantity"]
    try:
        os.remove("./portfolio.csv")
    except FileNotFoundError:
        logging.error("./portfolio.csv not found. Continuing...")
    try:
        with open("./portfolio.csv", 'r') as f:
            pass
    except FileNotFoundError:
        append_to_csv("./portfolio.csv", headers)
    po_headers = ["OrderID"]
    try:
        with open("./pending_orders.csv") as f:
            pass
    except FileNotFoundError:
        append_to_csv("./pending_orders.csv", po_headers)
    pnl_headers = ["Pair", "Quantity", "PriceBought", "PriceSold", "FinishTimestamp", "PnL"]
    try:
        with open("./pnl.csv") as f:
            pass
    except FileNotFoundError:
        append_to_csv("./pnl.csv", pnl_headers)

def add_to_pnl(order, init_price, PnL, csv_file = "./pnl.csv"):
    df = pd.read_csv(csv_file)
    new_row_value = [order["Pair"], order["Quantity"], init_price, order["Price"], order["FinishTimestamp"], PnL]
    df.loc[len(df)] = new_row_value
    df.to_csv(csv_file, index = False)

    """
    query_order("BNB/USD", pending_only=False) returns:
    query_order returns:
    {
    "Success": true,
    "ErrMsg": "",
    "OrderMatched": [
        {"Pair": "BNB/USD",
            "OrderID": 2795709,
            "Status": "FILLED",
            "Role": "TAKER",
            "ServerTimeUsage": 0.004961759,
            "CreateTimestamp": 1774103089466,
            "FinishTimestamp": 1774103089471,
            "Side": "BUY",
            "Type": "MARKET",
            "StopType": "GTC",
            "Price": 645.27,
            "Quantity": 0.542,
            "FilledQuantity": 0.542,
            "FilledAverPrice": 645.27,
            "CoinChange": 0.542,
            "UnitChange": 349.73634,
            "CommissionCoin": "USD",
            "CommissionChargeValue": 0.349736,
            "CommissionPercent": 0.001,
            "OrderWalletType": "SPOT",
            "OrderSource": "PUBLIC_API"},
        {"Pair": "BNB/USD",
            "OrderID": 2782035,
            "Status": "FILLED",
            "Role": "TAKER",
            "ServerTimeUsage": 0.004498809,
            "CreateTimestamp": 1774076845620,
            "FinishTimestamp": 1774076845624,
            "Side": "SELL",
            "Type": "MARKET",
            "StopType": "GTC",
            "Price": 643,
            "Quantity": 0.14,
            "FilledQuantity": 0.14,
            "FilledAverPrice": 643,
            "CoinChange": 0.14,
            "UnitChange": 90.02,
            "CommissionCoin": "USD",
            "CommissionChargeValue": 0.09002,
            "CommissionPercent": 0.001,
            "OrderWalletType": "SPOT",
            "OrderSource": "PUBLIC_API"}
        ]
    }
    """
    # New implementation below (proposed)
    # order is a query_order object as per the comment above
    # remember to change the function create_headers() headers if we are using the below implementation for pnl folder
    # df = pd.read_csv(csv_file)
    # order_detail = order["OrderMatched"]
    # if order_detail["Side"] == "BUY":
    #     price_bought = order_detail["FilledAverPrice"] * order_detail["FilledQuantity"]
    #     price_sold = 0
    # elif order_detail["Side"] == "SELL":
    #     price_bought = 0
    #     price_sold = order_detail["FilledAverPrice"] * order_detail["FilledQuantity"]
    # PnL = price_sold - price_bought
    # new_row_value = [order_detail["OrderID"], order_detail["FinishTimestamp"], order_detail["Pair"], price_bought, price_sold, order_detail["Quantity"], PnL, order_detail["CommissionChargeValue"]]
    # df.loc[len(df)] = new_row_value
    # df.to_csv(csv_file, index = False)
    
def add_pending_orders(order, csv_file = "./pending_orders.csv", drop = False):
    df = pd.read_csv(csv_file)
    if drop:
        logging.info(f"Removed {order} to {csv_file}")
        drop_index = df[df["OrderID"] == order["OrderID"]].index
        df.drop(drop_index, inplace = True)
        df.to_csv(csv_file, index = False)
        if order["Status"] == "FILLED":
            update_pfo(order["Pair"])
    else:
        logging.info(f"Added {order} to {csv_file}")
        append_to_csv(order["OrderID"], csv_file)

async def poll_for_trades(ticker_list):
    for ticker in ticker_list:
        path = f"./ticker_csv/{ticker.replace('/','_')}.csv"
        if os.path.getsize(path) == 0:
            continue
        df = pd.read_csv(path)
        curr_cash = balance["SpotWallet"]["USD"]["Free"]
        threshold = 0.02
        buy_expenditure = min(curr_cash * threshold, 500)
        check_for_trades(df, ticker, curr_cash, buy_expenditure)

def query_pending_trades():
    df = pd.read_csv("./pending_orders.csv").iloc[:,0]
    if df.empty:
        return
    for key, value in df.items():
        order = query_order(value)["OrderMatched"][0]
        if order["Status"] == "CANCELED":
            add_pending_orders(order, "./pending_orders.csv", True)
        elif order["Status"] == "FILLED":
            add_pending_orders(order, "./pending_orders.csv", True)
            update_pfo(order["Pair"])
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
        compute_metrics(ticker_list),    # Always recompute DEMA        
        poll_for_trades(ticker_list)     # Check for trades
    )
    print(f"Finished at {time.strftime('%X')}")

def create_csvs(tickers):
    folder_path = "./ticker_csv/"
    os.makedirs(folder_path, exist_ok = True)
    for ticker in tickers:
        filepath = f"./{folder_path}/{ticker.replace('/','_')}.csv"
        try:
            if not os.path.exists(filepath):
                headers = ["Timestamp", "MaxBid", "MinAsk", "LastPrice", "Change", "CoinTradeValue", "UnitTradeValue"]
                append_to_csv(filepath, headers)
        except Exception as e:
            logging.error(f"Error processing {ticker}: {e}")

def remove_files():
    folder_path = './ticker_csv/'
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

    for file_path in csv_files:
        try:
            os.remove(file_path)
            logging.info(f"Removed {file_path} on startup")
        except Exception as e:
            logging.error(f"Error removing {file_path}: {e}")

# ------------------------------
# Quick Demo Section
# ------------------------------
if __name__ == "__main__":
    all_orders = query_order()

    logging.info("--- Remove all temporary data ---")
    remove_files()
    
    logging.info("--- Checking Current Balance ---")
    balance = get_balance()
    logging.info(balance)

    logging.info("--- Getting Exchange Info ---")
    info = get_exchange_info()
    logging.info(info)

    logging.info("--- Creating Ticker CSVs ---")
    ticker_list = list(info.get('TradePairs', {}).keys())
    create_csvs(ticker_list)   

    logging.info("--- Creating Utility CSVs ---")
    create_headers()

    logging.info("--- Updating Portfolio CSV ---")
    update_pfo()

    logging.info("--- Remove all pending trades ---")
    remove_pending_orders(all_orders)

    logging.info("--- Checking Server Time ---")
    logging.info(check_server_time())

    # if info:
    #     print(f"Available Pairs: {list(info.get('TradePairs', {}).keys())}")
    
    # logging.info("--- Running query_order ---")
    # logging.info(query_order(None, "CRV/USD"))

    # print("\n--- Getting Market Ticker (BTC/USD) ---")
    # ticker = get_ticker("BTC/USD")
    # if ticker:
    #     print(ticker.get("Data", {}).get("BTC/USD", {}))

    #print("\n--- Triggering place_order ---")
    #print(place_order("ADA/USD", "SELL", "3.5", "0.34", "LIMIT"))
    #print("\n--- Triggering query_order ---")
    #print([order["Status"] for order in query_order(None, "ADA/USD")["OrderMatched"]])
    while True:
        asyncio.run(main())

    # print("\n--- Checking Pending Orders ---")
    # print(get_pending_count())

    # # Uncomment these to test trading actions:
    # # print(place_order("BTC", "BUY", 0.01, price=95000))  # LIMIT
    # print(place_order("BNB/USD", "BUY", 1))      
    # print(place_order("BNB/USD", "SELL", 1))             # MARKET       
    # print(query_order(pair="BNB/USD", pending_only=False))
    # print(cancel_order(pair="BNB/USD"))

