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
# Metrics Computation
# ------------------------------

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

def calculate_ATR_stdev(df, column, time_period):
    r_t = (df[column] / df[column].shift(1)) - 1
    ATR = r_t.rolling(window=time_period).std()
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

# ------------------------------
# Main Trading Logic
# ------------------------------

def trailing_stop_loss(pair_usd, quantity, price, csv_file = './pending_orders.csv'):
    df = pd.read_csv(csv_file)
    pair = pair_usd.replace('/USD','')
    curr_buy = df.loc[(df["Pair"] == pair) & (df["Type"] == "MARKET")] 
    # If we are holding a buy position
    if not curr_buy.empty: 
        tp_price = df.loc[(df["Pair"] == pair) & (df["Side"] == "BUY")]["TPPrice"].iloc[0] 
        sl_price = df.loc[(df["Pair"] == pair) & (df["Side"] == "BUY")]["SLPrice"].iloc[0]
        get_orders = query_order(None, pair_usd, None)
        if get_orders['Success'] == True:
            market_pendingorders = list(filter(lambda x: x['Status'] == 'PENDING', get_orders['OrderMatched']))
        else:
            market_pendingorders = []
        # If there are no pending orders, we create our TP Order
        if market_pendingorders == []:
            new_sell_limit = place_order(pair_usd, 'SELL', quantity, tp_price)
            add_to_pending_orders(new_sell_limit, pair)
            logging.info(f"[LIMIT SELL] {pair} No current pending orders, created {new_sell_limit}")
        # There is a pending order
        elif market_pendingorders:
            recreate_flag = False
            # Check if the order has the correct TP Price. If not, cancel and recreate
            for order in market_pendingorders: 
                logging.info(f"{pair} Pending order: {order}")
                if order['Price'] != tp_price:
                    recreate_flag = True
                    cancel_order(order['OrderID'], pair)
                    if not df.loc[df['OrderID'] == order['OrderID']].empty:
                        df = df.drop(df[df['OrderID'] == order['OrderID']].index)
                        df.to_csv(csv_file, index = False)
                        logging.info(f"{pair} Removed stale limit sell order {order['OrderID']} from pending orders csv.")
            time.sleep(0.1)
            if recreate_flag:
                new_sell_limit = place_order(pair_usd, 'SELL', quantity, tp_price)
                logging.info(f"{pair} Added new sell limit order {new_sell_limit['OrderDetail']['OrderID']} to pending orders csv")
                logging.info(f"{pair} [LIMIT SELL] New order: {new_sell_limit}")
                add_to_pending_orders(new_sell_limit, pair)
        # At this point, there is only one PENDING SELL LIMIT Order. 
        # If the current price drops below stop loss - Cancel all pending orders, then market sell. 
        if price < sl_price:
            for order in market_pendingorders:
                cancel_order(order['OrderID'], pair)
                df = df.drop(df[df['OrderID'] == order['OrderID']].index)
                df.to_csv(csv_file, index = False)
            time.sleep(0.1)
            try:
                stop_loss_order = place_order(pair_usd, 'SELL', quantity)
                add_to_orders_and_pnl(stop_loss_order)
                update_pfo(pair)
            except:
                logging.error(f"{pair} Unable to place SL order: {stop_loss_order}")
        
        # Revise SL Price 
        elif price > df[(df["Pair"] == pair) & (df["Side"] == "BUY")]['PriceBought'].iloc[0]:
            curr_SLPrice = df[(df["Pair"] == pair) & (df["Side"] == "BUY")]['SLPrice'].iloc[0]
            new_SLPrice = round(max(curr_SLPrice, price * 0.996), info['TradePairs'][pair_usd]['PricePrecision'])
            df.loc[(df["Pair"] == pair) & (df["Side"] == "BUY"), 'SLPrice'] = new_SLPrice
            logging.info(f"{pair} Revising stop loss price from {curr_SLPrice} to {new_SLPrice}")
            df.to_csv(csv_file, index = False)

        # If the current price is above SL, we check if the TP order was taken
        if get_orders['Success'] == True:
            filled_orders = list(filter(lambda x: x['Status'] == 'FILLED', get_orders['OrderMatched']))
        else:
            filled_orders = []
        # If it has been filled, we will update pending_orders.csv
        # If it has not been filled, we ignore it
        mask = (df["Pair"] == pair) & (df["Status"] == "PENDING")
        if not df[mask].empty:
            target_order_id = df[mask]['OrderID'].iloc[-1]
            target_order = list(filter(lambda x: x['OrderID'] == target_order_id, filled_orders))
            # If the pending order id exists in the filled orders.
            # This means the pending order has been filled. 
            # Remove order details from pending_orders.csv + update orders and pnl
            if target_order:
                df = df.drop(df[df['Pair'] == pair].index)
                df.to_csv(csv_file, index = False)
                add_to_orders_and_pnl(target_order[0])
                logging.info(f"{pair} [LIMIT SELL] Limit sell order triggered. Added order to pnl csv: {target_order[0]}.")
    return None

def check_for_trades(df, pair_or_coin, curr_cash, buy_expenditure):
    # TODO Iterate through CSV. (Added: CSV should be maintained at (2*long_time_period-1) rows)
    # Check if we take a trade - To check past information in CSV to see if the last price is above / lower EMA bounds
    rows = 59
    if len(df) < rows: # check function on compute_metrics for number, rows
        return 
    balance = get_balance()
    # Only trade coins that fulfill these conditions. 
    if df["UnitTradeValue"].iloc[-1] < 10000000:
        clear_coin(pair_or_coin, balance)
        return 
    if df['LastPrice'].iloc[-1] < 1e-4:
        clear_coin(pair_or_coin, balance)
        return 
    spread = df["MaxBid"] - df["MinAsk"]
    mid_spread = (df["MaxBid"] + df["MinAsk"]) / 2
    quantity_buy = buy_expenditure / mid_spread.iloc[-1]
    coin_info = info['TradePairs'][pair_or_coin]
    quantity_buy_market = round(quantity_buy, coin_info['AmountPrecision'])  # 30% for market order    
    try:
        curr_position = max(balance['SpotWallet'][pair_or_coin.replace('/USD','')]['Free'], balance['SpotWallet'][pair_or_coin.replace('/USD','')]['Lock'])
        curr_cash = balance['SpotWallet']["USD"]["Free"]
        current_position = round(curr_position, coin_info['AmountPrecision'])
    except:
        logging.error(f"Failed to get balance: {balance}")
        return None

    last_price = df["LastPrice"].iloc[-1]

    if last_price * current_position >= coin_info['MiniOrder']:
        logging.info(f"{pair_or_coin}: last_price {last_price} curr_position {current_position} coin_info {coin_info['MiniOrder']}")
        trailing_stop_loss(pair_or_coin, current_position, last_price)

    # Example: if mid_spread = 10000, and buy_expenditure is $100, then we buy 100/10000 units
    # Current_position condition updated to current_position < 0.1 due to floating point error in the get_balance
    if last_price * current_position < coin_info['MiniOrder'] and calculate_signal(df, 3) > 0.7 \
        and curr_cash > buy_expenditure:
        logging.info(f"[BUY] Signal = {calculate_signal(df, 3)} --- Current_position {current_position} {pair_or_coin}, DEMA_Short: {df['DEMA_Short'].iloc[-1]}, DEMA_Long: {df['DEMA_Long'].iloc[-1]}, curr_cash: {curr_cash}, buy_exp: {buy_expenditure}")

        if spread.iloc[-1] < 0.001:
            # BUY at market order, followed by immediately updating portfolio
            logging.info(f"[BUY] Sending Order for {pair_or_coin} with quantity {quantity_buy_market}")
            order = place_order(pair_or_coin, "BUY", quantity_buy_market)
            pair = pair_or_coin.replace('/USD','') 
            logging.info(f"[BUY] Order info: {order}")
            add_to_pending_orders(order, pair)
            try:
                time.sleep(0.1)
                limit_sell = place_order(pair_or_coin, \
                                         'SELL', \
                                         round(quantity_buy_market, coin_info['AmountPrecision']), \
                                         round(1.014 * order['OrderDetail']['FilledAverPrice'], coin_info['PricePrecision']))
                logging.info(f"[LIMIT SELL] Place limit sell: {limit_sell}")
                add_to_pending_orders(limit_sell, pair)
                update_pfo(order["OrderDetail"]["Pair"])
                add_to_orders_and_pnl(order)
            except:
                logging.error(f"[BUY] Triggered buy at market: {order}")

def calculate_signal(df, num_indicators):
    signal = 0 
    total_indicators = num_indicators
    # Indicators
    if df["DEMA_Short"].iloc[-1] < df["DEMA_Long"].iloc[-1] and df["DEMA_Short"].iloc[-2] > df["DEMA_Long"].iloc[-2]:
        signal += 0
    elif df["DEMA_Short"].iloc[-1] > df["DEMA_Long"].iloc[-1] and df["DEMA_Short"].iloc[-2] < df["DEMA_Long"].iloc[-2]:
        signal += 1
    else:
        signal += 0.5
    if df["RSI"].iloc[-1] > 80:
        signal += 0
    elif df["RSI"].iloc[-1] < 20:
        signal += 1
    else:
        signal += 0.5
    if df["EMA12"].iloc[-1] < df["EMA50"].iloc[-1] and df["EMA12"].iloc[-2] > df["EMA50"].iloc[-2]:
        signal += 0
    elif df["EMA12"].iloc[-1] > df["EMA50"].iloc[-1] and df["EMA12"].iloc[-2] < df["EMA50"].iloc[-2]:
        signal += 1
    else:
        signal += 0.5
    return round(signal / total_indicators, 2)

# ------------------------------
# Helper Functions
# ------------------------------

def update_pfo(pair = None, csv_file = "./portfolio.csv"):
    balance = get_balance()
    df = pd.read_csv(csv_file)
    all_coins = balance['SpotWallet']
    if pair:
        df.loc[df["Pair"] == pair.replace('/USD',''), "Quantity"] = all_coins[pair.replace('/USD','')]['Free']
    else:
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
    po_headers = ["Pair", "OrderID", "PriceBought", "SLPrice", "TPPrice", "Quantity", "Status", "Side", "Type"]
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
    if not os.path.isfile("./orders.csv"):
        open("./orders.csv", 'a').close()
    
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

def remove_csv_files(csv_path):
    csv_files = glob.glob(os.path.join(csv_path, "*.csv"))
    for file_path in csv_files:
        try:
            os.remove(file_path)
            logging.info(f"Removed {file_path} on startup")
        except Exception as e:
            logging.error(f"Error removing {file_path}: {e}")
    
def add_to_orders_and_pnl(order, order_file = "./orders.csv", pnl_file = "./pnl.csv"):
    try:
        order_info = order["OrderDetail"]
    except:
        try:
            order_info = order
            test_direction = order['Side']           
        except:
            logging.error(f"Order {order_info} threw error")
            return None
    if order_info["Status"] != "FILLED":
        logging.info(f"Order not added to orders and pnl csv due to status {order_info['Status']}")
        return
    order_info['Direction'] = 1 if order_info['Side'] == "BUY" else -1
    order_info['MoneySpentOnTrade'] = order_info["Price"] * order_info["Quantity"] * order_info["Direction"] + order_info["CommissionChargeValue"]
    logging.info(f"ORDER INFO: {order_info}")
    order_df = pd.read_csv(order_file)
    new_row = pd.DataFrame([order_info])
    order_df = pd.concat([order_df, new_row], ignore_index = True)
    order_df.to_csv(order_file, index = False)

    pnl_df = pd.read_csv(pnl_file)
    ticker = order_info['Pair']
    if not pnl_df.loc[pnl_df['Pair'] == ticker, 'MoneySpentOnTrade'].empty:
        pnl_df.loc[pnl_df['Pair'] == ticker, 'MoneySpentOnTrade'] += order_info['MoneySpentOnTrade']
    else:
        pnl_df.loc[len(pnl_df)] = [ticker, order_info['MoneySpentOnTrade']]
    logging.info(f"Total Spent on {order_info['Pair']} = {pnl_df[pnl_df['Pair'] == ticker]['MoneySpentOnTrade'].iloc[0]}")
    pnl_df.to_csv(pnl_file, index = False)

def create_orders(balance_wallet, csv_file = "./orders.csv"):
    for pair in balance_wallet:
        if pair == "USD":
            continue
        try:
            orders = query_order(None, f"{pair}/USD")
            if not os.path.getsize(csv_file):
                df = pd.json_normalize(orders['OrderMatched'])
                df["Direction"] = df.apply(lambda x: 1 if x["Side"] == "BUY" else -1, axis = 1)
                df["MoneySpentOnTrade"] = df["Price"] * df["Quantity"] * df["Direction"] + df["CommissionChargeValue"]
                df = df[::-1]
                df.to_csv(csv_file, index = False)
            else:
                init_df = pd.read_csv(csv_file)
                df = pd.json_normalize(orders['OrderMatched'])
                df["Direction"] = df.apply(lambda x: 1 if x["Side"] == "BUY" else -1, axis = 1)
                df["MoneySpentOnTrade"] = df["Price"] * df["Quantity"] * df["Direction"] + df["CommissionChargeValue"]
                df = df[::-1]
                result = pd.concat([init_df, df], ignore_index = True)
                result.to_csv(csv_file, index = False)
        except:
            logging.error(f"Unable to query {pair}: {orders}")
            pass
        if balance_wallet[pair]['Lock'] and df.iloc[-1]['Side'] == "SELL":
            pair_usd = pair + "/USD"
            last_filled = (df['Pair'] == pair_usd) & (df['Status'] == 'FILLED')
            last_pending = (df['Pair'] == pair_usd) & (df['Status'] == 'PENDING')
            add_to_pending_orders(df.loc[last_filled].iloc[-1], pair)
            add_to_pending_orders(df.loc[last_pending].iloc[-1], pair)
        elif balance_wallet[pair]['Free'] and df.loc[df["Status"] == "FILLED"]['Side'].iloc[-1] == "BUY":
            add_to_pending_orders(df.loc[df["Status"] == "FILLED"].iloc[-1], pair)
    logging.info(f"--- Orders.csv successfully created ---")

def add_to_pending_orders(input_order, pair, remove = False):
    # pair in the input is BTC (without /USD)
    try:
        order = input_order['OrderDetail']
    except:
        try:
            order = input_order 
            test_direction = order['Side'] 
        except:
            logging.error(f"Order failed to add: {order}")
            return None
    po_csv_file = "./pending_orders.csv"
    po_csv = pd.read_csv(po_csv_file)  
    if remove:
        po_csv = po_csv[~(po_csv["Pair"] == pair)]
        po_csv.to_csv(po_csv_file, index = False)
    else:
        last_buy_id = order['OrderID'] 
        last_buy_status = order['Status']
        if last_buy_status == 'PENDING':
            last_buy_price = order['Price']
            last_buy_quantity = order['Quantity']
        else:
            last_buy_price = order['FilledAverPrice']
            last_buy_quantity = order['FilledQuantity']
        last_buy_side = order['Side']
        last_buy_type = order['Type']
        stop_loss_price = round(0.996 * last_buy_price, info['TradePairs'][pair + "/USD"]['PricePrecision'])
        take_profit_price = round(1.014 * last_buy_price, info['TradePairs'][pair + "/USD"]['PricePrecision'])
        po_csv.loc[len(po_csv)] = [pair, last_buy_id, last_buy_price, stop_loss_price, take_profit_price, last_buy_quantity, last_buy_status, last_buy_side, last_buy_type]
        po_csv.to_csv(po_csv_file, index = False) 
        logging.info(f"Added targets for {pair} bought --- SL: {stop_loss_price}, TP: {take_profit_price}")

def create_pnl():
    pnl = 0
    df = pd.read_csv("./orders.csv")
    summed_df = df[df['Status'] == 'FILLED'].groupby(["Pair"])["MoneySpentOnTrade"].sum()
    for val in summed_df:
        pnl -= val
    logging.info(f"Total PnL on starting: {pnl}")
    summed_df.to_csv("./pnl.csv")

def clear_coin(input_pair, balance):
    pair = input_pair.replace('/USD','')
    pair_usd = pair + '/USD'
    if pair == 'USD':
        return
    if balance['SpotWallet'][pair]['Free'] > 0:
        flat_pos = place_order(pair_usd, 'SELL', balance['SpotWallet'][pair]['Free'])
        logging.info(f"[MARKET SELL {pair}] {flat_pos}")
    elif balance['SpotWallet'][pair]['Lock'] > 0:
        pending = list(filter(lambda x: x['Status'] == 'PENDING', query_order(None, pair_usd)['OrderMatched']))
        cancel_pending = cancel_order(pending[0]['OrderID'])
        logging.info(f"[CANCEL {pair}] {cancel_pending}")
        time.sleep(0.1)
        flat_pos = place_order(pair_usd, 'SELL', balance['SpotWallet'][pair]['Lock'])
        logging.info(f"[MARKET SELL {pair}] {flat_pos}")



# ------------------------------
# Async Functions
# ------------------------------

async def process_ticker(ticker):
    time.sleep(0.1)
    time_now = _get_timestamp()

    try:
        # 把同步函数变成异步执行（线程池）
        tdata = await asyncio.to_thread(get_ticker, ticker)
        file_name = ticker.replace("/", "_")
        path = f"./ticker_csv/{file_name}.csv"

        if tdata and tdata.get("Success") is True:
            info = tdata.get("Data", {}).get(ticker, {})
        else:
            logging.info(f"Failed to get data for {ticker}")
            if os.path.exists(path):
                return
            else:
                headers = ["Timestamp", "MaxBid", "MinAsk", "LastPrice", "Change", "CoinTradeValue", "UnitTradeValue"]
                append_to_csv(path, headers)
        
        # 如果文件不存在 → 写header
        if not os.path.exists(path):
            headers = ["Timestamp"] + list(info.keys())
            append_to_csv(path, headers)

        # 写数据
        upload_info = [time_now] + list(info.values())
        append_to_csv(path, upload_info)

    except Exception as e:
        logging.error(f"Error processing {ticker}: {e}")

async def tickers_to_csv(ticker_list):
    tasks = [process_ticker(ticker) for ticker in ticker_list]
    # 并发执行
    await asyncio.gather(*tasks)

async def compute_metrics(ticker_list, short=10, long=30):
    # Compute the DEMA 
    # Convert csv to pandas dataframe for computation of DEMA
    # Remove earliest row if length exceeds 2*long_period - 1
    period_DS = 10
    period_DL = 30
    period_MA = 20
    period_ATR = 20
    period_RSI = 14
    period_EMAS = 12
    period_EMAL = 50
    period_ATR_stdev = 14
    StopLoss_factor = 3
    TakeProfit_factor = 3.5

    for ticker in ticker_list: 
        path = f"./ticker_csv/{ticker.replace('/', '_')}" + ".csv"
        if os.path.getsize(path) == 0:
            continue
        df = pd.read_csv(path)
        rows = max(period_DS, 2*period_DL - 1, period_MA, period_ATR, period_RSI, period_EMAS, period_EMAL, period_ATR_stdev)
        if len(df) >= rows:
            df["DEMA_Short"] = calculate_double_EMA(df, period_DS, "LastPrice")
            df["DEMA_Long"] = calculate_double_EMA(df, period_DL, "LastPrice")
            df["MA"] = calculate_MA(df, period_MA, "LastPrice")
            df["ATR"] = calculate_ATR(df, period_ATR, "LastPrice")
            df["RSI"] = calculate_RSI(df, "LastPrice", period_RSI) # RSI<20 BUY, RSI>80 SELL
            df["EMA12"] = calculate_EMA(df, "LastPrice", period_EMAS) # EMA12>EMA50 BUY, else SELL
            df["EMA50"] = calculate_EMA(df, "LastPrice", period_EMAL)
            df["ATRstdev"] = calculate_ATR_stdev(df, "LastPrice", period_ATR_stdev)
            df["DynamicStopLoss"] = StopLoss_factor * df["ATRstdev"]
            df["DynamicTakeProfit"] = TakeProfit_factor * df["ATRstdev"]
        while len(df) > rows:
            df = df.iloc[1:]
        df.to_csv(path, index=False) # Update the csv

async def poll_for_trades(ticker_list):
    for ticker in ticker_list:
        path = f"./ticker_csv/{ticker.replace('/','_')}.csv"
        if os.path.getsize(path) == 0:
            continue
        df = pd.read_csv(path)
        curr_cash = balance["SpotWallet"]["USD"]["Free"]
        threshold = 0.05
        buy_expenditure = min(curr_cash * threshold, 25000)
        check_for_trades(df, ticker, curr_cash, buy_expenditure)

async def minute_data():
    time.sleep(55)

async def main():
    """
    For each ticker, do (while True:)
    1. Open CSV of the ticker + Calculate DEMA 
    2. Populate CSV with data (see function tickers_to_csv or see the csv that has been populated)
    3. Compute the DEMA for that CSV while it is still open
        a. Also, ensure there are exactly 2*long_period - 1 rows in the CSV 
        b. In particular, add the latest data, followed by removing the earliest row, then compute the DEMA columns
     """
    try:
        print(f"Started at {time.strftime('%X')}")
        info = get_exchange_info()
        ticker_list = list(info.get('TradePairs', {}).keys())
        await asyncio.gather(
            tickers_to_csv(ticker_list),     # Always keep getting exchange data
            compute_metrics(ticker_list),    # Always recompute DEMA        
            poll_for_trades(ticker_list),     # Check for trades
            minute_data()
        )
        print(f"Finished at {time.strftime('%X')}")
    except Exception as e:
        logging.error(f"Error: {e}")

# ------------------------------
# Quick Demo Section
# ------------------------------
if __name__ == "__main__":
    all_orders = query_order()

    logging.info("--- Remove all temporary data ---")
    # remove_csv_files("./ticker_csv/")
    remove_csv_files("./")
    
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

    logging.info("--- Creating Orders CSV ---")
    create_orders(balance['SpotWallet'])

    logging.info("--- Creating PnL CSV ---")
    create_pnl()

    logging.info("--- Checking Server Time ---")
    logging.info(check_server_time())

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