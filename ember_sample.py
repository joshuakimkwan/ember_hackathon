import requests
import time
import hmac
import hashlib
import csv
import pandas as pd

import numpy as np
import matplotlib.pyplot as plt


# --- API Configuration ---
BASE_URL = "https://mock-api.roostoo.com"
# General Portfolio Testing API
API_KEY = "VEl4HZ01sGMbCyr48He5r7CGhTIgaoUj4IeEHpFdb77qyJIOb3YlsdQm681AJs6A"      # Replace with your actual API key
SECRET_KEY = "7ue4oQRfdkGu4bhRXBmkbjA5iO7fTY4Zdaz6l0XGT6mXvNiYQqpiz4mWPVriU4Wo"  # Replace with your actual secret key
# R1 Competition API
# API_KEY = "KPmLBKLYVEmPgRsiyYkN33UY5KlLPv1Qi7ykUJAvqEB5Fj888IiALZncN1YlwmO4"
# SECRET_KEY = "h7rT1aw2MiHgCgOt2Hu0crUZy1kSG6oho4UMMcgRUveMvIQ3H3B7ivla16krAegj"

# ------------------------------
# Utility Functions
# ------------------------------

def append_to_csv(file_path, row_data):
    with open(file_path, mode='a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(row_data)

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

def tickers_to_csv(ticker_list):
    # TODO Change this to run asynchronously 
    time_now = _get_timestamp()
    for ticker in ticker_list:
        tdata = get_ticker(ticker)

        if tdata and tdata["Success"] == True:
            info = tdata.get("Data", {}).get(ticker, {})
        else:
            print(f"Failed to get data for {ticker}")
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

def calculate_double_EMA(df, time_period, column):
    # df refers to the csv being opened at that point in time, which will be accessed via pandas df
    EMA = df[column].ewm(span=time_period, adjust=False).mean()
    DEMA = 2*EMA - EMA.ewm(span=time_period, adjust=False).mean()
    return DEMA

def create_double_EMA_columns(df, column, short_time_period=20, long_time_period=50):
    df["DEMA_Short"] = calculate_double_EMA(df, short_time_period, column)
    df["DEMA_Long"] = calculate_double_EMA(df, long_time_period, column)
    return None

def check_for_trades(df, portfolio, pair_or_coin, curr_cash, buy_expenditure):
    # TODO Iterate through CSV. (Added: CSV should be maintained at (2*long_time_period-1) rows)
    # Check if we take a trade - To check past information in CSV to see if the last price is above / lower EMA bounds
    spread = df["MaxBid"] - df["MinAsk"]
    mid_spread = (df["MaxBid"] + df["MinAsk"]) / 2
    quantity_buy = buy_expenditure / mid_spread
    current_position = portfolio.loc[portfolio["Ticker"] == pair_or_coin, "Curr_qty_holding"].values
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
            place_order(pair_or_coin, "BUY", quantity_buy)
            update_orders(pair_or_coin, "BUY", quantity_buy)
            update_portfolio(pair_or_coin, "BUY", current_position, "MARKET", price, PnL) # TODO Use API to get current price and compute PnL
            df["indicator"][1] = True
        else:
            # BUY at limit order
            place_order(pair_or_coin, "BUY", quantity_buy, price=mid_spread)
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

        # For now, do market order if spread is < 0.001
        if spread < 0.001:
            # SELL at market order. SELL will close entire position for simplicity
            place_order(pair_or_coin, "SELL", current_position)
            update_orders(pair_or_coin, "SELL", quantity_buy)
            update_portfolio(pair_or_coin, "SELL", current_position, "MARKET", price, PnL) # TODO Use API to get current price and compute PnL
            df["indicator"][1] = False

        else:
            # SELL at limit order
            place_order(pair_or_coin, "SELL", current_position, price=mid_spread)
            # Query order to see if we can place a buy
        
        # Change indicator to True, indicating a position is now buy
        

    else:
        # Continue to hold
        # Append position as NaN in our portfolio CSV since no BUY/SELL action taken
    
    # Submit post request if we take a trade
    # Add the orders to an orders.csv
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

def update_portfolio(pair_or_coin, side, quantity, transaction_type, price, PnL): # TODO Parameters WIP
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
                headers = ["Ticker_name", "Timestamp", "Transaction_type", "Price", "Quantity", "Side", "PnL", "Commission"]
                append_to_csv(portfolio_file, headers)

                # After headers are written, append the actual data row
                timestamp = _get_timestamp()
                data_row = [pair_or_coin, timestamp, transaction_type, price, quantity, side, PnL, commission]
                append_to_csv(portfolio_file, data_row)
            else:
                # File is not empty and has headers, append a new row
                timestamp = _get_timestamp()
                data_row = [pair_or_coin, timestamp, transaction_type, price, quantity, side, PnL, commission]
                append_to_csv(portfolio_file, data_row)
    except FileNotFoundError:
        headers = ["Ticker_name", "Timestamp", "Transaction_type", "Price", "Quantity", "Side", "PnL", "Commission"]
        append_to_csv(portfolio_file, headers)
        timestamp = _get_timestamp()
        data_row = [pair_or_coin, timestamp, transaction_type, price, quantity, side, PnL, commission]
        append_to_csv(portfolio_file, data_row)

# ------------------------------
# Quick Demo Section
# ------------------------------
if __name__ == "__main__":
    print("\n--- Checking Server Time ---")
    print(check_server_time())

    print("\n--- Getting Exchange Info ---")
    info = get_exchange_info()
    if info:
        print(f"Available Pairs: {list(info.get('TradePairs', {}).keys())}")

    print("\n--- Getting Market Ticker (BTC/USD) ---")
    ticker = get_ticker("BTC/USD")
    if ticker:
        print(ticker.get("Data", {}).get("BTC/USD", {}))

    while True:
        tickers_to_csv(list(info.get('TradePairs', {}).keys()))
        check_for_trades()

        time.sleep(1)



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

