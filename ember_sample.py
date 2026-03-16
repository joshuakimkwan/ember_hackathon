import requests
import time
import hmac
import hashlib
import csv
import pandas as pd

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

def check_for_trades():
    # TODO Iterate through CSV
    # Check if we take a trade - To check past information in CSV to see if the last price is above / lower EMA bounds
    # For now, do market order if spread is < 0.001
    # Else, we do limit orders
    # Submit post request if we take a trade
    # place_order("BNB/USD", "BUY", 1)
    # Add the orders to an orders.csv
    update_orders()
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

def update_portfolio(): # TODO Parameters WIP
    # TODO To add/remove successful orders into portfolio
    # Close entire position for simplicity, can advance this next time
    # If a successful order is removed, we want to update our PnL / balance (this might have a function)
    portfolio_file = "./portfolio.csv"
    try:
        with open(portfolio_file, 'r') as f:
            pass
    except FileNotFoundError:
        headers = [] # Headers TBD
        append_to_csv(portfolio_file, headers)

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

