import time
import os
import argparse
import pyotp
import datetime
import math
from jugaad_trader import Zerodha
from jugaad_data.nse import NSELive
import pandas as pd
import numpy as np
import csv
import requests
import re
import json
import concurrent.futures
import holidays

base_url = "https://kite.zerodha.com"
login_url = "https://kite.zerodha.com/api/login"
twofa_url = "https://kite.zerodha.com/api/twofa"
instruments_url = "https://api.kite.trade/instruments"
equities_csv = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
proxy_url = "http://87.254.212.120:8080"
CREDENTIALS_FILE = "zerodha_credentials.json"
LOCAL_EQUITIES_FILE = "equities_local.csv"
FILENAME_CURRENT = "currentStocks.csv"
CURRENT_PORTFOLIO = os.path.abspath(
    r'C:\Users\shrrao\OneDrive - Nokia\Misc\Downloads\Finance\Smallcase'
    fr'\{FILENAME_CURRENT}')
PRECISION = 3
MAX_THREADS = 8
TICKER_NAME_MAX = 25
MIN_MARKET_CAP = 2000.0
MIN_TRADING_DAYS_DATA = 4
WEIGHT_1_MONTH = 0.27
WEIGHT_3_MONTH = 0.33
WEIGHT_6_MONTH = 0.23
WEIGHT_12_MONTH = 0.17

report_date = datetime.datetime.now()


def set_proxies(args):
    if args.proxy:
        print(f'Setting proxy server {proxy_url}')
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["http_proxy"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        os.environ["https_proxy"] = proxy_url
    else:
        os.environ["HTTP_PROXY"] = ""
        os.environ["http_proxy"] = ""
        os.environ["HTTPS_PROXY"] = ""
        os.environ["https_proxy"] = ""


def login():
    with open(CREDENTIALS_FILE, 'r') as creds_file:
        creds = json.load(creds_file)
    kite.user_id = creds['user_id']
    kite.password = creds['password']
    kite.twofa = pyotp.TOTP(creds['totp_key']).now()
    try:
        response_kite_login = kite.login()
    except Exception as e:
        print(f'Exception: {e}')
        return False
    else:
        return response_kite_login['status'] == 'success'


def nse_equities_from_zerodha():
    global nse_instruments
    nse_instruments = kite.instruments(kite.EXCHANGE_NSE)
    # with open("equities.csv", 'w', newline='') as f:
    #     csvwriter = csv.DictWriter(f, nse_instruments[0].keys())
    #     csvwriter.writerows(nse_instruments)
    print(f'Number of instruments: {len(nse_instruments)}')
    # for instrument in nse_instruments:
    #     if instrument['lot_size'] == 1:
    #         nse_equities_list.append(instrument)
    #         # print(instrument['tradingsymbol'])
    # print(f'Number of equity instruments {len(nse_equities_list)}')


def set_date_range(number_of_days):
    start_date = (report_date - datetime.timedelta(days=number_of_days))
    return start_date, report_date


def get_historical_data(ticker, days):
    from_date, to_date = set_date_range(days)
    retry_count = 3
    while retry_count:
        try:
            return kite.historical_data(instrument_token=ticker, from_date=from_date, to_date=to_date, interval='day')
        except Exception:
            retry_count -= 1
            time.sleep(0.5)
    return []


def calculate_trading_days():
    for interval in trading_days.keys():
        if interval == '1M':
            number_of_days = 31
        elif interval == '3M':
            number_of_days = 92
        elif interval == '6M':
            number_of_days = 184
        elif interval == '12M':
            number_of_days = 366
        else:
            print(f"Invalid interval {interval}")
            return 0

        from_date, to_date = set_date_range(number_of_days)
        days = np.busday_count(from_date.date(), to_date.date(), holidays=list(
            holidays.country_holidays(country="IN", years=[from_date.year, to_date.year]).keys()))
        trading_days[interval]['open'] = days + 1
        trading_days[interval]['start_date'] = from_date
        print(f'{interval} = {days} days')


def are_trading_days_range_within_range(ticker, ticker_data, interval):
    days = trading_days[interval]['open']
    max_delta = trading_days[interval]['max_delta']
    from_date = trading_days[interval]['start_date']
    date_in_ticker_data = ticker_data[-days]['date'].date()
    delta = abs((date_in_ticker_data - from_date.date()).days)
    if delta > max_delta:
        print(f'{ticker}: Date difference of {interval} is {delta}. Date in ticker date: {date_in_ticker_data}, Start '
              f'date: {from_date.date()}')
        return False
    return True


def calculate_returns(ticker, ticker_data, interval):
    # print(ticker_data[-days])
    # print(f"{interval}({days}): {ticker_data[-days]['close']} Today: {ticker_data[-1]['close']}")
    days = trading_days[interval]['open']
    if len(ticker_data) == 0 or len(ticker_data) < days:
        print(f"{ticker}: {interval} return is set to 0. Days available {len(ticker_data)}.")
        return 0
    if not are_trading_days_range_within_range(ticker, ticker_data, interval):
        return 0
    ret = (ticker_data[-1]['close'] - ticker_data[-days]['close']) * 100 / ticker_data[-days]['close']
    return round(ret, PRECISION)


def calculate_volatility(ticker_data):
    # df1 = pd.DataFrame.from_dict(ticker_data)
    df = pd.DataFrame([(data['close']) for data in ticker_data],
                      columns=['close'])
    daily_returns = df['close'].pct_change() * 100
    vol = daily_returns.std() * math.sqrt(len(ticker_data))
    return round(vol, PRECISION)


def sort_on_returns_ratio(sub_li):
    # reverse = None (Sorts in Ascending order)
    # key is set to sort using second element of
    # sublist lambda has been used
    sub_li.sort(key=lambda x: x[2], reverse=True)
    return sub_li


# def calculate_ratio(tickers):
#     skip_count = 0
#     total_count = 0
#     num_tickers = len(tickers)
#     nse_live = NSELive()
#     for ticker in tickers:
#         skip = False
#         total_count += 1
#         if total_count % 128 == 0:
#             nse_live = NSELive()
#         print(f"[{total_count}/{num_tickers}] Calculating values of {ticker:{TICKER_NAME_MAX}}", end=" ")
#         # zerodha_ticker_name = f"NSE:{ticker}"
#         # quotes = kite.quote(zerodha_ticker_name)
#         retry_attempts = 3
#         market_cap = 0
#         while retry_attempts > 0:
#             try:
#                 quote = nse_live.trade_info(ticker)
#                 market_cap = quote['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100.0
#                 retry_attempts = 0
#             except (requests.exceptions.Timeout, KeyError):
#                 print(f'Retrying...{retry_attempts}')
#                 retry_attempts -= 1
#
#         if market_cap < MIN_MARKET_CAP:
#             skip_count += 1
#             print(f"Skipping[{skip_count}/{num_tickers}] {ticker} as market cap is {market_cap:.02f} Cr.", end=" ")
#             skip = True
#         ticker_token = ''
#         for instrument in nse_instruments:
#             if instrument['tradingsymbol'] == ticker or re.search(f'{ticker}-', instrument['tradingsymbol']):
#                 ticker_token = instrument['instrument_token']
#                 break
#         if ticker_token == '':
#             skip_count += 1
#             print(f"Skipping[{skip_count}/{num_tickers}] {ticker} not found,.")
#             continue
#         ticker_data = get_historical_data(ticker_token, 500)
#         if len(ticker_data) == 0:
#             skip_count += 1
#             print(f"Skipping[{skip_count}/{num_tickers}] {ticker} no data.")
#             continue
#         volatility = calculate_volatility(ticker_data)
#         one_month_returns = calculate_returns(ticker, ticker_data, '1M')
#         three_month_returns = calculate_returns(ticker, ticker_data, '3M')
#         six_month_returns = calculate_returns(ticker, ticker_data, '6M')
#         twelve_month_returns = calculate_returns(ticker, ticker_data, '12M')
#         ratio = round((WEIGHT_1_MONTH * one_month_returns +
#                        WEIGHT_3_MONTH * three_month_returns +
#                        WEIGHT_6_MONTH * six_month_returns +
#                        WEIGHT_12_MONTH * twelve_month_returns) / volatility, PRECISION)
#         result = [ticker, ticker_data[-1]['close'], ratio, one_month_returns, three_month_returns, six_month_returns,
#                   twelve_month_returns, volatility, round(market_cap, PRECISION)]
#         if skip:
#             skipped.append(result)
#         else:
#             results.append(result)
#         print(f"{ratio}")
#     print(f"Number of equities valid:   {num_tickers - skip_count} out of {num_tickers}")
#     print(f"Number of equities skipped: {skip_count} out of {num_tickers}")


def write_csv(values, skipped_file=False):
    headings = ['Ticker', 'Close Price', 'ReturnsRatio', '1M Returns', '3M Returns', '6M Returns', '12M Returns',
                '1Y Volatility', 'Market Cap']
    append_skipped = ''
    if skipped_file:
        append_skipped = "_skipped"
        print(f"Number of equities skipped:   {len(values)}")
    else:
        print(f"Number of equities valid:   {len(values)}")

    filename = f"Relative_Momentum_{report_date.month}_{report_date.day}_{report_date.year}{append_skipped}.csv"
    with open(filename, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headings)
        csvwriter.writerows(values)


def download_equities_csv():
    r = requests.get(equities_csv, allow_redirects=True)
    with open(LOCAL_EQUITIES_FILE, 'wb') as f:
        f.write(r.content)


def get_equities_list():
    ticker_synbols = []
    with open(LOCAL_EQUITIES_FILE, 'r') as f:
        csv_file = csv.reader(f)
        for lines in csv_file:
            if lines[0] == 'SYMBOL':
                continue
            ticker_synbols.append(lines[0])
    print(f'Number of equities: {len(ticker_synbols)}')
    return ticker_synbols


def get_portfolio_stocks():
    portfolio_symbols = []
    with open(CURRENT_PORTFOLIO, 'r') as f:
        csv_file = csv.reader(f)
        for lines in csv_file:
            portfolio_symbols.append(lines[0])
    print(f'Number of equities in portfolio: {len(portfolio_symbols)}')
    return portfolio_symbols


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', metavar='date', required=False,
                        help='Start date of the report . Format: Date.month.year')
    parser.add_argument('--proxy', action='store_true', required=False,
                        help='Use proxy server')
    args = parser.parse_args()
    return args


def thread_worker(ticker, portfolio):
    nse_live = NSELive()
    skip = False
    print(f"Calculating values of {ticker:{TICKER_NAME_MAX}}")
    # zerodha_ticker_name = f"NSE:{ticker}"
    # quotes = kite.quote(zerodha_ticker_name)
    retry_attempts = 3
    market_cap = 0
    while retry_attempts > 0:
        try:
            quote = nse_live.trade_info(ticker)
            market_cap = quote['marketDeptOrderBook']['tradeInfo']['totalMarketCap'] / 100.0
            retry_attempts = 0
        except (requests.exceptions.Timeout, KeyError, json.decoder.JSONDecodeError):
            print(f'{ticker} Retrying...{retry_attempts}')
            retry_attempts -= 1
            time.sleep(1)

    if market_cap < MIN_MARKET_CAP and ticker not in portfolio:
        # print(f"Skipping {ticker} as market cap is {market_cap:.02f} Cr.", end=" ")
        skip = True
    ticker_token = ''
    for instrument in nse_instruments:
        if instrument['tradingsymbol'] == ticker or re.search(f'^{ticker}-', instrument['tradingsymbol']):
            ticker_token = instrument['instrument_token']
            break
    if ticker_token == '':
        # print(f"Skipping {ticker} not found,.")
        return None
    ticker_data = get_historical_data(ticker_token, 500)
    if len(ticker_data) < MIN_TRADING_DAYS_DATA:
        print(f"Skipping {ticker} no data.")
        return None
    volatility = calculate_volatility(ticker_data)
    one_month_returns = calculate_returns(ticker, ticker_data, '1M')
    three_month_returns = calculate_returns(ticker, ticker_data, '3M')
    six_month_returns = calculate_returns(ticker, ticker_data, '6M')
    twelve_month_returns = calculate_returns(ticker, ticker_data, '12M')
    if one_month_returns is None \
            or three_month_returns is None \
            or six_month_returns is None \
            or twelve_month_returns is None:
        skip = True
    ratio = round((WEIGHT_1_MONTH * one_month_returns +
                   WEIGHT_3_MONTH * three_month_returns +
                   WEIGHT_6_MONTH * six_month_returns +
                   WEIGHT_12_MONTH * twelve_month_returns) / volatility, PRECISION)
    result = [ticker, ticker_data[-1]['close'], ratio, one_month_returns, three_month_returns, six_month_returns,
              twelve_month_returns, volatility, round(market_cap, PRECISION)]
    return result, skip


def thread_calculate_ratio(tickers, portfolio):
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = []
        for ticker in tickers:
            futures.append((ticker, executor.submit(thread_worker, ticker, portfolio)))
        for future in futures:
            if future[1].result() is not None:
                (result, skip) = future[1].result()
                if skip:
                    skipped.append(result)
                else:
                    results.append(result)


kite = Zerodha()
nse_instruments = []
results = []
skipped = []
trading_days = {'1M': {'open': 0, 'max_delta': 5, 'start_date': None},
                '3M': {'open': 0, 'max_delta': 7, 'start_date': None},
                '6M': {'open': 0, 'max_delta': 12, 'start_date': None},
                '12M': {'open': 0, 'max_delta': 15, 'start_date': None}}


def main():
    global report_date
    args = parse_arguments()
    if args.date:
        report_date = datetime.datetime.strptime(args.date, "%d.%m.%Y")
    print(report_date)

    set_proxies(args)
    if login():
        print("Login Successful")
    else:
        print("Login failed!")
        exit(-1)

    download_equities_csv()
    nse_equities_from_zerodha()

    # print(kite.quote(ticker))
    tickers = get_equities_list()
    portfolio = get_portfolio_stocks()
    # tickers = ['ATGL', 'SDBL', 'VBL', 'GOODYEAR', 'INDIGO', 'APARINDS',
    #            'JINDALSAW', 'KIRLOSIND', 'MAZDOCK', 'POWERMECH', 'RKFORGE', 'RVNL',
    #            'SAFARI', 'SAKSOFT', 'IZMO', 'TEGA', 'TITAGARH', 'ZENTEC2', 'RBL']
    # tickers = ['ZENTEC']
    calculate_trading_days()
    thread_calculate_ratio(tickers, portfolio)
    # calculate_ratio(tickers)
    sort_on_returns_ratio(results)
    write_csv(results)
    sort_on_returns_ratio(skipped)
    write_csv(skipped, skipped_file=True)


if __name__ == '__main__':
    main()
