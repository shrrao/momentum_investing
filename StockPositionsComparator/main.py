import pandas as pd
import csv
import os
import sys
import datetime
import argparse
sys.path.insert(0, '../')
from StockPositionPlotter import plotter


parser = argparse.ArgumentParser()
parser.add_argument('--last', metavar='date', required=False,
                    help='Date of previous report to compare. Format: Date.month.year')
parser.add_argument('--current', metavar='date', required=False,
                    help='Date of current report to compare. Format: Date.month.year')
args = parser.parse_args()

MIN_PORTFOLIO_VALUE = 200000
NUM_STOCKS_IN_PORTFOLIO = 20
FILENAME_LAST = "Relative_Momentum_10_27_2023.csv"
FILENAME_CURRENT = "Relative_Momentum_11_3_2023.csv"

if args.current:
    current_date = args.current.split('.')
    FILENAME_CURRENT = f'Relative_Momentum_{int(current_date[1])}_{int(current_date[0])}_{int(current_date[2])}.csv'
if args.last:
    last_date = args.last.split('.')
    FILENAME_LAST = f'Relative_Momentum_{int(last_date[1])}_{int(last_date[0])}_{int(last_date[2])}.csv'

FILE_LAST_WEEK = os.path.abspath(
    r'C:\Users\shrrao\OneDrive - Nokia\Misc\Downloads\Smallcase\Reports'
    fr'\{FILENAME_LAST}')
FILE_CURRENT_WEEK = os.path.abspath(
    r'C:\Users\shrrao\OneDrive - Nokia\Misc\Downloads\Smallcase\Reports'
    fr'\{FILENAME_CURRENT}')
FILE_PORTFOLIO_STOCKS = os.path.abspath(
    r'C:\Users\shrrao\OneDrive - Nokia\Misc\Downloads\Smallcase'
    r'\currentStocks.csv')
FILE_ANALYSIS = os.path.abspath(
    fr'C:\Users\shrrao\OneDrive - Nokia\Misc\Downloads\My_Docs_Personal\ID\Preethi\Finance\Smallcase\Analysis'
    fr'\Rebalance_{datetime.date.today().strftime("%d.%m.%Y")}.txt')
FILE_ENTRY_CANDIDATES = os.path.abspath(
    fr'C:\Users\shrrao\OneDrive - Nokia\Misc\Downloads\My_Docs_Personal\ID\Preethi\Finance\Smallcase\Analysis'
    fr'\entryCandidates.csv')


def write_entry_candidates_file(entry_candidates):
    with open(FILE_ENTRY_CANDIDATES, 'w') as f:
        for line in entry_candidates:
            f.write(f"{line}\n")


try:
    last_week_stocks = pd.read_csv(FILE_LAST_WEEK)
    last_week_stocks.sort_values(by=["ReturnsRatio"], ascending=False, inplace=True, ignore_index=True)
except FileNotFoundError:
    print(f'Error: File not found: {FILE_LAST_WEEK}')
    exit(-1)

try:
    current_week_stocks = pd.read_csv(FILE_CURRENT_WEEK)
    current_week_stocks.sort_values(by=["ReturnsRatio"], ascending=False, inplace=True, ignore_index=True)
except FileNotFoundError:
    print(f'Error: File not found: {FILE_CURRENT_WEEK}')
    exit(-1)

# sys.stdout.close()
# sys.stdout = sys.__stdout__
# stock = 'ELECON'
# print(
#     f'{stock:20}  - was in {last_week_stocks[last_week_stocks.Ticker == stock].index.item() + 1:4}'
#     f', now in {current_week_stocks[current_week_stocks.Ticker == stock].index.item() + 1:4} position')
# exit(0)
def get_portfolio_stocks():
    portfolio_symbols = []
    with open(FILE_PORTFOLIO_STOCKS, 'r') as f:
        csv_file = csv.reader(f)
        for lines in csv_file:
            portfolio_symbols.append((lines[0], int(lines[1]), float(lines[2])))
    print(f'Number of equities in portfolio: {len(portfolio_symbols)}')
    return portfolio_symbols

portfolio = get_portfolio_stocks()

portfolio_stocks_rank = {}
exit_candidates = []
portfolio_value = 0
money_put_in = 0

sys.stdout = open(FILE_ANALYSIS, 'w')
print(f'Date: {datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")}')
print(f"\n===== Portfolio [{len(portfolio)}]: =====")
print("Name                       Entry Price      Current Report Price     Quantity     Last Position     Current Position     Direction")
for stock in portfolio:
    stock_name = stock[0]
    quantity = stock[1]
    entry_price = stock[2]
    if '|' in stock_name:
        stock_name = stock_name.split('|')[0]
    stock_info = last_week_stocks[last_week_stocks.Ticker == stock_name]
    portfolio_stocks_rank[stock_name] = {'lastWeek': stock_info.index.item() + 1}
    portfolio_stocks_rank[stock_name]['lastWeekPrice'] = entry_price

    if '|' in stock:
        stock_name = stock_name.split('|')[1]
    stock_info = current_week_stocks[current_week_stocks.Ticker == stock_name]
    if len(stock_info.index):
        portfolio_stocks_rank[stock_name]['currentWeek'] = stock_info.index.item() + 1
    else:
        portfolio_stocks_rank[stock_name]['currentWeek'] = 999
    portfolio_stocks_rank[stock_name]['currentWeekPrice'] = stock_info['Close Price'].values[0]

    if portfolio_stocks_rank[stock_name]['currentWeek'] > 40:
        exit_candidates.append(stock)

    last_week_position = portfolio_stocks_rank[stock_name]['lastWeek']
    current_week_position = portfolio_stocks_rank[stock_name]['currentWeek']
    entry_price = portfolio_stocks_rank[stock_name]['lastWeekPrice']
    current_week_price = portfolio_stocks_rank[stock_name]['currentWeekPrice']
    direction = 'Down' if current_week_position > last_week_position else 'Up'
    portfolio_value += (current_week_price * quantity)
    money_put_in += (entry_price * quantity)
    returns = portfolio_value - money_put_in
    returns_percent = returns * 100 / money_put_in

    print(f"{stock_name:20}   {entry_price:15}        {current_week_price:18}     {quantity:8}     {last_week_position:13}    {current_week_position:17}     [{direction}]")
print(f"\nMoney Put In:    Rs. {money_put_in:>10.2f}")
print(f"Portfolio Value: Rs. {portfolio_value:>10.2f}")
print(f"Returns:         Rs. {returns:>10.2f}  [{returns_percent:.2f}%]")


print(f"\n\n===== Exit candidates [{len(exit_candidates)}]: =====")
print("Name                 Last Report Price     Current Report Price     Quantity     Last Position     Current Position     Direction")
if len(exit_candidates) > 0:
    for stock in exit_candidates:
        stock_name = stock[0]
        quantity = stock[1]
        last_week_position = portfolio_stocks_rank[stock_name]['lastWeek']
        current_week_position = portfolio_stocks_rank[stock_name]['currentWeek']
        last_week_price = portfolio_stocks_rank[stock_name]['lastWeekPrice']
        current_week_price = portfolio_stocks_rank[stock_name]['currentWeekPrice']
        direction = 'Down' if current_week_position > last_week_position else 'Up'
        print(
            f"{stock_name:20}   {last_week_price:15}       {current_week_price:18}     {quantity:8}     {last_week_position:13}    {current_week_position:17}     [{direction}]")
    print("================================")
else:
    print("None")

entry_candidates = []
print("\n\n===== Entry candidates: =====")
print("Name                 Last Report Price     Current Report Price     Quantity     Last Position     Current Position     Direction")
top_x_stocks = min(30, len(current_week_stocks))
for num in range(0, top_x_stocks):
    entry_candidate = current_week_stocks.iloc[num].Ticker
    quantity = 0
    current_week_stock_info = current_week_stocks[current_week_stocks.Ticker == entry_candidate]
    last_week_stock_info = last_week_stocks[last_week_stocks.Ticker == entry_candidate]
    is_present_in_portfolio = lambda x: (len([item for item in portfolio if item[0] == x]) > 0)

    if not is_present_in_portfolio(entry_candidate):
        stock_info = last_week_stocks[last_week_stocks.Ticker == entry_candidate]
        if len(stock_info.index):
            last_week_position = stock_info.index.item() + 1
            last_week_price = last_week_stock_info['Close Price'].values[0]
        else:
            last_week_position = 999
            last_week_price = 0.0
        current_week_position = num + 1
        current_week_price = current_week_stock_info['Close Price'].values[0]
        direction = 'Down' if current_week_position > last_week_position else 'Up'
        entry_candidates.append(entry_candidate)
        quantity = round(max(MIN_PORTFOLIO_VALUE, portfolio_value) / current_week_price / NUM_STOCKS_IN_PORTFOLIO)
        print(
            f"{entry_candidate:20}   {last_week_price:15}       {current_week_price:18}     {quantity:8}     {last_week_position:13}    {current_week_position:17}     [{direction}]")
print("============================")

write_entry_candidates_file(entry_candidates)
print("\nReports Used:")
print(f"Last Week:    {FILE_LAST_WEEK}")
print(f"Current Week: {FILE_CURRENT_WEEK}")
print(f"\nAnalysis in:  {FILE_ANALYSIS}")
print("============================")
print("Executed on: Pending")



sys.stdout.close()
sys.stdout = sys.__stdout__
print(f"Check analysis in {FILE_ANALYSIS}")
os.startfile(FILE_ANALYSIS)
print(f"Opening graph")
stock_positions_plotter = plotter.StockPositionPlotter()
stock_positions_plotter.plot()