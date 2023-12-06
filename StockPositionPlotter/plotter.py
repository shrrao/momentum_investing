import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import mplcursors
import argparse

ENTRY_CANDIDATES_FILE = os.path.abspath(
    r'C:\Users\shrrao\OneDrive - Nokia\Misc\Downloads\Smallcase\Analysis'
    r'\entryCandidates.csv')
REPORTS_DIRECTORY = os.path.abspath(
    r'C:\Users\shrrao\OneDrive - Nokia\Misc\Downloads\Smallcase\Reports')


class StockPositionPlotter():
    def __init__(self):
        # Create a variable to keep track of the currently selected stock
        self.selected_stock = None
        self.stocks = None

    def parse_arguments(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('--stock', type=str, required=False,
                                 help='Name(s) of the stock ticker. Separate multiple stocks by comma(,) ')
        args = self.parser.parse_args()
        self.stocks = args.stock

    # Function to find the row number of a stock in a CSV file
    def find_stock_row(self, filename, stock_symbol):
        df = pd.read_csv(filename)
        try:
            row_num = df[df['Ticker'] == stock_symbol].index[0] + 1  # Add 1 to convert from 0-based to 1-based index
            return row_num
        except IndexError:
            return None

    def read_stock_list(self, entry_candidate_csv):
        df = pd.read_csv(entry_candidate_csv, header=None)
        return df.iloc[:, 0].to_list()

    def get_report_files(self):
        files_path = os.path.join(REPORTS_DIRECTORY, '*.csv')
        files = sorted(
            glob.iglob(files_path), key=os.path.getctime, reverse=True)
        top_20_files = files[:20]
        top_20_files.reverse()
        return top_20_files

    def plot(self):
        # List of CSV files to process
        # csv_files = ['Relative_Momentum_8_8_2023.csv', 'Relative_Momentum_8_14_2023.csv', 'Relative_Momentum_8_18_2023.csv',
        #              'Relative_Momentum_8_22_2023.csv', 'Relative_Momentum_8_30_2023.csv', 'Relative_Momentum_9_3_2023.csv',
        #              'Relative_Momentum_9_11_2023.csv', 'Relative_Momentum_9_17_2023.csv', 'Relative_Momentum_9_25_2023.csv',
        #              'Relative_Momentum_10_1_2023.csv']  # Replace with your file names
        csv_files = self.get_report_files()

        # # Stock symbol you want to search for
        if self.stocks:
            target_stocks = str(self.stocks).split(',')
        else:
            target_stocks = self.read_stock_list(ENTRY_CANDIDATES_FILE)

        # target_stocks = ['ANANDRATHI', 'IRFC', 'GET&D', 'JWL', 'PFC', 'RVNL', 'IRCON',
        #                  'AURIONPRO', 'GRWRHITECH', 'J&KBANK',
        #                  'IFCI', 'HSCL', 'CHOLAHLDNG', 'POLYCAB',
        #                  'GABRIEL', 'SYRMA', ]  # Replace with the desired stock symbol

        # Dictionary to store row numbers for each stock
        stock_row_numbers = {stock: [] for stock in target_stocks}
        dates = []

        # Iterate through each CSV file
        for csv_file in csv_files:
            dates.append(csv_file.split("Relative_Momentum_")[1])
            for target_stock in target_stocks:
                row_number = self.find_stock_row(csv_file, target_stock)
                stock_row_numbers[target_stock].append(row_number)

        # Create a dictionary to store the alpha (transparency) value for each stock
        alpha_values = {stock: 0.15 for stock in target_stocks}

        # Create a line plot to visualize the row numbers for each stock
        lines = {}
        for target_stock in target_stocks:
            line, = plt.plot(dates, stock_row_numbers[target_stock], marker='o', label=target_stock,
                             alpha=alpha_values[target_stock])
            lines[target_stock] = line

        def on_click(event):
            self.selected_stock
            if event.inaxes is not None:
                for stock in target_stocks:
                    if lines[stock].contains(event)[0]:
                        if self.selected_stock:
                            lines[self.selected_stock].set_alpha(0.15)  # Reduce opacity of the previously selected line
                        self.selected_stock = stock
                        # alpha_values[selected_stock] = 1.0  # Set opacity of the selected line to 1 (fully opaque)
                        lines[self.selected_stock].set_alpha(1.0)
                        plt.legend()
                        plt.draw()

        # Activate hover and click functionality with mplcursors
        mplcursors.cursor(hover=True)
        plt.gcf().canvas.mpl_connect('button_press_event', on_click)

        plt.xlabel('CSV File')
        plt.ylabel('Row Number')
        plt.title('Relative Position of Entry Candidates')
        plt.xticks(rotation=45)

        plt.legend()

        plt.tight_layout()

        # Show the plot
        plt.show()


if __name__ == '__main__':
    stock_positions_plotter = StockPositionPlotter()
    stock_positions_plotter.parse_arguments()
    stock_positions_plotter.plot()
