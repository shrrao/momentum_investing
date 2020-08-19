import argparse
import logging

from datetime import date
from multiprocessing.pool import ThreadPool

import pandas
from pynse import Nse

from file_operations import LOCAL_FOLDER, is_company_list_available, downloadCompaniesCsv, readCompaniesCsv, saveOutputToExcel
from i_engine import IEngine
from nse_momentum import MAX_THREADS, calculateScripParameters

DEFAULT_REFRESH_OPTION = False
DEFAULT_LOG_LEVEL="INFO"
DEFAULT_LOG_FILE_NAME="shrinse.log"

FROM_DATE = { 'year':2020, 'month':1, 'day':1 }

def getDate():
    fromDate = date(year=FROM_DATE['year'], month=FROM_DATE['month'], day=FROM_DATE['day'])
    toDate = date.today()

    return fromDate, toDate


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", help="refresh database",
                        action="store_true", dest="refresh_db")
    parser.add_argument("-l", help="log level",
                        action="store", dest="log_level", default=DEFAULT_LOG_LEVEL,
                        choices=['error', 'warning', 'info', 'debug'])

    args = parser.parse_args()

    global refresh_db, logger

    logger = get_logger(args.log_level)
    refresh_db = args.refresh_db


def get_logger(log_level):
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}, chose one of error, warning, info, debug")

    l = logging.getLogger()
    l.setLevel(numeric_level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    fh = logging.FileHandler(filename=DEFAULT_LOG_FILE_NAME)
    fh.setLevel(numeric_level)
    fh.setFormatter(formatter)
    l.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(numeric_level)
    ch.setFormatter(formatter)
    l.addHandler(ch)

    return l


def updateDatabseIfRequired(csvList, dbList):
    logger.debug("Len in csv {}, len in db {}".format(len(csvList), len(dbList)))
    if set(csvList) != set(dbList):
        logger.info("Nifty symbols will be updated. This will take some time...")
        # nse.update_symbol_list()
    else:
        logger.info("Nifty symbols are up-to-date. No action required.")


def main():
    global nse, toDate

    parse_arguments()

    nse = Nse(path=LOCAL_FOLDER)

    # Get today's date in integer
    # (day, month, year)
    fromDate, toDate = getDate()
    logger.info(f"Today's date is {toDate.day:02}-{toDate.month:02}-{toDate.year:04}")

    company_list = get_company_list(nse, refresh_db)

    # Create dataFrame for output
    outputDf = pandas.DataFrame(
        columns=['Name', 'Close Price', 'SMA200 Price', 'Volatility', '1Y Return %', 'Returns/Volatility',
                 '% Above SMA200',
                 'Liquid Scrip'],
        index=company_list)

    results = generate_volatility(nse, company_list, fromDate, toDate, refresh_db)

    for result in results:
        outputDf.loc[result[0]] = result

    outputDf.sort_values(by=['Returns/Volatility', '% Above SMA200'], inplace=True, ascending=False)
    saveOutputToExcel(outputDf)


def generate_volatility(nse, company_list, from_date, to_date, refresh_db=False):
    ienginelist = list()
    for company in company_list:
        ienginelist.append(IEngine(
            i_nse_handler=nse,
            i_company=company,
            i_from_date=from_date,
            i_to_date=to_date
        ))

    if refresh_db:
        results = ThreadPool(MAX_THREADS).imap_unordered(calculateScripParameters, ienginelist)
    else:
        results = ()
    return results


def get_company_list(nse, refresh_db=False):
    # Read Nifty500 companies CSV
    if refresh_db or not is_company_list_available():
        downloadCompaniesCsv()
        companies_from_local = readCompaniesCsv()
        company_list = nse.symbols['Nifty500']
        updateDatabseIfRequired(companies_from_local, company_list)
    else:
        company_list = readCompaniesCsv()
    return company_list


if __name__ == "__main__":
    main()
