import argparse
import logging
import os
import pandas

from datetime import date
from multiprocessing.pool import ThreadPool
from pathlib import Path
from pynse import Nse

from file_operations import is_company_list_available, downloadCompaniesCsv, readCompaniesCsv, saveOutputToExcel
from i_engine import IEngine
from nse_momentum import calculateScripParameters

MAX_THREADS=20
DEFAULT_REFRESH_OPTION = False
DEFAULT_LOG_LEVEL="INFO"
DEFAULT_LOG_FILE_NAME="shrinse.log"
FROM_DATE = { 'year': 2020, 'month': 1, 'day': 1 }
LOCAL_FOLDER = "C:\code\shrinse\data"
default_nselib_path = str(Path.home() / "Documents" / "pynse") + str(os.sep)


def getDate():
    fromDate = date(year=FROM_DATE['year'], month=FROM_DATE['month'], day=FROM_DATE['day'])
    toDate = date.today()

    return fromDate, toDate


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


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", help="do not refresh database (WIP)",
                        action="store_false", dest="refresh_db")
    parser.add_argument("-l", help="log level",
                        action="store", dest="log_level", default=DEFAULT_LOG_LEVEL,
                        choices=['error', 'warning', 'info', 'debug'])
    parser.add_argument("-p", "--lib_path", help=f"Enter path to store pynse library info (default: {default_nselib_path})",
                        default=default_nselib_path)
    parser.add_argument("-o", "--output_path", help=f"Enter path to store XLS (default: {LOCAL_FOLDER})",
                        default=LOCAL_FOLDER)

    args = parser.parse_args()

    global refresh_db, logger, nselib_path, output_path

    logger = get_logger(args.log_level)
    refresh_db = args.refresh_db
    nselib_path = args.lib_path
    output_path = args.output_path

    logger.debug(f"Arguments: Log Level - {args.log_level}")
    logger.debug(f"Arguments: refresh DB -  {refresh_db}")
    logger.debug(f"Arguments: NSE Library Path - {nselib_path}")
    logger.debug(f"Arguments: Output Path - {output_path}")


def updateDatabseIfRequired(csvList, dbList):
    logger.debug("Len in csv {}, len in db {}".format(len(csvList), len(dbList)))
    if set(csvList) != set(dbList):
        logger.info("Nifty symbols will be updated. This will take some time...")
        # nse.update_symbol_list()
    else:
        logger.info("Nifty symbols are up-to-date. No action required.")


def generate_volatility(nse, company_list, from_date, to_date, refresh_db=False):
    ienginelist = list()
    for company in company_list:
        ienginelist.append(IEngine(
            i_nse_handler=nse,
            i_company=company,
            i_from_date=from_date,
            i_to_date=to_date
        ))

    results = ThreadPool(MAX_THREADS).imap_unordered(calculateScripParameters, ienginelist)
    return results


def get_company_list(nse, refresh_db=False):
    # Read Nifty500 companies CSV
    if refresh_db or not is_company_list_available():
        downloadCompaniesCsv(nselib_path)
        companies_from_local = readCompaniesCsv(nselib_path)
        company_list = nse.symbols['Nifty500']
        updateDatabseIfRequired(companies_from_local, company_list)
    else:
        company_list = readCompaniesCsv(nselib_path)
    return company_list


def main():
    global nse, toDate

    parse_arguments()

    nse = Nse(path=nselib_path)

    fromDate, toDate = getDate()

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
    saveOutputToExcel(outputDf, output_path)


if __name__ == "__main__":
    main()
