#!/usr/bin/env python
# coding: utf-8

import os
import numpy
import pandas

from pynse import *
from datetime import date
from requests import get
from multiprocessing.pool import ThreadPool
import logging

# Set debug log level
logging.basicConfig(level=logging.INFO)

ARCHIVE_CSV_NAME = "ind_nifty500list.csv"
ARCHIVE_URL = f"https://archives.nseindia.com/content/indices/{ARCHIVE_CSV_NAME}"
LOCAL_FOLDER = "C:\code\shrinse\data"


FROM_DATE = {'year': 2020,
             'month': 8,
             'day': 13}


def getDate():
    todayDate = date.today()

    return { 'year': todayDate.year,
             'month': todayDate.month,
             'day': todayDate.day }


def downloadCompaniesCsv():
    url = ARCHIVE_URL
    response = get(url)
    logging.debug("Fetching Nifty500 CSV from {}".format(url))
    with open(os.path.join(LOCAL_FOLDER, ARCHIVE_CSV_NAME), 'wb') as f:
        f.write(response.content)


def readCompaniesCsv():
    companies = pd.read_csv(os.path.join(LOCAL_FOLDER, ARCHIVE_CSV_NAME), delimiter=',')
    logging.debug("Nifty500 CSV has {} entries".format(len(companies['Symbol'])))
    return companies['Symbol'].to_list()


def updateDatabseIfRequired(csvList, dbList):
    logging.debug("Len in csv {}, len in db {}".format(len(csvList), len(dbList)))
    if set(csvList) != set(dbList):
        logging.info("Nifty symbols will be updated. This will take some time...")
        # nse.update_symbol_list()
    else:
        logging.info("Nifty symbols are up-to-date. No action required.")


def calculateScripParameters(companyName):
    logging.info("Fetching data of {} from NSE".format(companyName))
    try:
        scripHistorical = nse.get_hist(companyName,
                                       from_date=dt.date(FROM_DATE['year'], FROM_DATE['month'], FROM_DATE['day']),
                                       to_date=dt.date(toDate['year'], toDate['month'], toDate['day']))
    except ValueError as e:
        logging.warning("Error for {}. {}".format(companyName, e))
        return [companyName, 9999, 9999, 9999, 9999, 9999, 9999, False]

    try:
        closePrice = scripHistorical['close'].tolist()
    except KeyError as e:
        logging.warning("Error for {}. {}".format(companyName, e))
        return [companyName, 9999, 9999, 9999, 9999, 9999, 9999, False]

    scripHistorical['Returns'] = numpy.log(scripHistorical['close'] / scripHistorical['close'].shift(1))

    numDays = len(closePrice)
    if numDays == 0:
        logging.warning("Error for {}. {}".format(companyName, "No historical data"))
        return [companyName, 9999, 9999, 9999, 9999, 9999, 9999, False]

    volatility = scripHistorical['Returns'].std() * numpy.sqrt(numDays) * 100

    yearReturn = (closePrice[-1] - closePrice[0]) * 100 / closePrice[0]

    ratio = yearReturn / volatility

    if numDays > 200:
        sma200 = numpy.sum(closePrice[0:199]) / 200
    else:
        sma200 = numpy.sum(closePrice) / numDays

    priceAboveSma200 = (closePrice[-1] - sma200) * 100 / sma200

    dailyTurnover = 0
    for index, row in scripHistorical.iterrows():
        dailyTurnover += ((row['open'] + row['high'] + row['low'] + row['close']) / 4) * row['volume']
    dailyTurnover = dailyTurnover / numDays

    isLiquid = True if dailyTurnover > 10000000 else False

    logging.debug("Close {} SMA200 {:.2f}".format(closePrice[-1], sma200))
    logging.debug("% SMA200    {:8.2f} %".format(priceAboveSma200))
    logging.debug("1Y Return   {:8.2f} %".format(yearReturn))
    logging.debug("Volatility  {:8.2f} %".format(volatility))
    logging.debug("Ratio       {:8.2f}".format(ratio))
    logging.debug("Turnover    {:8}".format(dailyTurnover))

    return [companyName, round(closePrice[-1], 2), round(sma200, 2), round(volatility, 2), round(yearReturn, 2),
            round(ratio, 3), round(priceAboveSma200, 2), isLiquid]


def saveOutputToExcel(output):
    fileName = f"{LOCAL_FOLDER}Momentum_{date.today().strftime('%d_%b_%Y')}.xlsx"
    logging.info("Saving {}".format(fileName))

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pandas.ExcelWriter(fileName, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    output.to_excel(writer, sheet_name='Sheet1', index=False, header=True)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    writer.close()


def main():
    global nse, toDate

    nse = Nse(path=LOCAL_FOLDER)

    # Get today's date in integer
    # (day, month, year)
    toDate = getDate()
    logging.info(f"Today's date is {toDate['day']:02}-{toDate['month']:02}-{toDate['year']:04}")

    # Read Nifty500 companies CSV
    downloadCompaniesCsv()
    companiesCsv = readCompaniesCsv()
    companiesDb = nse.symbols['Nifty500']
    updateDatabseIfRequired(companiesCsv, companiesDb)

    # Create dataFrame for output
    outputDf = pandas.DataFrame(
        columns=['Name', 'Close Price', 'SMA200 Price', 'Volatility', '1Y Return %', 'Returns/Volatility',
                 '% Above SMA200',
                 'Liquid Scrip'],
        index=companiesDb)

    results = ThreadPool(20).imap_unordered(calculateScripParameters, companiesDb)

    for result in results:
        outputDf.loc[result[0]] = result

    outputDf.sort_values(by=['Returns/Volatility', '% Above SMA200'], inplace=True, ascending=False)
    saveOutputToExcel(outputDf)


if __name__ == "__main__":
    main()
