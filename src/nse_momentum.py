#!/usr/bin/env python
# coding: utf-8
from datetime import date

import numpy

from pynse import *

from i_engine import IEngine

MAX_THREADS = 20

logger = logging.getLogger(__name__)


def calculateScripParameters(data: IEngine):
    nse = data.i_nse_handler
    companyName = data.i_company
    fromDate = data.i_from_date
    toDate = data.i_to_date

    logger.info(f"Fetching data of {companyName} from NSE. Date Range: {fromDate} - {toDate}")

    return [companyName, 9999, 9999, 9999, 9999, 9999, 9999, False]
    try:
        scripHistorical = nse.get_hist(companyName,
                                       from_date=fromDate,
                                       to_date=toDate)
    except ValueError as e:
        logger.warning("Error for {}. {}".format(companyName, e))
        return [companyName, 9999, 9999, 9999, 9999, 9999, 9999, False]

    try:
        closePrice = scripHistorical['close'].tolist()
    except KeyError as e:
        logger.warning("Error for {}. {}".format(companyName, e))
        return [companyName, 9999, 9999, 9999, 9999, 9999, 9999, False]

    scripHistorical['Returns'] = numpy.log(scripHistorical['close'] / scripHistorical['close'].shift(1))

    numDays = len(closePrice)
    if numDays == 0:
        logger.warning("Error for {}. {}".format(companyName, "No historical data"))
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

    logger.debug("Close {} SMA200 {:.2f}".format(closePrice[-1], sma200))
    logger.debug("% SMA200    {:8.2f} %".format(priceAboveSma200))
    logger.debug("1Y Return   {:8.2f} %".format(yearReturn))
    logger.debug("Volatility  {:8.2f} %".format(volatility))
    logger.debug("Ratio       {:8.2f}".format(ratio))
    logger.debug("Turnover    {:8}".format(dailyTurnover))

    return [companyName, round(closePrice[-1], 2), round(sma200, 2), round(volatility, 2), round(yearReturn, 2),
            round(ratio, 3), round(priceAboveSma200, 2), isLiquid]
