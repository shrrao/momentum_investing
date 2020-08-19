# TODOs
# [ ] download data file
# [ ] export data to file
# [ ] read data from file
import logging
import os
import pandas

from datetime import date
from requests.api import get

logger = logging.getLogger(__name__)

ARCHIVE_CSV_NAME = "ind_nifty500list.csv"
ARCHIVE_URL = f"https://archives.nseindia.com/content/indices/{ARCHIVE_CSV_NAME}"


def saveOutputToExcel(output, path):
    if not os.path.exists(path):
        os.mkdir(path)
    fileName = f"Momentum_{date.today().strftime('%d_%b_%Y')}.xlsx"
    logger.info("Saving {}".format(fileName))

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pandas.ExcelWriter(fileName, engine='xlsxwriter')

    # Convert the dataframe to an XlsxWriter Excel object.
    output.to_excel(writer, sheet_name='Sheet1', index=False, header=True)

    # Close the Pandas Excel writer and output the Excel file.
    writer.save()


def is_company_list_available():
    return True


def downloadCompaniesCsv(path):
    url = ARCHIVE_URL
    response = get(url)
    logging.debug("Fetching Nifty500 CSV from {}".format(url))
    if not os.path.exists(path):
        os.mkdir(path)
    with open(os.path.join(path, ARCHIVE_CSV_NAME), 'wb') as f:
        f.write(response.content)
        f.close()


def readCompaniesCsv(path):
    companies = pandas.read_csv(os.path.join(path, ARCHIVE_CSV_NAME), delimiter=',')
    logging.debug("Nifty500 CSV has {} entries".format(len(companies['Symbol'])))
    return companies['Symbol'].to_list()