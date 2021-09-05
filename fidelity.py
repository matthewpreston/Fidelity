#!/usr/bin/env python3
# fidelity.py - Scrapes ETF information from Fidelity's website
#
# Written By: Matt Preston
# Written On: Sep 5, 2021
# Revised On: Never
#
# TODO:
# - Import argparse for better database file control (currently too lazy)
# - Log output in case things go awry (can't seem to have Windows Task Scheduler
#   to do this automatically for me/grab stdout and store it)

import datetime
import sys
import time
from typing import List

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait

from database import FidelityDB
from RequestDelayer import RequestDelayer

DEFAULT_OUTPUT_FILE_NAME = "ETFs.csv"
USAGE = "python3 {} <funds.csv> [{}]\n".format(sys.argv[0], DEFAULT_OUTPUT_FILE_NAME)
MIN_POSITIONAL_ARGS = 2

DEBUG_LOGGER = True
LOGGING_FILE = "output.log"

DATABASE_LOCATION = "funds.db"
DELAY = 3                               # Seconds of delay per request
INTERVAL_RANGE = "-1 year"              # To ask for this past interval's dollar changes for funds
MAX_RETRIES = 3                         # Number of retries a request attempts to ask Fidelity
SIGNIFICANT_DIGITS_MULTIPLIER = 10000   # To story monetary values, must multiply number into an int
TODAY = datetime.datetime.today().strftime("%Y-%m-%d")
URL = "https://www.fidelity.ca/fidca/en/priceandperformance"

MONTH_NAME_TO_NUM = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12"
}

class FidelityError(Exception):
    def __init__(self):
        self.message = ""
    
class CannotFindDateError(FidelityError):
    def __init__(self):
        self.message = "Cannot find date on Fidelity website"
        
class DifferentDateError(FidelityError):
    def __init__(self, fidelityDate, todayDate):
        self.message = "Different dates. Fidelity's date: {}; Today's date: {}".format(fidelityDate, todayDate)
    
class CannotMatchFundError(FidelityError):
    def __init__(self, fundName: str, fundLookupID: str, foundFundName: str):
        self.message = "Fund name provided does not match what was searched. Fund name: {}; Fund look-up ID: {}; Found: {}".format(fundName, fundLookupID, foundFundName)

def fidelityDateToYYYY_MM_DD(fidelityDate: str) -> str:
    """Converts the date given by Fidelity to YYYY-MM-DD format"""
    
    day, month, year = fidelityDate.split("-")
    month = MONTH_NAME_TO_NUM[month]
    return "{}-{}-{}".format(year, month, day)
    
def incrementDate(date: str) -> str:
    """Advances data by 1 day"""
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    dt += datetime.timedelta(days=1)
    return dt.strftime("%Y-%m-%d")

class FidelityScraper:
    DELAY_DATE = 10 # Wait up to 10 seconds to fetch the date
    requestDelayer = RequestDelayer(DELAY)
    
    def __init__(self):
        # Create hidden webdriver instance
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        self.webdriver = webdriver.Chrome(options=options) # Driver set up in PATH
        self.webdriver.get(URL)
        self.listingSearch = self.webdriver.find_element_by_id("listing_search")
        
        # Attempt to fetch date
        try:
            WebDriverWait(self.webdriver, FidelityScraper.DELAY_DATE).until( \
                expected_conditions.presence_of_element_located((By.CLASS_NAME, "AG_price_date")) \
            )
            self.priceDate = self.webdriver.find_element_by_class_name("AG_price_date").text.strip()
            while self.priceDate == "":
                self.priceDate = self.webdriver.find_element_by_class_name("AG_price_date").text.strip()
            self.priceDate = fidelityDateToYYYY_MM_DD(self.priceDate)
        except TimeoutException:
            raise CannotFindDateError()
        
        # See if today's date matches with Fidelity's records
        # If they don't, today may be a weekend/public holiday/non-trading day
        if self.priceDate != TODAY:
            raise DifferentDateError(self.priceDate, TODAY)
        time.sleep(DELAY)
        
    def close(self):
        self.webdriver.close()
        
    @requestDelayer.delayRequest
    def getFundDollarChange(self, fundName: str, fundLookupID: str) -> int:
        """Given a funds name, get its dollar change for the day"""
        
        self.listingSearch.send_keys(fundLookupID)
        time.sleep(DELAY)
        fund = self.webdriver.find_element_by_class_name("fund")
        foundFundName = fund.find_element_by_class_name("fund_name").text.split('\n',1)[0]
        if fundName != foundFundName:
            raise CannotMatchFundError(fundName, fundLookupID, foundFundName)
        numerics = fund.find_elements_by_class_name("numeric")
        return int(float(numerics[1].text) * SIGNIFICANT_DIGITS_MULTIPLIER)
        
def main(argc: int, argv: List[str]) -> None:
    if argc < MIN_POSITIONAL_ARGS:
        sys.stderr.write("Not enough arguments given. Expected {}, got {}\n".format(MIN_POSITIONAL_ARGS, argc))
        sys.stderr.write(USAGE)
        sys.exit(-1)
        
    inputFile = argv[1]
    outputFile = DEFAULT_OUTPUT_FILE_NAME if argc <= 2 else argv[2]
    
    # Parse input file
    funds = {}
    simplifiedNames = {}
    fundIDs = []
    with open(inputFile) as inputHandle:
        inputHandle.readline()
        for l in inputHandle.readlines():
            name, lookupID, simplifiedName = l.split(',')
            simplifiedName = simplifiedName.strip()
            funds[lookupID] = name
            if simplifiedName == "":
                simplifiedNames[lookupID] = name
            else:
                simplifiedNames[lookupID] = simplifiedName
            fundIDs.append(lookupID)
    
    # Search funds on Fidelity website
    try:
        fs = FidelityScraper()
    except CannotFindDateError as e:
        # TODO - log proof that this program ran
        if DEBUG_LOGGER:
            with open(LOGGING_FILE, 'w') as o:
                o.write("{} ran; Error:\n".format(sys.argv[0]))
                o.write("{}\n".format(e.message))
        sys.stderr.write("{}\n".format(e.message))
        sys.exit(-2)
    except DifferentDateError as e:
        # TODO - log proof that this program ran
        if DEBUG_LOGGER:
            with open(LOGGING_FILE, 'w') as o:
                o.write("{} ran; Error:\n".format(sys.argv[0]))
                o.write("{}\n".format(e.message))
        sys.stderr.write("{}\n".format(e.message))
        sys.exit(-3)
    sys.stdout.write("Daily prices for {}:\n".format(fs.priceDate))
    dollarChanges = {}
    for lookupID,name in funds.items():
        sys.stdout.write("{} ".format(name))
        sys.stdout.flush()
        for tries in range(MAX_RETRIES):
            try:
                dollarChanges[lookupID] = fs.getFundDollarChange(name, lookupID)
                sys.stdout.write("." * (MAX_RETRIES-tries) + " {:0.4f}\n".format(dollarChanges[lookupID]/SIGNIFICANT_DIGITS_MULTIPLIER))
                sys.stdout.flush()
                break
            except CannotMatchFundError as e:
                if tries+1 < MAX_RETRIES:
                    sys.stdout.write("x")
                    sys.stdout.flush()
                else:
                    sys.stdout.write("x\n")
                    sys.stdout.flush()
                    sys.stderr.write("Error: " + e.message + "\n")
                    sys.stderr.flush()
            except Exception as e:
                if tries+1 < MAX_RETRIES:
                    sys.stdout.write("x")
                    sys.stdout.flush()
                else:
                    sys.stdout.write("x\n")
                    sys.stdout.flush()
                    raise e
                    
    fs.close()
    
    # Store in database
    db = FidelityDB(DATABASE_LOCATION, initialize=False)
    db.insertOrIgnoreFunds([(name,lookup) for lookup,name in funds.items()])
    for lookup,dollarChange in dollarChanges.items():
        db.insertDollarChange(lookup, dollarChange, TODAY)
    
    # Fetch a past interval of data from today's date
    intervalDate = db.getIntervalToDate(INTERVAL_RANGE)
    data = {}
    for lookupID in funds.keys():
        temp = db.getDollarChangesIntervalToDateByFund(INTERVAL_RANGE, lookupID)
        data[lookupID] = {date: float(dollarChange) for date,dollarChange in temp}
    db.close()
    
    # Output to Excel-readable file
    with open(outputFile, 'w') as o:
        o.write("Date," + ",".join([simplifiedNames[id] for id in fundIDs]) + "\n")
        while intervalDate <= TODAY:
            isRecordForDate = False
            temp = []
            for lookupID in fundIDs:
                try:
                    temp.append(str(data[lookupID][intervalDate]/SIGNIFICANT_DIGITS_MULTIPLIER))
                    isRecordForDate = True
                except:
                    temp.append("")
            if isRecordForDate:
                o.write(intervalDate + "," + ",".join(temp) + "\n")
            intervalDate = incrementDate(intervalDate)
            
    # TODO - log proof that this program finished
    if DEBUG_LOGGER:
        with open(LOGGING_FILE, 'w') as o:
            o.write("{} ran successfully.\n".format(sys.argv[0]))
    sys.exit(0)
    
if __name__ == "__main__":
    main(len(sys.argv), sys.argv)