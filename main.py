import os
import sys
import petl
import pymssql
import configparser
import requests
import datetime
import json
import decimal

# Read config file
config = configparser.ConfigParser()
try:
    config.read('ETLDemo.ini')
except:
    print('Error reading config file')
    sys.exit()

# Read config file
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
testServer = config['CONFIG']['server']
testDatabase = config['CONFIG']['database']

try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('Could not make request:' + str(e))
    sys.exit()

BOCDates = []
BOCRates = []

if(BOCResponse.status_code == 200):
    BOCData = json.loads(BOCResponse.text)
    for row in BOCData['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    # Create table  from BOC data
    exchangeRates = petl.fromcolumns([BOCDates, BOCRates], header=['Date', 'Rate'])
    print(exchangeRates)
