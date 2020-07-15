import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import string
import time
from datetime import datetime

# Price History API Documentation
# https://developer.tdameritrade.com/price-history/apis/get/marketdata/%7Bsymbol%7D/pricehistory

# Function to turn a datetime object into unix
def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)

# Get the historical dates you need.
start_date = datetime.strptime('2020-01-01', '%Y-%m-%d')
end_date = datetime.strptime('2020-07-14', '%Y-%m-%d')

# Convert to unix for the API
start_date_ms = unix_time_millis(start_date)
end_date_ms = unix_time_millis(end_date)

# Get a current list of all the stock symbols for the NYSE
alpha = list(string.ascii_uppercase)
alpha = ['A']
#print ("alpha:")
print(alpha)
symbols = []
print ("Get alpha")
for each in alpha:
    print ("Get url")
    url = 'http://eoddata.com/stocklist/NYSE/{}.htm'.format(each)
    print ("Get resp")
    resp = requests.get(url)
    site = resp.content
    soup = BeautifulSoup(site, 'html.parser')
    table = soup.find('table', {'class': 'quotes'})
    for row in table.findAll('tr')[1:]:
        #print (symbols)
        symbols.append(row.findAll('td')[0].text.rstrip())

# Remove the extra letters on the end
symbols_clean = []
for each in symbols:
    each = each.replace('.', '-')
    symbols_clean.append((each.split('-')[0]))

#save symbols to file
#print ("Save to CSV")
print (symbols_clean)
#symbols_clean.to_csv(r'/Users/rowanmccann/Documents/GitHub/symbols.csv')

# Get the price history for each stock. This can take a while
consumer_key = 'I4PNW349QNCPDHVCXMAHIPZRRXNVWZSQ'

data_list = []

for each in symbols_clean:
    url = r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(each)
    print (url)
    # You can do whatever period/frequency you want
    # This will grab the data for a single day
    params = {
        'apikey': consumer_key,
        'periodType': 'month',
        'frequencyType': 'daily',
        'frequency': '1',
        'startDate': start_date_ms,
        'endDate': end_date_ms,
        'needExtendedHoursData': 'true'
        }

    request = requests.get(
        url=url,
        params=params
        )

    data_list.append(request.json())
    #print ("data list is:")
    #print (data_list)
    time.sleep(.1)

# Create a list for each data point and loop through the json, adding the data to the lists
symbl_l, open_l, high_l, low_l, close_l, volume_l, date_l = [], [], [], [], [], [], []
print ("Create lists")
for data in data_list:
    try:
        symbl_name = data['symbol']
    except KeyError:
        symbl_name = np.nan
    try:
        for each in data['candles']:
            symbl_l.append(symbl_name)
            open_l.append(each['open'])
            high_l.append(each['high'])
            low_l.append(each['low'])
            close_l.append(each['close'])
            volume_l.append(each['volume'])
            date_l.append(each['datetime'])
    except KeyError:
        pass

# Create a df from the lists
df = pd.DataFrame(
     {
        'symbol': symbl_l,
        'open': open_l,
        'high': high_l,
        'low': low_l,
        'close': close_l,
        'volume': volume_l,
        'date': date_l
    }
 )

# Format the dates
df['date'] = pd.to_datetime(df['date'], unit='ms')
df['date'] = df['date'].dt.strftime('%Y-%m-%d')

# Add to bigquery
#client = bigquery.Client()
#dataset_id = 'equity_data'
#table_id = 'daily_quote_data_nyse'
# Save to csv
print ("Save to CSV")
df.to_csv(r'equities_nyse.csv')
