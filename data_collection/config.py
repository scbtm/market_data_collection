import os

# Use os.environ.get() to read environment variables
STOCK_DATA_BUCKET = os.environ.get('STOCK_DATA_BUCKET', 'stock_data_lake')

bucket = STOCK_DATA_BUCKET

constants = {}
constants['remote'] = {}

#Paths to store data in bucket
constants['remote']['bucket'] = bucket
constants['remote']['ticker_file'] = f"{bucket}/tickers.csv"
constants['remote']['data_folder'] = f"{bucket}/data"
constants['remote']['metadata_file'] = f"{constants['remote']['data_folder']}/metadata.csv"
constants['remote']['data_file'] = f"{constants['remote']['data_folder']}/stock_history.csv"