import os

bucket = 'stock_data_lake'

constants = {}
constants['remote'] = {}

#Paths to store data in bucket
constants['remote']['bucket'] = bucket
constants['remote']['ticker_file'] = f"{bucket}/tickers.csv"
constants['remote']['data_folder'] = f"{bucket}/data"
constants['remote']['metadata_file'] = f"{constants['remote']['data_folder']}/metadata.csv"
constants['remote']['data_file'] = f"{constants['remote']['data_folder']}/stock_history.csv"