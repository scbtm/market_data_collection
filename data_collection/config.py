import os

bucket = 'stock_data_lake'

constants = {}
constants['remote'] = {}
constants['local'] = {}

#Paths to storage data in bucket
constants['remote']['bucket'] = bucket
constants['remote']['ticker_file'] = f"{bucket}/tickers.csv"
constants['remote']['data_folder'] = f"{bucket}/data"
constants['remote']['metadata_file'] = f"{constants['remote']['data_folder']}/metadata.csv"
constants['remote']['data_file'] = f"{constants['remote']['data_folder']}/stock_history.csv"

#Set the paths to local reads and writes
#Get the working directory
working_directory = os.getcwd()
constants['local']['data_directory'] = f"{working_directory}/data"
constants['local']['tmp_directory'] = f"{constants['local']['data_directory']}/tmp"