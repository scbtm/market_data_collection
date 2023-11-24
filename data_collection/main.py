
from multiprocessing.pool import ThreadPool
from data_collection.stock import Stock, MarketDataCollector
from data_collection import functions as Fns
from data_collection.config import constants as Config
import pandas as pd #type: ignore
import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--batch_size', 
                    type=int, 
                    default=0,
                    help='The batch size to use for the data collection')
args = parser.parse_args()


batch_size = args.batch_size

#Check if data and metadata exist already in remote bucket
def check_remote_data_exists() -> bool:
    """
    Checks if the remote data exists already

    Returns:
        bool: True if the remote data exists
    """
    #Check if the metadata file exists
    metadata_exists = Fns.validate_file_exists(Config['remote']['metadata_file'])

    #Check if the data file exists
    data_exists = Fns.validate_file_exists(Config['remote']['data_file'])

    #Return true if both exist
    return metadata_exists and data_exists

#Read data if it exsits
def read_remote_data() -> tuple:
    """
    Reads the remote data if it exists

    Returns:
        tuple: Tuple of dataframes (metadata, data)
    """
    #Read the metadata
    metadata = pd.read_csv(Config['remote']['metadata_file'])

    #Read the data
    data = pd.read_csv(Config['remote']['data_file'])

    return metadata, data

def read_ticker_directory() -> list:
    tdf = pd.read_csv(Config['remote']['ticker_file'])

    return tdf['ticker'].tolist()

def get_ticker_info(tickers:list, metadata:pd.DataFrame) -> list:
    market_data_collector = MarketDataCollector(tickers = tickers, tmp_folder=Config['local']['tmp_directory'], existing_metadata=metadata)
    return market_data_collector.get_relevant_metadata()

def update_all_stocks(tickers):
    # Initialize a thread pool for concurrent execution
    with ThreadPool(processes=len(tickers)) as pool:
        pool.map(lambda ticker_info: Stock(ticker = ticker_info[0], 
                                           tmp_folder=Config['local']['tmp_directory'],
                                           last_datapoint=ticker_info[1]).run_pipeline(), 
                                           tickers)
        
def read_metadata_files_from_tmp_folder() -> list:

    return

def read_data_files_from_tmp_folder() -> list:

    return
        
def make_local_data_paths():

    #Create the data directory if it does not exist
    Fns.create_path_if_not_exists(Config['local']['data_directory'])

    #Create the tmp directory if it does not exist
    Fns.create_path_if_not_exists(Config['local']['tmp_directory'])
        
def run_full_pipeline(tickers, data, metadata):
    #Step 1: make_local_data_paths
    make_local_data_paths()

    #Step 2: get_ticker_info
    ticker_info = get_ticker_info(tickers, metadata)

    #Step 3: update_all_stocks
    update_all_stocks(ticker_info)

    #Step 6: read_metadata_files_from_tmp_folder
    tmp_metadata = read_metadata_files_from_tmp_folder()

    #concatenate in a single df
    new_metadata = pd.concat(tmp_metadata, ignore_index=True, axis = 0)

    #Step 7: read_data_files_from_tmp_folder
    tmp_data = read_data_files_from_tmp_folder()

    #concatenate in a single df
    new_data = pd.concat(tmp_data, ignore_index=True, axis = 0)

    #append new data and metadata to existing data and metadata if they exist
    if remote_data_exists:
        metadata = pd.concat([metadata, new_metadata], ignore_index=True, axis = 0)
        data = pd.concat([data, new_data], ignore_index=True, axis = 0)
    else:
        metadata = new_metadata
        data = new_data

    #Step 8: write_data_to_bucket
    metadata.to_csv(Config['remote']['metadata_file'], index=False)
    data.to_csv(Config['remote']['data_file'], index=False)

    #Step 9: delete_tmp_folder
    successful_run = Fns.delete_folder(Config['local']['tmp_directory'])

    return successful_run


    


if __name__ == "__main__":

    tickers = read_ticker_directory()
    
    # check_remote_data_exists
    remote_data_exists = check_remote_data_exists()

    #read_remote_data
    if remote_data_exists:
        metadata, data = read_remote_data()
    else:
        metadata = None
        data = None

    if batch_size > 0:
        #Create iterable chunks of tickers
        batch_size = 100
        ticker_chunks = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]
        run_full_pipeline(ticker_chunks, data, metadata)
    else:
        run_full_pipeline(tickers, data, metadata)
