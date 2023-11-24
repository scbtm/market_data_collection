
## TODO complete testing and verify

import pytest # type: ignore
from data_collection.main import run_full_pipeline
import pandas as pd # type: ignore
import os

#Test that the pipeline runs successfully. We will mock the yfinance response and remote reads and writes.
#We also build different dataframes to test different possible scenarios
def test_run_full_pipeline(mock):
    #Arrange
    Config = {}
    Config['remote'] = {}
    Config['local'] = {}
    Config['remote']['bucket'] = 'stock_data_lake'
    Config['remote']['ticker_file'] = f"{Config['remote']['bucket']}/tickers.csv"
    Config['remote']['data_folder'] = f"{Config['remote']['bucket']}/data"
    Config['remote']['metadata_file'] = f"{Config['remote']['data_folder']}/metadata.csv"
    Config['remote']['data_file'] = f"{Config['remote']['data_folder']}/stock_history.csv"
    Config['local']['data_directory'] = f"{os.getcwd()}/data"
    Config['local']['tmp_directory'] = f"{Config['local']['data_directory']}/tmp"

    mock.patch('data_collection.main.make_local_data_paths', return_value = True)

    #Simulate stocks, last datapoint, and last update date
    ticker_info = [
        ('AAPL', '2023-11-01', '2023-11-01'),
        ('MSFT', '2023-11-01', '2023-11-01'),
        ]

    mock.patch('data_collection.main.get_ticker_info', 
               return_value = ticker_info)
    
    #Mocks for the fetch history function
    #the yf.Ticker('AAPL').history(**{start: 2021-01-01}) function returns the following dataframe:
    df_aapl = pd.DataFrame({'Date': [pd.to_datetime(t) for t in ['2021-01-01','2021-01-02','2021-01-03']],
                            'Open': [500.2, 501, 502],
                            'High': [510.2, 511, 512],
                            'Low': [490.1, 491, 492],
                            'Close': [500.1, 501, 502],
                            'Volume': [100, 101, 102],
                            'Dividends': [0, 0, 0],
                            'Stock Splits': [0, 0, 0]
                            })
    
    #the yf.Ticker('MSFT').history(**{start: 2021-01-01}) function returns the following dataframe:
    df_msft = pd.DataFrame({'Date': [pd.to_datetime(t) for t in ['2021-01-01', '2021-01-02','2021-01-03']],
                            'Open': [200.3, 201, 202],
                            'High': [210, 211.8, 212],
                            'Low': [190, 191.2, 192],
                            'Close': [200.1, 201, 202],
                            'Volume': [100, 101, 102],
                            'Dividends': [0, 0, 0],
                            'Stock Splits': [0, 0, 0]})
    
    #Mocks for the yf.Ticker('AAPL').history(**{start: 2021-01-01}) and 
    # yf.Ticker('MSFT').history(**{start: 2021-01-01}) functions, return previous dfs

    mock.patch('yf.Ticker(self.ticker).history(**self.timeframe)', 
               return_value = df_aapl)
    
    mock.patch('yf.Ticker(self.ticker).history(**self.timeframe)',
                return_value = df_msft)

    #Mocks for the read and write functions

    #Each element in list is a metadata dataframe
    md_aapl = pd.DataFrame({
            'ticker': 'AAPL',
            'ingestion_date': '2021-01-04',
            'first_day': '2021-01-01',
            'last_day': '2021-01-03',
            'timespan': 3,
            'final_df_length': 3,
            'input_null_values': 0,
            'processed_null_values': 0,
        }, index=[0])
    
    md_msft = pd.DataFrame({
            'ticker': 'MSFT',
            'ingestion_date': '2021-01-04',
            'first_day': '2021-01-01',
            'last_day': '2021-01-03',
            'timespan': 3,
            'final_df_length': 3,
            'input_null_values': 0,
            'processed_null_values': 0,
        }, index=[0])
    
    metadata_files_from_tmp_folder = [md_aapl, md_msft]
    mock.patch('data_collection.main.read_metadata_files_from_tmp_folder', 
               return_value = metadata_files_from_tmp_folder)

    #Mock temporary data
    df_aapl2 = df_aapl.copy()
    df_aapl2['Date'] = df_aapl2['Date'].astype(str)
    df_aapl2 = df_aapl2[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']]
    df_aapl2['Ticker'] = 'AAPL'
    df_msft2 = df_msft.copy()
    df_msft2['Date'] = df_msft2['Date'].astype(str)
    df_msft2 = df_msft2[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']]
    df_msft2['Ticker'] = 'MSFT'
    data_files_from_tmp_folder = [df_aapl2, df_msft2]

    mock.patch('data_collection.main.read_data_files_from_tmp_folder',
                return_value = data_files_from_tmp_folder)
    
    #Mock data write
    mock.patch('data_collection.main.metadata.to_csv', 
               return_value = True)
    mock.patch('data_collection.main.data.to_csv',
                return_value = True)
    
    #Mock delete tmp folder
    mock.patch('data_collection.main.Fns.delete_folder',
                return_value = True)
    
    #Act
    result = run_full_pipeline()

    #Assert
    assert result == True


