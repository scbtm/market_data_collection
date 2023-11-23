import yfinance as yf #type: ignore
import logging
from multiprocessing.pool import ThreadPool
import pandas as pd #type: ignore
from data_collection import functions as Fns

class Stock:
    def __init__(self, ticker:str, tmp_folder:str, last_datapoint:str = None):
        #Stock ticker
        self.ticker = ticker

        #Last seen value of timeseries of the stock's history
        self.last_datapoint = last_datapoint

        #Date to start fetching data from (the day after last_datapoint)
        self.start_date = None

        #Later on, we see if the new data has null values. O if not, total-count-of-nulls if so
        self.null_values_exist = None
        self.processed_null_values = None

        #Timeframe to fetch data for (dict that will be passed to yfinance as either period or start date)
        self.determine_timeframe()
        timeframe_is_valid = isinstance(self.timeframe, dict)
        if not timeframe_is_valid:
            raise ValueError("Error instantiating Stock object. Invalid timeframe")
        
        #First and last days of the new data, if successful 
        self.first_day = None
        self.last_day = None

        #Timespan of the new data, if successful
        self.timespan = None

        #Ingestion date of the new data
        self.ingestion_date = pd.Timestamp.now().strftime('%Y-%m-%d')

        self.tmp_folder = tmp_folder

    def determine_timeframe(self):
        # Implement logic to determine the timeframe to fetch data for

        #convert last_datapoint to datetime object
        if self.last_datapoint is not None:

            try:
                last_datapoint = pd.to_datetime(last_datapoint)
                start_date = (last_datapoint + pd.DateOffset(days=1)).strftime('%Y-%m-%d')
                self.start_date = start_date
                self.timeframe = {'start': start_date}

            except Exception as e:
                logging.error(f"Error converting last_datapoint to datetime object: {e}")                

        else:
            # Fetch all data
            self.timeframe = {'period': 'max'}
        

    def fetch_history(self):
        """
        Fetches stock history from yfinance and processes it

        Returns:
            pd.DataFrame: Processed dataframe
        """

        # Initialize variables
        data_is_valid = False
        data_processed_is_valid = False
        df = None

        # Fetch stock history
        try:
            # Use yfinance to fetch the data
            df = yf.Ticker(self.ticker).history(**self.timeframe)

            self.null_values_exist = df.isnull().sum().sum()

            df['Ticker'] = self.ticker

            data_is_valid = isinstance(df, pd.DataFrame) and not df.empty
            
        except Exception as e:
            logging.error(f"Error fetching data for {self.ticker}: {e}")

        if data_is_valid:
            try:
                # Process the data
                df = self.process_stock_dataframe(df)
                data_processed_is_valid = isinstance(df, pd.DataFrame) and not df.empty
                

            except Exception as e:
                logging.error(f"Error processing data for {self.ticker}: {e}")

        # Extract metadata if data is valid
        if data_processed_is_valid:
            self.first_day = df['Date'].iloc[0]
            self.last_day = df['Date'].iloc[-1]
            self.timespan = (pd.to_datetime(self.last_day) - pd.to_datetime(self.first_day)).days + 1
            self.final_df_length = len(df)
            self.processed_null_values = df.isnull().sum().sum()

        return df if data_processed_is_valid else None
    
    def process_stock_dataframe(self, df:pd.DataFrame) -> pd.DataFrame:
        #Make sure the index is a datetime object
        df.index = pd.to_datetime(df.index)

        #Make sure the index is sorted
        df.sort_index(inplace=True)
        #Fill in weekends with last seen value in series
        df = df.resample('D').ffill()
        df.reset_index(inplace=True)
        df = df[columns]
        first_day = df['Date'].iloc[0]
        last_day = df['Date'].iloc[-1]
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        df['Volume'] = df['Volume'].astype(int)

        columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']
        types = {'Date': 'str', 
                'Open': 'float', 
                'High': 'float', 
                'Low': 'float', 
                'Close': 'float', 
                'Volume': 'int', 
                'Ticker': 'str'}

        #Verify the final schema (col types only) is correct
        schema_is_valid = all([df[col].dtype == types[col] for col in columns]) 
        return df if schema_is_valid else None
    
    def write_tmp_data(self, df:pd.DataFrame):
        """
        Writes the dataframe and some metadata to a temporary file

        Args:
            df (pd.DataFrame): Dataframe to write
        """
        # Write the dataframe to a temporary file
        tmp_folder = self.tmp_folder

        ts_path = f"{tmp_folder}/history_{self.ticker}.csv"

        metadata_path = f"{tmp_folder}/metadata_{self.ticker}.csv"

        #Build metadata dataframe
        metadata = pd.DataFrame({
            'ticker': self.ticker,
            'ingestion_date': self.ingestion_date,
            'first_day': self.first_day,
            'last_day': self.last_day,
            'timespan': self.timespan,
            'final_df_length': self.final_df_length,
            'input_null_values': self.null_values_exist,
            'processed_null_values': self.processed_null_values
        }, index=[0])


        df.to_csv(ts_path, index=False)
        metadata.to_csv(metadata_path, index=False)

        successfully_written = Fns.validate_file(ts_path) and Fns.validate_file(metadata_path)

        return successfully_written
    
    def run_pipeline(self):
        """
        Runs the entire pipeline for a stock
        """

        try:
            # Fetch stock history
            df = self.fetch_history()

            # Write the data to a temporary file
            successfully_written = self.write_tmp_data(df)

        except Exception as e:
            logging.error(f"Error running stock-data pipeline for {self.ticker}: {e}")
            successfully_written = False

        return successfully_written
        


##############################################################################################################

class MarketDataCollector:
    def __init__(self, tickers:list, tmp_folder:str, existing_metadata:pd.DataFrame = None):

        #This ticker list should be ingested directly from a remote bucket and should
        #contain tickers that are worth tracking regardless of whether or not they have been tracked before
        self.tickers = tickers

        #Temporary folder to write data to
        self.tmp_folder = tmp_folder

        #This should be a dataframe with the most recent metadata ingested from a remote bucket
        self.existing_metadata = existing_metadata
        self.existing_tickers = None
        self.most_recent_datapoints = None
        self.previous_ingestion_dates = None


    def get_relevant_metadata(self):
        if self.existing_metadata is not None:
            self.existing_tickers = self.metadata['ticker'].tolist()
            self.most_recent_datapoints = self.metadata['last_day'].tolist()
            self.previous_ingestion_dates = self.metadata['ingestion_date'].to_list()

            ingestion_information = list(zip(self.existing_tickers, 
                                             self.most_recent_datapoints, 
                                             self.previous_ingestion_dates))

            return ingestion_information
        
        else:
            return None
        
    def filter_existing_tickers(self):
        #If the difference between a ticker's last datapoint and last ingestion date is greater than 30 days, don't ingest it,
        #as it could be delisted or have other issues
        relevant_metadata = self.get_relevant_metadata()

        tickers_to_ingest = []

        if relevant_metadata is not None:
            for ticker, last_datapoint, last_ingestion_date in relevant_metadata:
                if (pd.to_datetime(last_ingestion_date) - pd.to_datetime(last_datapoint)).days < 30:
                    tickers_to_ingest.append((ticker, last_datapoint, last_ingestion_date))
                else:
                    continue

            return tickers_to_ingest
        
        else:
            return None
    
    def get_tickers_to_ingest(self):

        #These are tickers that should be tracked and are in metadata ie previously tracked
        tickers_to_ingest = self.filter_existing_tickers()
        ticker_names = [ticker[0] for ticker in tickers_to_ingest]
        selected_tickers = self.tickers

        #If a ticker is existing in previous metadata, but it is not in the list of tickers to ingest, 
        # it should not be ingested
        selected_and_existing = set(selected_tickers).intersection(ticker_names)
        selected_and_non_existing = set(selected_tickers) - selected_and_existing

        final_ticker_list = list(selected_and_existing.union(selected_and_non_existing))

        tickers_to_ingest = [ticker for ticker in tickers_to_ingest if ticker[0] in final_ticker_list]

        if tickers_to_ingest is not None:
            ticker_names = [ticker[0] for ticker in tickers_to_ingest]

            #These are tickers that should be tracked but are not in metadata ie not previously tracked
            #i.e. their last datapoint and ingestion date are both None
            new_tickers = [(ticker, None, None) for ticker in self.tickers if ticker not in ticker_names]

            #Combine the two lists
            tickers_to_ingest.extend(new_tickers)

        else:
            tickers_to_ingest = [(ticker, None, None) for ticker in self.tickers]

        return list(set(tickers_to_ingest)) #remove duplicates
