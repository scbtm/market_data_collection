import yfinance as yf
import logging
from multiprocessing.pool import ThreadPool
import pandas as pd
from . import functions as Fns

class StockMetadataManager:

    def __init__(self, config:dict):
        self.config = config
        self.ingestion_plan = None
        self.ingestion_date = pd.Timestamp.now().strftime('%Y-%m-%d')

    def load_tickers(self):
        """
        Loads the tickers from the remote bucket

        Returns:
            list: List of tickers
        """
        # Get the tickers from the remote bucket
        tickers = Fns.read_remote_csv(self.config['remote']['ticker_file'])
        tickers = tickers['Ticker'].tolist()

        # Validate the tickers
        tickers_is_valid = isinstance(tickers, list) and len(tickers) > 0

        return tickers if tickers_is_valid else None

    def get_metadata(self):
        """
        Gets the metadata from the remote bucket

        Returns:
            pd.DataFrame: Metadata dataframe
        """
        # Get the metadata from the remote bucket
        metadata = Fns.read_metadata_csv(self.config['remote']['metadata_file'])
        metadata_is_valid = False

        if metadata is not None:
            metadata['ingestion_date'] = pd.to_datetime(metadata['ingestion_date'])

            #Keep only the latest data in the metadata file
            metadata = metadata.sort_values(by=['ticker', 'ingestion_date'], ascending=True)
            metadata = metadata.groupby('ticker').last().reset_index()

            # Validate the metadata
            metadata_is_valid = isinstance(metadata, pd.DataFrame) and not metadata.empty

        return metadata if metadata_is_valid else None
    
    def get_ticker_ingestion_plan(self):
        """
        Decide which tickers to ingest and with what timeframe

        Returns:
            list: pd.DataFrame: List of tickers to ingest and their timeframes
        """
        # Get the tickers
        tickers = self.load_tickers()

        # Get the metadata
        metadata = self.get_metadata()

        # Validate the tickers and metadata
        tickers_is_valid = tickers is not None
        metadata_is_valid = metadata is not None

        if tickers_is_valid and metadata_is_valid:
            #keep only the tickers in the metadata (those ingested at previous times) 
            # that keep updating new data since the last 30 days at ingestion time
            last_ingestion = metadata['ingestion_date']
            time_delta = last_ingestion - pd.DateOffset(days=30)
            metadata = metadata[metadata['last_day'] >= time_delta]

            """
            metadata['last_day'] = metadata['last_day'].dt.strftime('%Y-%m-%d')
            metadata = metadata[['ticker', 'last_day', 'ingestion_date']]
            #ingest only the tickers that have not been ingested at most in the last 30 days
            metadata = metadata[metadata['ingestion_date'] < (pd.Timestamp.now() - pd.DateOffset(days=30))]
            """

            metadata = metadata[['ticker', 'last_day']]

            #metadata.drop(columns=['ingestion_date'], inplace=True)
            metadata.rename(columns={'last_day': 'last_datapoint'}, inplace=True)

            #join the two dataframes
            tickers_df = pd.DataFrame({'ticker': tickers})
            tickers_to_ingest = tickers_df.merge(metadata, how='left', on='ticker')

            # Fill the missing values (those without metadata) with 'max' to fetch all data
            tickers_to_ingest['last_datapoint'] = tickers_to_ingest['last_datapoint'].fillna('max')

            tickers_to_ingest_is_valid = isinstance(tickers_to_ingest, pd.DataFrame) and not tickers_to_ingest.empty

            return tickers_to_ingest if tickers_to_ingest_is_valid else None
        
        elif tickers_is_valid and not metadata_is_valid:
            #If there is no metadata, ingest all tickers with 'max' to fetch all data
            tickers_to_ingest = pd.DataFrame({'ticker': tickers, 'last_datapoint': 'max'})
            tickers_to_ingest_is_valid = isinstance(tickers_to_ingest, pd.DataFrame) and not tickers_to_ingest.empty

            return tickers_to_ingest if tickers_to_ingest_is_valid else None

        else:
            return None
        
    def get_ingestion_plan_list(self):

        # Get the ingestion plan
        ingestion_plan = self.get_ticker_ingestion_plan()

        self.ingestion_plan = ingestion_plan

        # Validate the ingestion plan
        ingestion_plan_is_valid = isinstance(ingestion_plan, pd.DataFrame) and not ingestion_plan.empty

        #Turn the ingestion plan into a list of tuples
        ingestion_plan = ingestion_plan.to_records(index=False).tolist()

        tickers_info = []
        for ticker, last_datapoint in ingestion_plan:
            if last_datapoint == 'max':
                tickers_info.append((ticker, {'period': 'max'}))
            else:
                #ingest one day after the last datapoint
                tickers_info.append((ticker, {'start': (pd.to_datetime(last_datapoint) + pd.DateOffset(days=1)).strftime('%Y-%m-%d')}))

        if ingestion_plan_is_valid:
           return tickers_info
        else:
            logging.error("Error running ingestion plan. Ingestion plan is invalid")
            return None
        

class Stock:
    def __init__(self, ticker:str, ingestion_plan:str, ingestion_date:str):
        logging.info(f"Ticker {ticker} initialized")
        #Stock ticker
        self.ticker = ticker

        #Last seen value of timeseries of the stock's history
        self.ingestion_plan = ingestion_plan

        #Later on, we see if the new data has null values. O if not, total-count-of-nulls if so
        self.null_values_exist = None
        self.processed_null_values = None
        
        #First and last days of the new data, if successful 
        self.first_day = None
        self.last_day = None

        #Timespan of the new data, if successful
        self.timespan = None
        self.final_df_length = None

        #Ingestion date of the new data
        self.ingestion_date = ingestion_date
        

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
            df = yf.Ticker(self.ticker).history(**self.ingestion_plan)

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

        else:
            logging.error(f"Error loading history of {self.ticker}. Retrieved data is not valid")
            return None

        # Extract metadata if data is valid
        if data_processed_is_valid:
            self.first_day = df['Date'].iloc[0]
            self.last_day = df['Date'].iloc[-1]
            self.timespan = (pd.to_datetime(self.last_day) - pd.to_datetime(self.first_day)).days + 1
            self.final_df_length = len(df)
            self.processed_null_values = df.isnull().sum().sum()

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

        if data_processed_is_valid:
            logging.info(f"Ticker {self.ticker} history fetched")

        else:
            logging.info(f"Ticker {self.ticker} history failed")

        return df, metadata if data_processed_is_valid else None
    
    def process_stock_dataframe(self, df:pd.DataFrame) -> pd.DataFrame:
        #columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker']

        #Make sure the index is a datetime object
        df.index = pd.to_datetime(df.index)

        #Make sure the index is sorted
        df.sort_index(inplace=True)

        #Fill in weekends with last seen value in series
        #df = df.resample('D').ffill()
        df.reset_index(inplace=True)
        #df = df[columns]

        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        df['Volume'] = df['Volume'].astype(int)

        types = {'Date': 'str', 
                'Open': 'float', 
                'High': 'float', 
                'Low': 'float', 
                'Close': 'float', 
                'Volume': 'int', 
                'Ticker': 'str'}

        #Verify the final schema (col types only) is correct
        #schema_is_valid = all([df[col].dtype == types[col] for col in columns]) 
        #print(str(schema_is_valid))
        return df #if schema_is_valid else None
        

class StockDataCollector:
    def __init__(self, metadata_manager:StockMetadataManager):
        self.metadata_manager = metadata_manager

    def run_ingestion_pipeline_localy(self):
        """
        Runs data ingestion pipeline localy using multithreading
        """
        # Get the ingestion plan
        ingestion_plan = self.metadata_manager.get_ingestion_plan_list()

        if ingestion_plan is not None:
            # Initialize variables
            dataframes = []
            metadata = []

            # Initialize the thread pool
            pool = ThreadPool(processes=len(ingestion_plan))

            # Run the threads
            results = [pool.apply_async(Stock(ticker=ticker, 
                                              ingestion_plan = timeframe, 
                                              ingestion_date= self.metadata_manager.ingestion_date).fetch_history) for ticker, timeframe in ingestion_plan]

            pool.close()
            pool.join()
            # Get the results
            for result in results:
                output = result.get()
                if output is not None:
                    df, metadata_df = output
                    dataframes.append(df)
                    metadata.append(metadata_df)

            # Concatenate the dataframes
            dataframes = pd.concat(dataframes, ignore_index=True, axis=0)

            # Concatenate the metadata
            metadata = pd.concat(metadata, ignore_index=True, axis=0)

            # Validate the dataframes
            dataframes_is_valid = isinstance(dataframes, pd.DataFrame) and not dataframes.empty
            metadata_is_valid = isinstance(metadata, pd.DataFrame) and not metadata.empty

            if dataframes_is_valid and metadata_is_valid:
                return dataframes, metadata
            else:
                logging.error("Error running ingestion pipeline. Dataframes or metadata is invalid")
                return None, None
            
    def save_updates(self, data:pd.DataFrame, metadata:pd.DataFrame):
        """
        Writes the dataframes and metadata to the bucket

        Args:
            dataframes (pd.DataFrame): Dataframe of stock history
            metadata (pd.DataFrame): Dataframe of metadata
        """
        # Get the paths
        data_path = self.metadata_manager.config['remote']['data_file']
        metadata_path = self.metadata_manager.config['remote']['metadata_file']

        #Read the existing data and metadata
        existing_data = Fns.read_data_csv(data_path)
        existing_metadata = Fns.read_metadata_csv(metadata_path)

        if existing_data is not None and existing_metadata is not None:
            # Append the new data and metadata
            data = pd.concat([existing_data, data], ignore_index=True, axis=0)
            metadata = pd.concat([existing_metadata, metadata], ignore_index=True, axis=0)

            #Drop duplicates:
            data.drop_duplicates(inplace=True)
            metadata.drop_duplicates(inplace=True)

            # Write the dataframes
            data.to_csv(data_path, index=False)
            metadata.to_csv(metadata_path, index=False)
            return True
        
        else:
            # Write the dataframes
            logging.info("No existing data or metadata found. Writing new data and metadata")
            data.to_csv(data_path, index=False)
            metadata.to_csv(metadata_path, index=False)

            
    