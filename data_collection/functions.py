import pandas as pd

def read_remote_csv(path:str) -> pd.DataFrame:
    """
    Reads a csv from a remote bucket

    Args:
        path (str): Path to the csv

    Returns:
        pd.DataFrame: Dataframe of the csv
    """
    try:
        df = pd.read_csv(path)
        return df
        
    except Exception as e:
        print(f"Error reading file {path}: {e}")
        return None
    
def read_metadata_csv(path:str) -> pd.DataFrame:
    """
    Reads a metadata csv from a remote bucket

    Args:
        path (str): Path to the csv

    Returns:
        pd.DataFrame: Dataframe of the csv
    """
    try:
        df = pd.read_csv(path)
        return df
        
    except Exception as e:
        print(f"All tickers will be ingested. No metadata csv file found {path}.")
        return None
    
def read_data_csv(path:str) -> pd.DataFrame:
    """
    Reads a data csv from a remote bucket

    Args:
        path (str): Path to the csv

    Returns:
        pd.DataFrame: Dataframe of the csv
    """
    try:
        df = pd.read_csv(path)
        return df
        
    except Exception as e:
        print(f"First time ingestion: No data csv file found {path}.")
        return None