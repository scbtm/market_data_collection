import pandas as pd #type: ignore
import os
import shutil

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
        print(f"No metadata csv file found {path}: {e}")
        return None