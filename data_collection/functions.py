import pandas as pd #type: ignore
import os
import shutil

#Local data directory
def validate_file(path:str) -> bool:
    """
    Validates that a file exists and is not empty

    Args:
        path (str): Path to the file

    Returns:
        bool: True if the file exists and is not empty
    """
    try:
        #Use the os module to check if the file exists and is not empty
        return os.path.isfile(path) and os.path.getsize(path) > 0
        
    except Exception as e:
        print(f"Error validating file {path}: {e}")
        return False
    
def create_path_if_not_exists(path:str) -> bool:
    """
    Creates a path if it does not exist

    Args:
        path (str): Path to create

    Returns:
        bool: True if the path exists or was created
    """
    try:
        #Use the os module to create the path if it does not exist
        if not os.path.exists(path):
            os.makedirs(path)
        return True
        
    except Exception as e:
        print(f"Error creating path {path}: {e}")
        return False
    
#Remote data directory/bucket
def validate_file_exists(path:str) -> bool:
    """
    Validates that a bucket path exists and is not empty

    Args:
        path (str): Path to the file

    Returns:
        bool: True if the path exists and is not empty
    """
    try:
        metadata = pd.read_csv(path, n_rows=5)
        return True
        
    except Exception as e:
        print(f"Error validating file {path}: {e}")
        return False
    
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
    
def delete_folder(path:str) -> bool:
    """
    Deletes a folder and all of its contents

    Args:
        path (str): Path to the folder

    Returns:
        bool: True if the folder was deleted
    """
    try:
        #Use the os module to delete the folder and all of its contents
        if os.path.exists(path):
            shutil.rmtree(path)
        return True
        
    except Exception as e:
        print(f"Error deleting folder {path}: {e}")
        return False
    