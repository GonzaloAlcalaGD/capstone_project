# Dependencies
import logging
import os
import pendulum
from datetime import datetime


def get_subdirectories(path: str) -> list:
    """
    Function that searchs for sub-directories inside a directory 
    then returns a list of all the directories names.
    """
    subdirectories = [f.path for f in os.scandir(path) if f.is_dir()]
    subdirectories = [x.split('/')[5] for x in subdirectories]
    logging.info(f'Found the following subdirectories {subdirectories}')
    return [datetime.strptime(directory, '%Y-%m-%d').date() for directory in subdirectories] 


def get_latest_folder(folders: list) -> str:
    """
    Returns the latest date from the list.
    """
    try:
        logging.info(f'Latest folder: {str(max(folders))}')
        return str(max(folders))
    except:
        return logging.critical('Couldn\'t find any sub-directory.')