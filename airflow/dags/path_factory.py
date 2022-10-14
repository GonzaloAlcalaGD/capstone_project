import logging
import os
import pendulum

def generate_path(parent_path: str, day: int) -> str:
    """
    Function that returns a valid directory path with 
    the name of the current name
    e.g /path/to/app/2022-10-01 
    """
    now = pendulum.now()
    return os.path.join(parent_path, now.add(days=day).to_date_string())


def check_path_exists(path: str) -> bool:
    """
    Function that returns true if the path exist or 
    False if it doesn't
    """
    return os.path.exists(path)


def create_path(path: str) -> bool:
    """
    Function that creates a directory from path.
    """
    try:
        os.mkdir(path)
        return True
    except:
        logging.critical('Something wrong ocurred')
        return False
