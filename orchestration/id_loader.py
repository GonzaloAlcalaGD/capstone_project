import pickle
import random

id_path = '/Users/gonzo/Desktop/capstone_project/orchestration/data_storage/id_storage/ids.pickle'

def load_ids() -> dict:
    """
    Loads the dict from id file and returns the dict.
    """
    with open(id_path, 'rb') as handle:
        id = pickle.load(handle)
    return id

def save_ids(updated_dict: dict):
    """
    Saves the dict into the id file
    """
    with open(id_path, 'wb') as handle:
        pickle.dump(updated_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
    return True

def get_id(dict_ids : dict) -> int:
    """
    Gets a random tuple(key,value)  from the dictionary
    If the value it's True then it returns the ID.
    Otherwise it call's himself and makes a different random choice.
    """
    key, value = random.choice(list(dict_ids.items()))
    if value: 
        dict_ids[key] = False
        save_ids(updated_dict=dict_ids)
        return key
    else:
        return get_id(dict_ids)    