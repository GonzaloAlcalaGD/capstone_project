from faker import Faker
import pickle
import random


fake = Faker()
id_path = '/Users/gonzo/Desktop/capstone_project/data/ids.pickle'


def generate_fname() -> str:
    """
    Returns a fake first name.
    """
    return fake.first_name()

def generate_lname() -> str:
    """
    Returns a fake last name.
    """
    return fake.last_name()


def generate_phone_number() -> str:
    """
    Returns a fake phone number generated from a msisdn.
    """
    return fake.msisdn()[3:]

def generate_address() -> str:
    """
    Returns a fake generated address.
    """
    return random.choice(['Allenton', 'Hicksview', 'Smithberg', 'Robertland', 'Veronicaton', 'Lake Jamesville', 'Port Benjaminfurt', 'Averymouth', 'Erikville', 'Port Loriview', 'Grahamstad', 'Edwardsburgh', 'New Marthaborough', 'Melissafurt', 'Lanefurt', 'Clayview', 'West Nichole', 'Brownchester', 'Lake Karina', 'Michelleburgh'])


def generate_customer_id() -> int:
    """
    Returns a fake customer id generated from 1 to 9999
    """
    return fake.random_int(min=1, max=9999)


def generate_transaction_ts() -> str:
    """
    Returns a fake transaction timestamp
    """
    time = fake.date_time_between(start_date='-3d', end_date='now')
    return time.isoformat()


def generate_amount() -> str:
    """
    Returns a fake amount pricetag
    """
    return str(fake.pricetag())


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
    if value == True: 
        dict_ids[key] = False
        save_ids(updated_dict=dict_ids)
        return key
    else:
        return get_id(dict_ids)    
