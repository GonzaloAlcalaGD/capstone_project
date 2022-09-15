from faker import Faker
import pickle
import random


fake = Faker()


def last_id() -> int:
    try:
        id = pickle.load(open('/Users/gonzo/Desktop/capstone_project/data/id.dump', 'rb'))
        return id
    except:
        return False


def generate_id() -> int:
    """
    Calls function to check if id file exist, if not starts id -> 1 and saves id file.
    If id file found it loads  id += 1 and save current id into file and returns it.
    """
    id = last_id()

    if id:
        id+=1
        pickle.dump(id, open('/Users/gonzo/Desktop/capstone_project/data/id.dump', 'wb'))
        return id
    else:
        id = 1
        pickle.dump(id, open('/Users/gonzo/Desktop/capstone_project/data/id.dump', 'wb'))
        return id
    

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
    return fake.city()


def generate_customer_id() -> int:
    """
    Returns a fake customer id generated from 1 to 9999
    """
    return fake.random_int(min=1, max=9999)


def generate_transaction_ts() -> str:
    """
    Returns a fake transaction timestamp
    """
    time = fake.date_time_between(start_date='-10y', end_date='now')
    return time.isoformat()


def generate_amount() -> str:
    """
    Returns a fake amount pricetag
    """
    return str(fake.pricetag())