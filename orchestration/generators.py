import random
from faker import Faker


class DataFactory():

    global fake, cities
    fake = Faker()
    cities = ['Allenton', 'Hicksview', 'Smithberg', 'Robertland', 'Veronicaton', 'Lake Jamesville', 'Port Benjaminfurt', 'Averymouth', 'Erikville', 'Port Loriview', 'Grahamstad', 'Edwardsburgh', 'New Marthaborough', 'Melissafurt', 'Lanefurt', 'Clayview', 'West Nichole', 'Brownchester', 'Lake Karina', 'Michelleburgh']


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
        return random.choice(cities)
    

    def generate_customer_id() -> int:
        """
        Returns a fake customer id generated from 1 to 9999
        """
        return fake.random_int(min=1, max=900)
    

    def generate_random_date() -> str:
        """
        Returns a fake transaction timestamp
        """
        time = fake.date_time_between(start_date='-3d', end_date='now')
        return time.isoformat()
    

    def generate_amount() -> str:
        """
        Returns a fake amount pricetag
        """
        return fake.pricetag()
    

    def generate_random_store_id() -> int:
        """
        Function that generates a random id for a store
        """
        return random.randint(1, 20)
    

    def generate_random_type():
        """
        Function that returns a random type of buy
        0 for offline (in store)
        1 for online
        """
        return random.randint(0,1)