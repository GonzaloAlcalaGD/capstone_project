import random 
from faker import Faker

class UDFCatalog:

    def transform_amount(self, x) -> float:
        return float(x[1:].replace(',', ''))


    def transform_timestamp(self, x) -> str:
        return x.split('T')[0]
    

    def transform_store_id(self, x) -> int:
        if x is None:
            return random.randint(1,20)
        else:
            return x


    def transform_phone_number(self, x) -> int:
        fake = Faker()
        if x is None:
            return fake.msisdn()[3:]
        else:
            return x


    def transform_address(self, x) -> str:
        if x is None:
            return random.choice(['Allenton', 'Hicksview', 'Smithberg', 'Robertland', 'Veronicaton', 'Lake Jamesville', 'Port Benjaminfurt', 'Averymouth', 'Erikville', 'Port Loriview', 'Grahamstad', 'Edwardsburgh', 'New Marthaborough', 'Melissafurt', 'Lanefurt', 'Clayview', 'West Nichole', 'Brownchester', 'Lake Karina', 'Michelleburgh'])
        else:
            return x


    def transform_customer_id(self, x) -> int:
        if x is None:
            return random.randint(1, 900)
        else:
            return x