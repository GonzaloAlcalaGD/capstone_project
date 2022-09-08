import random
import pandas as pd
import pyarrow.parquet as pq
from faker import Faker
import pendulum

class ParquetFactory():
    
    def __init__(self, n_records: int) -> None:
        self.n_records = n_records
        
    def generate_random_id(self) -> int:
        """
        Function that generates a random id
        """
        return random.randint(0, 1000)
    

    def generate_random_fname(self) -> str:
        """
        Function that generates a random first name
        """
        fake = Faker()
        return fake.name().split(' ')[0]


    def generate_random_lname(self) -> str:
        """
        Function that generates a random last name
        """
        fake = Faker()
        return fake.name().split(' ')[1]


    def generate_random_amount(self) -> str:
        """
        Function that generates a random price amount
        """
        fake = Faker()
        return fake.pricetag()

    
    def generate_random_store_id(self) -> int:
        """
        Function that generates a random id for a store
        """
        return random.randint(1, 20)


    def generate_random_date(self) -> str:
        """
        Function that generates a random date from 10 years to now.
        """
        fake = Faker()
        time = fake.date_time_between(start_date='-10y', end_date='now')
        return time.isoformat()


    def generate_data(self, n_records: int) -> list:
        """
        Function that generates X amount of records inside a parquet file.
        """
        records = []

        for _ in range(1, n_records+1):
            print(f'Generating record {_}')
            id = self.generate_random_id()
            first_name = self.generate_random_fname()
            last_name = self.generate_random_lname()
            amount = self.generate_random_amount()
            timestamp = self.generate_random_date()
            store_id = self.generate_random_store_id()
            records.append([id, first_name, last_name, amount, timestamp, store_id])

        print('Data generated correctly')
        return records


    def generate_parquet(self, records : list) -> None:
        """
        Function that generates .parquet file with provided nested list.
        """
        file_path = f'/Users/gonzo/Desktop/capstone_project/data_generators/Dump_data/parquet_dump/testing_{pendulum.now()}.parquet'
        df = pd.DataFrame(records, columns=['Id',  'First_name', 'Last_name', 'Amount', 'timestamp', 'Store_id'])
        df.to_parquet(path=file_path, engine='pyarrow', compression='snappy')

        return file_path


if __name__ == '__main__':
    
    factory = ParquetFactory(None)
    records  = factory.generate_data(n_records = 10)
    factory.generate_parquet(records)
