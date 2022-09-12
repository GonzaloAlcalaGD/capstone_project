from datetime import datetime
import random
from faker import Faker
import jsonlines
import pendulum

class JsonFactory():
    
    def __init__(self, n_lines: int) -> None:
        self.n_lines = n_lines
    

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

    
    def generate_random_date(self) -> str:
        """
        Function that generates a random date from 10 years to now.
        """
        fake = Faker()
        time = fake.date_time_between(start_date='-10y', end_date='now')
        return time.isoformat()


    def generate_random_type(self):
        return random.randint(0,1)


    def generate_jsonlines(self, n_lines: int) -> None:
        
        dump = []
        file_path = f'/Users/gonzo/Desktop/capstone_project/data_generators/Dump_data/json_dump/testing_{pendulum.now()}.jsonl'

        for _ in range(0, n_lines):
            print(f'Generating jsonline: {_+1}')
            id = self.generate_random_id()
            first_name = self.generate_random_fname()
            last_name = self.generate_random_lname()
            amount = self.generate_random_amount()
            timestamp = self.generate_random_date()
            type = self.generate_random_type()

            j_dict = {
                        'id': id,
                        'ts': timestamp,
                        'customer_first_name': first_name,
                        'customer_last_name': last_name,
                        'amount': amount,
                        'type': type
                    }

            dump.append(j_dict)

        with jsonlines.open(file_path, 'w') as writer:
            writer.write_all(dump)

        return file_path

if __name__ == '__main__':

    factory = JsonFactory(None)

    records = factory.generate_jsonlines(n_lines=10)