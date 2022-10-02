import jsonlines
import pendulum
import os
import sys
from directories_factory import path_factory as pf
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/data_generators')
from generators import DataFactory as data
import id_loader as ID

class JsonFactory():

    def __init__(self, n_lines: int) -> None:
        self.n_lines = n_lines
    

    def generate_jsonlines(self, n_lines: int) -> None:
        """
        Function that writes n amount of json lines into a 
        jsonl file.
        """
        dump = []
        ids = ID.load_ids()
        for _ in range(0, n_lines):
            print(f'Generating jsonline: {_+1}')
            id = ID.get_id(dict_ids = ids)
            first_name = data.generate_fname()
            last_name = data.generate_lname()
            amount = data.generate_amount()
            timestamp = data.generate_random_date()
            type = data.generate_random_type()
            j_dict = {
                        'id': id,
                        'ts': timestamp,
                        'customer_first_name': first_name,
                        'customer_last_name': last_name,
                        'amount': amount,
                        'type': type
                    }
            dump.append(j_dict)
        path = pf.generate_path(parent_path='/Users/gonzo/Desktop/capstone_project/data_storage/json_storage')
        filename = f'jsonl_{pendulum.now().to_date_string()}'
        if not pf.check_path_exists(path):
                pf.create_path(path)
        with jsonlines.open(f'{path}/{filename}', 'w') as writer:
            writer.write_all(dump)
        return True