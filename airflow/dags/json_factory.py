import jsonlines
import pendulum
import sys
import path_factory as pf
from generators import DataFactory as data
import id_loader as ID

class JsonFactory():
    def __init__(self, n_lines: int) -> None:
        self.n_lines = n_lines
    
 
    def generate_jsonlines(self, n_lines: int, days: int) -> None:
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
        path = pf.generate_path(parent_path='/opt/airflow/dags/json_storage', day=days)
        now = pendulum.now()
        filename = f'jsonl_{now.add(days=days).to_date_string()}'
        if not pf.check_path_exists(path):
                pf.create_path(path)
        with jsonlines.open(f'{path}/{filename}', 'w') as writer:
            writer.write_all(dump)
        return True