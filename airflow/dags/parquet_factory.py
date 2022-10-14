import pandas as pd
import pendulum
from generators import DataFactory as data
import id_loader as ID
import path_factory as pf

class ParquetFactory():

    
    def __init__(self, n_records: int) -> None:
        self.n_records = n_records
    
    
    def generate_data(self, n_records: int) -> list:
        """
        Function that generates X amount of records inside a parquet file.
        """
        records = []
        ids = ID.load_ids()
        for _ in range(1, n_records+1):
            print(f'Generating record {_}')
            id = ID.get_id(dict_ids = ids)
            first_name = data.generate_fname()
            last_name = data.generate_lname()
            amount = data.generate_amount()
            timestamp = data.generate_random_date()
            store_id = data.generate_random_store_id()
            records.append([id, first_name, last_name, amount, timestamp, store_id])
        print('Data generated correctly')
        return records


    def generate_parquet(self, records : list, days: int) -> None:
        """
        Function that generates .parquet file with provided nested list.
        """
        path = pf.generate_path(parent_path='/opt/airflow/dags/parquet_storage', day=days) 
        now = pendulum.now()
        filename = f'parquet_{now.add(days=days).to_date_string()}'
        if not pf.check_path_exists(path):
            pf.create_path(path)
        df = pd.DataFrame(records, columns=['Id',  'First_name', 'Last_name', 'Amount', 'timestamp', 'Store_id'])
        df.to_parquet(path=f'{path}/{filename}', engine='pyarrow', compression='snappy')
        return path+filename