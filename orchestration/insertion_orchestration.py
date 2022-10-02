from insertion_factory import Factory
import sys
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/files_factories')
from json_factory import JsonFactory
from parquet_factory import ParquetFactory

if __name__ == '__main__':
    
    insertions = 100

    jf = JsonFactory(None)
    pf = ParquetFactory(None)

    factory = Factory(n_transactions=insertions, 
                      database='capstone',
                      user='root',
                      password='root',
                      host='localhost')


    status = factory.generate_insertions()
    jf.generate_jsonlines(n_lines=insertions)
    records = pf.generate_data(n_records=insertions)
    pf.generate_parquet(records=records)