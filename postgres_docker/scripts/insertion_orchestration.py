from insertion_factory import Factory
import sys
import data_generators as dg
sys.path.insert(0, '/Users/gonzo/Desktop/capstone_project/data_generators/Dump_data')
from json_generator import JsonFactory
from parquet_generator import ParquetFactory

if __name__ == '__main__':
    
    insertions = 10
    jf = JsonFactory(None)
    pf = ParquetFactory(None)
    id = dg.generate_id()

    factory = Factory(id=id,
                      n_transactions=insertions, 
                      database='root',
                      user='root',
                      password='root',
                      host='localhost')


    status = factory.generate_insertions()
    jf.generate_jsonlines(n_lines=insertions)
    records = pf.generate_data(n_records=insertions)
    pf.generate_parquet(records=records)