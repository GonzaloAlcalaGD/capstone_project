from insertion_factory import Factory
import sys
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/files_factories')
from json_factory import JsonFactory
from parquet_factory import ParquetFactory
import pickle

global path
path = '/Users/gonzo/Desktop/capstone_project/data_storage/days/days.pickle'

def get_days():
        with open(path, 'rb') as handle:
            days = pickle.load(handle)
        return days


def save_days(days: int):
    with open(path, 'wb') as handle:
        pickle.dump(days, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return True

if __name__ == '__main__':
    
    insertions = 100

    jf = JsonFactory(None)
    pf = ParquetFactory(None)
    days = get_days()
    factory = Factory(n_transactions=insertions, 
                      database='capstone',
                      user='root',
                      password='root',
                      host='localhost')


    status = factory.generate_insertions()
    jf.generate_jsonlines(n_lines=insertions, days= days)
    records = pf.generate_data(n_records=insertions)
    pf.generate_parquet(records=records, days=days)
    save_days(days= days+1)