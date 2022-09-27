import sys
from os import path
from datetime import datetime
import pandas as pd
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/data_generators/Dump_data')
sys.path.insert(0, '/Users/gonzo/Desktop/capstone_project/postgres_docker/scripts')
import data_generators as dg
from parquet_generator import ParquetFactory

factory = ParquetFactory(None)

# Testing for data field generators
def test_id_gen():
    """
    Test for instance id : must be int
    """
    assert isinstance(dg.get_id(), int)


def test_fname_gen():
    """
    Test for instance first name : must be str
    """
    assert isinstance(factory.generate_random_fname(), str)


def test_lname_gen():
    """
    Test for instance last name : must be str
    """
    assert isinstance(factory.generate_random_lname(), str)


def test_amount_gen():
    """
    Test for instance of amount : must be str 
    """
    assert isinstance(factory.generate_random_amount(), str)


def test_storeId_gen():
    """
    Test for instance of store id : must be int
    """
    assert isinstance(factory.generate_random_store_id(), int)

def test_date_gen():
    """
    Test for instance date generated : must return str with timestamp in ISO format,
    converted to datetime object for assertion
    """
    assert isinstance(datetime.fromisoformat(factory.generate_random_date()), datetime)


def test_datalist_generated():
    """
    Test for list of dictionaries with data generated : must be list 
    """
    assert isinstance(factory.generate_data(n_records=10), list)

# Testing for .parquet files
def test_file_exists():
    """
    Test that checks if the file got generated.
    """
    records = factory.generate_data(n_records=10)
    file_path = factory.generate_parquet(records)
    assert path.exists(file_path)


def test_file_records_count():
    """
    Test that checks if all records get generated and 
    writed inside the parquet file
    """
    expected_records = 10
    records = factory.generate_data(n_records=expected_records)
    file_path = factory.generate_parquet(records)

    df = pd.read_parquet(file_path, engine='pyarrow')

    assert df.shape[0] == expected_records