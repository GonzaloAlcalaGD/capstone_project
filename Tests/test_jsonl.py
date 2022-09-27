from datetime import datetime
from importlib.resources import path
import json
import pytest
import sys
from os import path
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/data_generators/Dump_data')
sys.path.insert(0, '/Users/gonzo/Desktop/capstone_project/postgres_docker/scripts')
from json_generator import JsonFactory
import data_generators as dg

factory = JsonFactory(None)


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


def test_date_gen():
    """
    Test for instance date generated : must return str with timestamp in ISO format,
    converted to datetime object for assertion
    """
    assert isinstance(datetime.fromisoformat(factory.generate_random_date()), datetime)


def test_type_store_gen():
    """
    Test for type store : must be 0 for offline (Physical store) or 1 for online transaction
    """
    assert factory.generate_random_type() in [0,1]


def test_type_gen_instance():
    """
    Test for type instance : must be int
    """
    assert isinstance(factory.generate_random_type(), int)


# Testing for .jsonlines file.


def test_file_exists():
    """
    Test that checks if the file got generated.
    """
    file = factory.generate_jsonlines(n_lines=10)
    assert path.exists(file)


def test_file_content_instance():
    """
    In this test we check that the generated jsonl file has the correct schema and data types
    """
    file = factory.generate_jsonlines(n_lines=10)
    
    with open(file,'r') as json_file:
        json_list = list(json_file)
    
    for json_str in json_list:
        result = json.loads(json_str)
        
        assert isinstance(result, dict)
    

def test_file_lines():
    """
    Test that checks that the generate_jsonlines functions generates
    the exact amount of records inside the jsonl file.
    """
    expected_records = 10 
    current_records = 0

    file = factory.generate_jsonlines(n_lines=expected_records)

    with open(file,'r') as json_file:
        json_list = list(json_file)
    
    for _ in json_list:
        current_records+=1
    
    assert expected_records == current_records