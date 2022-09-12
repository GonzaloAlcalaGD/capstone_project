import re
import pytest
import psycopg2
import sys
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/postgres_docker/scripts')
from data_generator_customer import CustomerFactory
from db_conn import DatabaseConection

# DE AQUI SACAMOS LOS TEST RAZA
# # Queries 

factory = CustomerFactory(n_transactions=5, 
                          database='root',
                          user='root',
                          password='root',
                          host='localhost')

# Data generators tests.
def test_id():
    """
    Test for generated id : must be an int
    """
    assert isinstance(factory.generate_id(), int)


def test_first_name():
    """
    Test for generated first name : must be a str
    """
    assert isinstance(factory.generate_fname(), str)


def test_last_name():
    """
    Test for generated last name : must be a str
    """
    assert isinstance(factory.generate_lname(), str)


def test_phone_number():
    """
    Test for generated phone number : must be a str 
    """
    assert isinstance(factory.generate_phone_number(), str)


def test_address():
    """
    Test for generated address : must be a str
    """
    assert isinstance(factory.generate_address(), str)


# Connection Tests
def test_db_disconnect():
    """
    Tests if the connection automatically closes right after commiting inserts into database
    0 == Connection Open
    >0 == Connection Closed
    """
    status = factory.generate_insertions()
    assert status != 0 


def test_db_connection():
    """
    Tests
    """
    connection = DatabaseConection()
    assert connection.connect_database(database='root',
                                       user='root',
                                       password='root',
                                       host='localhost') == True

# Insertions Tests
def test_insertions():
    """
    Test if the desired amount of records get inserted in the database
    """
    # Desired amount of insertions
    records = 10

    connection = DatabaseConection()

    test_factory = CustomerFactory(n_transactions=records, 
                          database='root',
                          user='root',
                          password='root',
                          host='localhost')

    connection.connect_database(database='root',
                                       user='root',
                                       password='root',
                                       host='localhost')


    # We count the current rows before insertions
    current_rows = connection.count_rows()
    #Insertions
    test_factory.generate_insertions()
    # Rows after insertion
    new_rows = connection.count_rows()

    assert new_rows == current_rows+records
