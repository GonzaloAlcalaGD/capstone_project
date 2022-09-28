import sys
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/postgres_docker/scripts')
from insertion_factory import Factory
from db_conn import DatabaseConection
import data_generators as dg
from datetime import datetime

id = dg.get_id()

factory = Factory(id = id,
                  n_transactions=5, 
                  database='root',
                  user='root',
                  password='root',
                  host='localhost')

# Data generators tests.
def test_id():
    """
    Test for generated id : must be an int
    """
    assert isinstance(dg.get_id(), int)


def test_first_name():
    """
    Test for generated first name : must be a str
    """
    assert isinstance(dg.generate_fname(), str)


def test_last_name():
    """
    Test for generated last name : must be a str
    """
    assert isinstance(dg.generate_lname(), str)


def test_phone_number():
    """
    Test for generated phone number : must be a str 
    """
    assert isinstance(dg.generate_phone_number(), str)


def test_address():
    """
    Test for generated address : must be a str
    """
    assert isinstance(dg.generate_address(), str)


def test_customer_id():
    """
    Test for generated customer id : must be an int
    """
    assert isinstance(dg.generate_customer_id(), int)


def test_transaction_ts():
    """
    Test for generated transaction timestamp : must be a string with iso
    format and a also a valid datetime string.
    """
    time = dg.generate_transaction_ts()
    assert isinstance(datetime.fromisoformat(time), datetime)


def test_amount():
    """
    Tests for generated amount : must be a str 
    """
    assert isinstance(dg.generate_amount(), str)

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

    test_factory = Factory(id=id,
                          n_transactions=records, 
                          database='root',
                          user='root',
                          password='root',
                          host='localhost')

    connection.connect_database(database='root',
                                       user='root',
                                       password='root',
                                       host='localhost')


    # We count the current rows before insertions
    old_rows = connection.count_rows(table='customer')
    #Insertions
    test_factory.generate_insertions()
    # Rows after insertion
    new_rows = connection.count_rows(table='customer')

    assert old_rows == new_rows
