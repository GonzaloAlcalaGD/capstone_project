from datetime import date, datetime
import sys
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/postgres_docker/scripts')
from data_generator_transactions import TransactionFactory
from db_conn import DatabaseConection


factory = TransactionFactory(n_transactions=5, 
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


def test_customer_id():
    """
    Test for generated customer id : must be an int
    """
    assert isinstance(factory.generate_customer_id(), int)


def test_transaction_ts():
    """
    Test for generated transaction timestamp : must be a string with iso
    format and a also a valid datetime string.
    """
    time = factory.generate_transaction_ts()
    assert isinstance(datetime.fromisoformat(time), datetime)


def test_amount():
    """
    Tests for generated amount : must be a str 
    """
    assert isinstance(factory.generate_amount(), str)


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

    test_factory = TransactionFactory(n_transactions=records, 
                          database='root',
                          user='root',
                          password='root',
                          host='localhost')

    connection.connect_database(database='root',
                                       user='root',
                                       password='root',
                                       host='localhost')


    # We count the current rows before insertions
    current_rows = connection.count_rows_transaction()
    #Insertions
    test_factory.generate_insertions()
    # Rows after insertion
    new_rows = connection.count_rows_transaction()

    assert new_rows == current_rows+records