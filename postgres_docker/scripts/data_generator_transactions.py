from audioop import add
import psycopg2
from faker import Faker
import logging
from db_conn import DatabaseConection as db

class TransactionFactory():
    
    logging.basicConfig(level=logging.INFO)
    global fake
    fake = Faker()
    
    def __init__(self, n_transactions: int, database: str, user: str, password: str, host: str) -> None:
        self.n_transactions = n_transactions
        self.database = database
        self.user = user
        self.password = password
        self.host = host
    
    
    def connect_database(self):
        """
        Make connection to database and returns cursor to perfom
        operations inside of it.
        """
        try:
            conn = psycopg2.connect(
                database = self.database,
                user = self.user,
                password = self.password,
                host = self.host
            )

            cursor = conn.cursor()
            logging.info('Connection to database success')
            return cursor, conn
        except Exception as e:
            logging.critical(e.__class__)
            return logging.critical('Connection to database failed')


    def generate_id(self) -> int:
        """
        Returns a fake id generated from a range of
        1 to 9999
        """
        return fake.random_int(min=1, max=9999)

    
    def generate_fname(self) -> str:
        """
        Returns a fake first name.
        """
        return fake.first_name()

    def generate_lname(self) -> str:
        """
        Returns a fake last name.
        """
        return fake.last_name()


    def generate_phone_number(self) -> str:
        """
        Returns a fake phone number generated from a msisdn.
        """
        return fake.msisdn()[3:]

    def generate_address(self) -> str:
        """
        Returns a fake generated address.
        """
        return fake.address()
    

    def generate_insertions(self):
        cursor, conn = self.connect_database()
        id = self.generate_id()
        first_name = self.generate_fname()
        last_name = self.generate_lname()
        phone_number = self.generate_phone_number()
        address = self.generate_address()

        for _ in range(self.n_transactions):
            try:
                logging.info(f'Working on transaction #{_+1} with data: {id}, {first_name}, {last_name}, {phone_number}, {address}')
                cursor.execute('INSERT INTO dev_test.transaction(id, first_name, last_name, phone_number, address) VALUES(%i, %s, %s, %s, %s)', (id, first_name, last_name, phone_number, address))
            except Exception as error:
                logging.critical(error.__class__)
                return logging.critical('Insertion into database failed, check params.')
        conn.commit()

        return logging.info('Insertions into database success.')


if __name__ == '__main__':
    
    factory = TransactionFactory(n_transactions=5, 
                                 database='root',
                                 user='root',
                                 password='root',
                                 host='localhost')

    factory.generate_insertions()
