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
    

    def generate_id(self) -> int:
        """
        Returns a fake id generated from a range of
        1 to 9999
        """
        return fake.random_int(min=1, max=99999)

    def generate_customer_id(self) -> int:
        """
        Returns a fake customer id generated from 1 to 9999
        """
        return fake.random_int(min=1, max=99999)
    

    def generate_transaction_ts(self) -> str:
        """
        Returns a fake transaction timestamp
        """
        time = fake.date_time_between(start_date='-10y', end_date='now')
        return time.isoformat()
    

    def generate_amount(self) -> str:
        """
        Returns a fake amount pricetag
        """
        return str(fake.pricetag())

    def generate_insertions(self):
        db.connect_database(self,
                            database=self.database,
                            user=self.user,
                            password=self.password,
                            host=self.host)        

        for _ in range(self.n_transactions):
            try:
                db.insertion_transactions(self,
                                          record = _,
                                          id = self.generate_id(),
                                          customer_id = self.generate_customer_id(),
                                          transaction_ts = self.generate_transaction_ts(),
                                          amount = self.generate_amount())
            except Exception as error:
                logging.critical(error.__class__)
                logging.critical(error)
                return logging.critical('Insertion into database failed, check params.')
        status = db.close_connection(self=self)
        logging.info('Insertions into database success.'), 
        return status


if __name__ == '__main__':
    
    factory = TransactionFactory(n_transactions=5, 
                                 database='root',
                                 user='root',
                                 password='root',
                                 host='localhost')

    factory.generate_insertions()
