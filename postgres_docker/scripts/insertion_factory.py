from faker import Faker
import logging
from db_conn import DatabaseConection as db
import data_generators as dg

class Factory():
    
    logging.basicConfig(level=logging.INFO)
    global fake
    fake = Faker()
    
    def __init__(self, id: int, n_transactions: int, database: str, user: str, password: str, host: str) -> None:
        self.id = id
        self.n_transactions = n_transactions
        self.database = database
        self.user = user
        self.password = password
        self.host = host
    
  
    def generate_insertions(self) -> None:
        #connection to database
        db.connect_database(self, database=self.database,
                            user=self.user,
                            password=self.password,
                            host=self.host)
        
        for _ in range(self.n_transactions):
            try:
                db.insertion(self, 
                                      record = _,
                                      id = dg.generate_id(),
                                      first_name = dg.generate_fname(),
                                      last_name = dg.generate_lname(),
                                      phone_number = dg.generate_phone_number(),
                                      address = dg.generate_address(),
                                      customer_id = dg.generate_customer_id(),
                                      transaction_ts = dg.generate_transaction_ts(),
                                      amount = dg.generate_amount())
            except Exception as error:
                logging.critical(error.__class__)
                logging.critical(error)
                return logging.critical('Insertion into database failed, check params.')
        status = db.close_connection(self=self)
        logging.info('Insertions into database success.'),
        return status