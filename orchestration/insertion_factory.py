import logging
import sys
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/database_connection')
sys.path.insert(1, '/Users/gonzo/Desktop/capstone_project/data_generators')
from db_conn import DatabaseConection as db
import insertions_generator as dg

class Factory():
    
    
    logging.basicConfig(level=logging.INFO)
    
    def __init__(self, n_transactions: int, database: str, user: str, password: str, host: str) -> None:
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
                ids = dg.load_ids()
                db.insertion(self, 
                            record = _,
                            id_customer = dg.get_id(dict_ids=ids),
                            id_transaction = dg.get_id(dict_ids=ids),
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