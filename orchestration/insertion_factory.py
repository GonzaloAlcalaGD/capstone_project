import logging
from db_conn import DatabaseConection as db
from generators import DataFactory as data
import id_loader as id

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
                ids = id.load_ids()
                db.insertion(self, 
                            record = _,
                            id = id.get_id(dict_ids=ids),
                            first_name = data.generate_fname(),
                            last_name = data.generate_lname(),
                            phone_number = data.generate_phone_number(),
                            address = data.generate_address(),
                            customer_id = data.generate_customer_id(),
                            transaction_ts = data.generate_random_date(),
                            amount = data.generate_amount())
            except Exception as error:
                logging.critical(error.__class__)
                logging.critical(error)
                return logging.critical('Insertion into database failed, check params.')

        status = db.close_connection(self=self)
        logging.info('Insertions into database success.'),
        return status