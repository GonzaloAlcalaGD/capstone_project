from faker import Faker
import logging
from db_conn import DatabaseConection as db

class CustomerFactory():
    
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
    

    def generate_insertions(self) -> None:
        #connection to database
        db.connect_database(self, database=self.database,
                            user=self.user,
                            password=self.password,
                            host=self.host)
        
        for _ in range(self.n_transactions):
            try:
                db.insertion_customer(self, 
                                      record = _,
                                      id = self.generate_id(),
                                      first_name = self.generate_fname(),
                                      last_name = self.generate_lname(),
                                      phone_number = self.generate_phone_number(),
                                      address = self.generate_address())

            except Exception as error:
                logging.critical(error.__class__)
                logging.critical(error)
                return logging.critical('Insertion into database failed, check params.')
        status = db.close_connection(self=self)
        logging.info('Insertions into database success.'), 
        return status

 
if __name__ == '__main__':
    
    factory = CustomerFactory(n_transactions=5, 
                                 database='root',
                                 user='root',
                                 password='root',
                                 host='localhost')

    status = factory.generate_insertions()
    