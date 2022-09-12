import psycopg2 
import logging

class DatabaseConection():

    def __init__(self) -> None:
        self.cursor = None
        self.conn = None
        
    def connect_database(self, database: str, user: str, password: str, host: str) -> None:
            """
            Make connection to database and returns cursor to perfom
            operations inside of it.
            """
            try:
                conn = psycopg2.connect(
                    database = database,
                    user = user,
                    password = password,
                    host = host
                )
                cursor = conn.cursor()
                logging.info('Connection to database success')
                self.cursor = cursor
                self.conn = conn
            except Exception as e:
                logging.critical(e.__class__)
                return logging.critical('Connection to database failed')

    def insertion_customer(self, record: int, id: str, first_name: str, last_name: str, phone_number: str, address: str) -> None:
        """
        Perfom insertions into database and commits them.
        """
        logging.info(f'Working on customer #{record+1} with data: {id}, {first_name}, {last_name}, {phone_number}, {address}')
        self.cursor.execute('INSERT INTO dev_test.customer(id, first_name, last_name, phone_number, address) VALUES(%s, %s, %s, %s, %s)', (id, first_name, last_name, phone_number, address))
        self.conn.commit()
    
    
    def close_connection(self) -> None:
        self.cursor.close()
        self.conn.close()
        if self.conn.closed != 0:
            return logging.info('Both cursor and conn closed successfully')
        return logging.info(f'Something went wrong, connection still open {self.conn.closed}')