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
                return True
            except (Exception, psycopg2.DatabaseError) as error:
                logging.critical(error)
                logging.critical('Connection to database failed')
                return False

    def insertion(self, record: int, id: str, first_name: str, last_name: str, phone_number: str, address: str, customer_id: int, transaction_ts, amount: str) -> None:
        """
        Perfom insertions into database and commits them.
        """
        logging.info(f'Working on customer #{record+1} with data: {id}, {first_name}, {last_name}, {phone_number}, {address}')
        logging.info(f'Working on transaction #{record+1} with data: {id}, {customer_id}, {transaction_ts}, {amount}')
        self.cursor.execute('INSERT INTO customer(id, first_name, last_name, phone_number, address) VALUES(%s, %s, %s, %s, %s)', (id, first_name, last_name, phone_number, address))
        self.cursor.execute('INSERT INTO transaction(id, customer_id, transaction_ts, amount) VALUES(%s, %s, %s, %s)', (id, customer_id, transaction_ts, amount))
        self.conn.commit()

    
    def close_connection(self) -> None:
        self.cursor.close()
        self.conn.close()
        if self.conn.closed == 0:
            logging.info(f'Something went wrong, connection still open {self.conn.closed}')
            return self.conn.closed
        logging.info('Both cursor and conn closed successfully')
        return self.conn.closed
    
    
    def count_rows(self, table: str):
        self.cursor.execute(f'SELECT * FROM {table}')
        rows = self.cursor.fetchall()

        if not len(rows):
            print('Empty')
        else:
            count = 0
            for row in rows:
                    count += 1
        return count