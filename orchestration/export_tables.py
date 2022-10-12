import psycopg2
import logging
import os
import pendulum
import pickle5 as pickle

class Docker2Host():
    
    logging.basicConfig(level=logging.INFO)
    global path
    path = '/Users/gonzo/Desktop/capstone_project/orchestration/data_storage/days/days.pickle'

    def get_days():
            with open(path, 'rb') as handle:
                days = pickle.load(handle)
            return days


    def save_days(days: int):
        with open(path, 'wb') as handle:
            pickle.dump(days, handle, protocol=pickle.HIGHEST_PROTOCOL)
        return True

    def __init__(self, database: str, user: str, password: str, host: str) -> None:
         self.database = database
         self.user = user
         self.password = password
         self.host = host
         self.path = None;
         self.cursor = None;
         self.conn = None;

    def connect_database(self) -> bool:
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
            self.cursor = cursor
            self.conn = conn
            return True
        except (Exception, psycopg2.DatabaseError) as error:
            logging.critical(error)
            logging.critical('Connection to database failed')
            return False


    def count_rows(self, table: str) -> int:
        """
        Function that count the rows of specified table.
        """
        self.cursor.execute(f'SELECT * FROM {table}')
        rows = self.cursor.fetchall()

        if not len(rows):
            print('Empty')
        else:
            count = 0
            for row in rows:
                    count += 1
        return count

    def generate_path(self, parent_path: str, day: int) -> str:
        """
        Function that returns a valid directory path with 
        the name of the current name
        e.g /path/to/app/2022-10-01 
        """
        now = pendulum.now()
        return os.path.join(parent_path, now.add(days=day).to_date_string())


    def create_path(self, path: str) -> bool:
        """
        Function that creates a directory from path.
        """
        if os.path.exists(path):
            return logging.info('Path already exist, overwriting the file')
        else:
            return os.mkdir(path)


    def copy_table(self, path: str, table: str, days: int) -> bool:
        """
        Function that executes a command that copies the content of a table
        into a .csv file
        """
        rows = self.count_rows(table=table)
        logging.info(f'Exporting {rows} rows into csv.')
        try:
            now = pendulum.now()
            filename = f'{table}_{now.add(days=days).to_date_string()}'
            final_path = self.generate_path(parent_path=f'{path}/{table}', day=days)
            self.create_path(path=final_path)
            self.cursor.execute(f'COPY {table} TO \'{final_path}/{filename}.csv\' WITH CSV HEADER DELIMITER \',\';')
            self.conn.commit()
            return True
        except:
            logging.critical('Something went wrong at exporting the table to container storage')
            return False


    def close_connection(self) -> None:
        """
        
        """
        self.cursor.close()
        self.conn.close()
        if self.conn.closed == 0:
            logging.info(f'Something went wrong, connection still open {self.conn.closed}')
            return self.conn.closed
        logging.info('Both cursor and conn closed successfully')
        return self.conn.closed

if __name__ == '__main__':

    table = {1: 'transaction', 2: 'customer'}
    connection = Docker2Host(database='capstone',
                                  user='postgres',
                                  password='admin',
                                  host='localhost')
    connection.connect_database()
    days = Docker2Host.get_days()
    
    for _ in range(1,3):
        if connection:
            logging.info(f'Working on {table.get(_)}')
            connection.copy_table(path='/Users/gonzo/Desktop/capstone_project/orchestration/data_storage/pgdata',table=table.get(_), days=days)
        else:
            logging.critical('Something went wrong')
    # Docker2Host.save_days(days= days+1)
    connection.close_connection()
    logging.info('Both thables export successfully')
