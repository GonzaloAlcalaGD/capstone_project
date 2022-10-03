import psycopg2
import logging
import os
import pendulum

class Docker2Host():
    
    logging.basicConfig(level=logging.INFO)

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
            self.cursor.execute(f'SELECT * FROM {table}')
            rows = self.cursor.fetchall()

            if not len(rows):
                print('Empty')
            else:
                count = 0
                for row in rows:
                        count += 1
            return count


    def copy_table(self, table: str) -> bool:
        rows = self.count_rows(table=table)
        logging.info(f'Exporting {rows} rows into csv.')
        try:
            self.cursor.execute(f'COPY {table} TO \'/var/lib/postgresql/data/{table}.csv\' WITH CSV HEADER DELIMITER \',\';')
            self.conn.commit()
            return True
        except:
            logging.critical('Something went wrong at exporting the table to container storage')
            return False
    

    def generate_path(self, parent_path: str, table: str) -> str:
        """
        Function that returns a valid directory path with 
        the name of the current name
        e.g /path/to/app/2022-10-01 
        """
        return os.path.join(parent_path, f'{table}_{pendulum.now().to_date_string()}')


    def create_path(self, path: str) -> bool:
        """
        Function that creates a directory from path.
        """
        try:
            os.mkdir(path)
            return True
        except:
            logging.critical('Something wrong ocurred')
            return False


    def container2host(self, table: str, containerID: str) -> bool:
        path = self.generate_path(parent_path=f'/Users/gonzo/Desktop/capstone_project/data_storage/pgdata/{table}', table=table)
        filename = f'{table}_{pendulum.now().to_date_string()}.csv'
        self.create_path(path=path)
        os.system(f"docker cp {containerID}:/var/lib/postgresql/data/{table}.csv {path}/{filename}")
        return True
    

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

    connection = Docker2Host(database='capstone',
                                  user='root',
                                  password='root',
                                  host='localhost')
    connection.connect_database()
    if connection:
        connection.copy_table(table='customer')
        connection.container2host(table='customer',
                                  containerID='f7cbb5820c26')
        connection.close_connection()
    else:
        logging.critical('Something went wrong')
