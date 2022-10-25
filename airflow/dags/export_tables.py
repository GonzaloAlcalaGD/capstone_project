import psycopg2
import logging
import os
import pendulum
import pickle5 as pickle


logging.basicConfig(level=logging.INFO)
global path
path = '/opt/airflow/dags/days/days.pickle'

def get_days():
    with open(path, 'rb') as handle:
        days = pickle.load(handle)
    return days


def save_days(days: int):
    with open(path, 'wb') as handle:
        pickle.dump(days, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return True


def connect_database(database: str, user: str, password: str, host: str) -> bool:
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
        return cursor, conn, True
    except (Exception, psycopg2.DatabaseError) as error:
        logging.critical(error)
        logging.critical('Connection to database failed')
        return False


def count_rows(cursor, table: str) -> int:
    """
    Function that count the rows of specified table.
    """
    cursor.execute(f'SELECT * FROM {table}')
    rows = cursor.fetchall()

    if not len(rows):
        print('Empty')
    else:
        count = 0
        for row in rows:
                count += 1
    return count

def generate_path(parent_path: str, day: int) -> str:
    """
    Function that returns a valid directory path with 
    the name of the current name
    e.g /path/to/app/2022-10-01 
    """
    now = pendulum.now()
    return os.path.join(parent_path, now.add(days=day).to_date_string())


def create_path(day: int) -> bool:
    """
    Function that creates a directory from path.
    """
    try:
        now = pendulum.now()
        os.system("docker exec -it b6a25d657cb4 /bin/bash")
        os.system("cd /opt/transaction")
        os.system(f"mkdir {now.add(days=day).to_date_string()}")
        os.system("cd /opt/customer")
        os.system(f"mkdir {now.add(days=day).to_date_string()}")
        return True
    except Exception as e:
        logging.critical(e.__class__)
        logging.critical(e)
        return False

    
def copy_table(cursor, conn, path: str, table: str, days: int) -> bool:
    """
    Function that executes a command that copies the content of a table
    into a .csv file
    """
    rows = count_rows(cursor, table=table)
    logging.info(f'Exporting {rows} rows into csv.')
    try:
        now = pendulum.now()
        filename = f'{table}_{now.add(days=days).to_date_string()}'
        final_path = generate_path(parent_path=path, day=days)
        create_path(day=days)
        cursor.execute(f'COPY {table} TO \'{final_path}/{filename}.csv\' CSV HEADER;')
        conn.commit()
        return True
    except Exception as e:
        logging.critical(e.__class__)
        logging.critical(e)
        logging.critical('Something went wrong at exporting the table to container storage')
        return False


def close_connection(cursor, conn) -> None:
    cursor.close()
    conn.close()
    if conn.closed == 0:
        logging.info(f'Something went wrong, connection still open {conn.closed}')
        return conn.closed
    logging.info('Both cursor and conn closed successfully')
    return conn.closed


def export():
    table = {1: 'transaction', 2: 'customer'}
    connection_cursor, connection, status = connect_database(database='capstone',
                                            user='root',
                                            password='root',
                                            host='capstone-db')
    days = get_days()

    for _ in range(1,3):
        if status:
            logging.info(f'Working on {table.get(_)}')
            copy_table(cursor=connection_cursor, conn=connection, path=f'/opt/{table.get(_)}', table=table.get(_), days=days)
        else:
            logging.critical('Something went wrong')

    close_connection(cursor=connection_cursor, conn=connection)

    logging.info('Both thables export successfully')

