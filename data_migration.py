import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

import sqlalchemy as db

from sqlalchemy import event
from memory_profiler import profile


READ_DB_URL = 'mysql+mysqlconnector://dubizzle:dubizzle@127.0.0.1:3308/employees'
WRITE_DB_URL = 'mysql+mysqlconnector://dubizzle:dubizzle@127.0.0.1:3306/dubizzle'

read_engine= create_engine(READ_DB_URL, echo=False)
write_engine = create_engine(WRITE_DB_URL, echo=True)

@event.listens_for(write_engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True

@profile
def migrate(from_table, to_table, pk='id', batch=50000, dry_run=True, **args):
    offset_val = 0
    with read_engine.connect().execution_options(autocommit=True) as read_conn:
        with write_engine.connect().execution_options(autocommit=True) as _:            
            while offset_val or offset_val == 0:
                query = f'SELECT {pk}, emp_no, from_date, to_date FROM {from_table} where {pk} > {offset_val} order by {pk} limit {batch}'
                offset_val = read_conn.execute(text(f'select max({pk}) from ({query}) as res;')).scalar()
                data = pd.read_sql(text(query), con = read_conn, index_col=[pk])
                if not dry_run:
                    data.to_sql(to_table, con=write_engine, schema='dubizzle', if_exists='append', index=False)



if __name__ == '__main__':
    migrate('salaries', 'salaries_copied_test_new', dry_run=False)
