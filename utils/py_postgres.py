'''
Python Postgres util
'''

import psycopg2

conns = {}

def connect(config, name='default'):
    try:
        conn_inst = psycopg2.connect(**config)
        conns[name] = conn_inst.cursor()
    except Exception as error:
        raise RuntimeError(f'Postgres util: connection error: {error}')


def query(query_str, data=None, headers=None, name='default'):
    if name not in conns:
        raise ValueError(f'Postgres util: haven\'t yet setup connection: {name}')
    try:
        conns[name].execute(query_str, data)
        raw_results = conns[name].fetchall()
    except Exception as error:
        raise RuntimeError(f'Postgres util: query error: {error}')

    if headers is not None:
        results = [dict(zip(headers, row)) for row in raw_results]
    else:
        results = raw_results

    return results
