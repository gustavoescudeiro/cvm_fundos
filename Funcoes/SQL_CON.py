import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import sqlite3

def Conn(usuario = None, senha = None):

    # @TODO Ideal colocar em variaveis de ambiente
    if usuario is None:
        usuario = "postgres"

    if senha is None:
        senha = "postgres"

    # usuario:senha
    conn_string = f'postgresql://{usuario}:{senha}@localhost:5432/postgres'

    # db = create_engine(conn_string)
    # conn = db.connect()

    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    # cursor = conn.cursor()

    return conn


def Db_insert(df = None, tabela = None, usuario = None, senha = None):

    # @TODO Ideal colocar em variaveis de ambiente
    if usuario is None:
        usuario = "postgres"

    if senha is None:
        senha = "postgres"

    # usuario:senha
    conn_string = f'postgresql://{usuario}:{senha}@localhost:5432/postgres'

    db = create_engine(conn_string)
    conn = db.connect()

    df.to_sql(tabela, con=conn, if_exists='append',
               index=False, chunksize=1000)
    conn = psycopg2.connect(conn_string
                            )
    conn.autocommit = True
    cursor = conn.cursor()

    # conn.commit()
    conn.close()