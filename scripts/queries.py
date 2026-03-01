import pandas as pd
from config import get_mssql_engine, get_sqlite_connection, read_s3_csv


def query_history(db_path='history.db'):
    conn = get_sqlite_connection(db_path)
    df = pd.read_sql('SELECT * FROM history_data', conn)
    conn.close()
    return df


def query_achievements():
    return read_s3_csv('gaming_data/achievements.csv')


def query_players():
    return pd.read_sql('SELECT * FROM players', get_mssql_engine())


def query_games():
    return pd.read_sql('SELECT * FROM games', get_mssql_engine())