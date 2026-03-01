import pandas as pd
import sqlalchemy
import os
from io import StringIO
import boto3
import sqlite3
import warnings
warnings.filterwarnings("ignore", category=sqlalchemy.exc.SAWarning)
import pyodbc


# Creds

sql_user     = os.environ.get('SQL_USER')
sql_password = os.environ.get('SQL_PASSWORD')
aws_access_key = os.environ.get('AWS_ACCESS_KEY')
aws_secret_key = os.environ.get('AWS_SECRET_KEY')
aws_bucket     = os.environ.get('AWS_BUCKET')
region = 'us-east-2'


# Connection functions

def get_mssql_engine():
    """Create and return a SQLAlchemy engine for the Azure SQL Server."""
    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc://{sql_user}:{sql_password}@"
        "svc-data215.database.windows.net:1433/svc_sql_db"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes"
        "&TrustServerCertificate=no"
        "&Connection+Timeout=30"
    )
    return engine


def get_s3_client():
    """Create and return a boto3 S3 client."""
    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )


def get_sqlite_connection(db_path='history.db'):
    """Return a SQLite3 connection to the given database """
    return sqlite3.connect(db_path)


# Data loading functions

def load_csv_to_sqlite(csv_path, table_name='history_data', db_path='history.db'):
    """
    Read a CSV file and write it to a SQLite database table.
    Replaces the table if it already exist
    """
    df = pd.read_csv(csv_path)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    return df


def read_sqlite_table(table_name='history_data', db_path='history.db'):
    conn = get_sqlite_connection(db_path)
    df = pd.read_sql(f'SELECT * FROM {table_name}', conn)
    conn.close()
    return df


def read_s3_csv(key, bucket=None):
    """
    Download a CSV from S3 and return it as a df
    """
    if bucket is None:
        bucket = aws_bucket
    client = get_s3_client()
    obj = client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'])


def read_mssql_table(table_name):
    """Read a full table from SQL Server and return it as a df"""
    engine = get_mssql_engine()
    return pd.read_sql(f'SELECT * FROM {table_name}', engine)


# Merge function 

def build_gaming_df(history_df, players_df, achievements_df, games_df):
    """
    Merge the four source df's into a single gaming df
    """
    df = history_df.merge(players_df, on='playerid', how='left')
    df = df.merge(achievements_df, on='achievementid', how='left')
    df = df.merge(games_df, on='gameid', how='left')
    return df


#exa

def print_df_info(*named_dfs):
    """
    Print column names for each df passed in.
    """
    for name, df in named_dfs:
        print(f"{name} columns: {df.columns.tolist()}")