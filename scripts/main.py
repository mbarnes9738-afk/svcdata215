# Imports

from config import load_csv_to_sqlite, build_gaming_df, print_drivers
from queries import query_history, query_achievements, query_players, query_games

# Read history.csv from local disk and write it

load_csv_to_sqlite(r'C:\Users\school\Downloads\history.csv')

# Query history_data from SQLite, achievements CSV from S3, and players and games tables from SQL Server

history_df      = query_history()
achievements_df = query_achievements()
players_df      = query_players()
games_df        = query_games()

# Merge all four df's into one mega df

gaming_df = build_gaming_df(history_df, players_df, achievements_df, games_df)
print(f"Final gaming_df shape: {gaming_df.shape}")
print(f"Columns: {gaming_df.columns.tolist()}")

