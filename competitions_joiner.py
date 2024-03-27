#The script imports previously exported CSV files containing DataFrame data, merges them iteratively into three combined DataFrames, and removes duplicate records. 
#Subsequently, it uploads them to a database using SQLAlchemy. 
#The DataFrames consist of match dates, results, and team performance, each uploaded to its corresponding table in the 'tm2024' database schema.

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, func

letters = ['a', 'b', 'c'] 
games_date_dict = {}
games_results_dict = {}
teams_performance_dict = {}

for letter in letters:
    games_date_dict[letter] = (pd.read_csv(f'C:/folder/games_date_df_{letter}.csv', sep='|', 
                                        encoding = 'UTF-8', keep_default_na=False))
    games_results_dict[letter] = (pd.read_csv(f'C:/folder/games_results_df_20_{letter}.csv', sep='|', 
                                        encoding = 'UTF-8', keep_default_na=False))
    teams_performance_dict[letter] = (pd.read_csv(f'C:/folder/team_performance_df_{letter}.csv', sep='|', 
                                        encoding = 'UTF-8', keep_default_na=False))
    
games_date_df = (pd.concat( objs = [games_date_dict['a'], games_date_dict['b'], games_date_dict['c']]).
                 drop_duplicates(subset='GAME_ID', keep='first').reset_index().drop(columns='index'))

games_results_df = (pd.concat(objs = [games_results_dict['a'], games_results_dict['b'], games_results_dict['c']]).
                                     drop_duplicates(subset='GAME_ID', keep='first').reset_index().drop(columns = 'index'))

team_performance_df = (pd.concat(objs = [teams_performance_dict['a'], teams_performance_dict['b'], teams_performance_dict['c']]).
                       reset_index().drop(columns = 'index'))


server_name = 'XXX\SQLEXPRESS'
database_name = 'TransferMarkt2024'
connection_string = f'mssql+pyodbc://{server_name}/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

engine = create_engine(connection_string)
metadata = MetaData()

games_date =        Table('games_date', metadata,
                    Column('GAME_ID', Integer),
                    Column('COUNTRY_ID', String(10)),
                    Column('HOME_TEAM_ID', Integer),
                    Column('HOME_TEAM', String(100)),
                    Column('AWAY_TEAM_ID', Integer),
                    Column('AWAY_TEAM', String(100)),
                    Column('GAME_YEAR', Integer),
                    Column('GAME_DOW', Integer),
                    Column('GAME_MONTH', Integer),
                    Column('GAME_DOM', Integer),
                    Column('GAME_TIME', String(25)),
                    Column('ATTENDANCE', Integer),
                    Column('GAME_LINK', String(100)),
                    Column('CREATED_AT', DateTime, server_default=func.current_timestamp()),
                    schema = 'tm2024')

games_result =      Table('games_result', metadata,
                    Column('GAME_ID', Integer),
                    Column('COUNTRY_ID', String(10)),
                    Column('HOME_TEAM_ID', Integer),
                    Column('HOME_TEAM', String(100)),
                    Column('AWAY_TEAM_ID', Integer),
                    Column('AWAY_TEAM', String(100)),
                    Column('COMPETITION', String(50)),
                    Column('MATCHDAY', String(50)),
                    Column('RESULT', String(50)),
                    Column('>90_FLG', Integer),
                    Column('HOME_GOALS', Integer),
                    Column('AWAY_GOALS', Integer),
                    Column('CREATED_AT', DateTime, server_default=func.current_timestamp()),
                    schema = 'tm2024')

games_team =        Table('games_team', metadata,
                    Column('GAME_ID', Integer),
                    Column('COUNTRY_ID', String(10)),
                    Column('HOME_FLG', Integer),
                    Column('TEAM_ID', Integer),
                    Column('TEAM', String(100)),
                    Column('OPPONENT_ID', Integer),
                    Column('OPPONENT', String(100)),
                    Column('COMPETITION', String(50)),
                    Column('MATCHDAY', String(50)),
                    Column('TEAM_PLACE', Integer),
                    Column('OPPONENT_PLACE', Integer),
                    Column('GOALS_SCORED', Integer),
                    Column('GOALS_LOST', Integer),
                    Column('FORMATION', String(50)),
                    Column('COACH', String(50)),
                    Column('POINTS', Integer),
                    Column('POINTS_SUM', Integer),
                    Column('GOALS_SCORED_ALL', Integer),
                    Column('GOALS_LOST_ALL', Integer),
                    Column('GOALS_DIFF', Integer),
                    Column('CREATED_AT', DateTime, server_default=func.current_timestamp()),
                    schema = 'tm2024')

if not metadata.tables.get(games_date.name):
    metadata.create_all(engine)

games_date_df.to_sql('games_date', schema='tm2024', con=engine, if_exists='append', index=False)
games_results_df.to_sql('games_result', schema='tm2024', con=engine, if_exists='append', index=False)
team_performance_df.to_sql('games_team', schema='tm2024', con=engine, if_exists='append', index=False)