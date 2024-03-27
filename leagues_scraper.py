#This code scrapes data for the top 25 European football leagues from Transfermarkt website and uploads it to an SQL database. 
#It uses Selenium and BeautifulSoup libraries to collect league information such as number of clubs, players, average age or league value. 
#After creating a DataFrame with this data, unique identifiers are generated for each league, and timestamps are added. 
#Finally, the script establishes a connection with the SQL database and uploads the DataFrame as a table named 'leagues' in the 'tm2024' schema.

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Numeric, TIMESTAMP
import pandas as pd 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
from bs4 import BeautifulSoup
import pyautogui
import datetime

pd.set_option('display.float_format', lambda x: '%.1f' % x)

service = Service()
op = webdriver.ChromeOptions()
driver = webdriver.Chrome(service = service, options = op)
driver.set_window_size(640, 480)

url = 'https://www.transfermarkt.com/wettbewerbe/europa'
driver.get(url)
time.sleep(2)

x_point_of_webpage = 170
y_point_of_webpage = 420
pyautogui.click(x_point_of_webpage, y_point_of_webpage)
time.sleep(2)

soup = BeautifulSoup(driver.page_source, 'lxml')
table_of_leagues = soup.find('table', class_ = 'items')
odd_rows = table_of_leagues.find_all('tr', class_ = 'odd')
even_rows = table_of_leagues.find_all('tr', class_ = 'even')

league_links = []
leagues_countries = []
leagues_number_of_clubs = []
leagues_number_of_players = []
leagues_average_age = []
leagues_foreigners_shares = []
leagues_overal_values = []

def league_data_generator(row_types):
    
    for league in row_types: 
        
        cells_per_league = league.find_all('td', class_ = 'zentriert')
        
        league_links.append('https://www.transfermarkt.com' + league.find('a').get('href'))
        leagues_countries.append(cells_per_league[0].find('img').get('title'))
        leagues_number_of_clubs.append(int(cells_per_league[1].text))
        leagues_number_of_players.append(int(cells_per_league[2].text))
        leagues_average_age.append(float(cells_per_league[3].text))
        leagues_foreigners_shares.append(float(cells_per_league[4].text[:-1].strip()))
        
        league_value = league.find('td', class_ = 'rechts hauptlink').text[1:]
        
        if league_value[-1] == 'n':
            league_value = float(league_value[:-2]) * 1000
        elif league_value[-1] == 'm':
            league_value = float(league_value[:-1]) * 1   
        
        leagues_overal_values.append(league_value)

league_data_generator(odd_rows)
league_data_generator(even_rows)

league_df = pd.DataFrame({'COUNTRY' : leagues_countries, 'NO_OF_CLUBS' : leagues_number_of_clubs, 'NO_OF_PLAYERS' : leagues_number_of_players,
                         'AVG_AGE' : leagues_average_age, 'FOREIGNERS_SH' : leagues_foreigners_shares, 'LEAGUE_VALUE' : leagues_overal_values,
                         'LINK' : league_links}).sort_values(by='LEAGUE_VALUE', ascending = False).reset_index().drop(columns = 'index')

league_id_formula = league_df['COUNTRY'].str[:3].str.upper() + (league_df.index + 1).astype(str).str.zfill(2)
league_df.insert(0, 'COUNTRY_ID', league_id_formula)
league_df['CREATED_AT'] = datetime.datetime.now()
league_df['LEAGUE_VALUE'] = league_df['LEAGUE_VALUE'].round(1)

server_name = '###\SQLEXPRESS'
database_name = 'TransferMarkt2024'
connection_string = f'mssql+pyodbc://{server_name}/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

engine = create_engine(connection_string)
metadata = MetaData()

leagues =   Table('leagues', metadata,
            Column('ID', Integer, primary_key=True),
            Column('COUNTRY_ID', String(10)),
            Column('COUNTRY', String(50)),
            Column('NO_OF_CLUBS', Integer),
            Column('NO_OF_PLAYERS', Integer),
            Column('AVG_AGE', Numeric(precision=10, scale=2)),
            Column('FOREIGNERS_SH', Numeric(precision=10, scale=2)),
            Column('LEAGUE_VALUE', Integer),
            Column('LINK', String(100)),
            Column('CREATED_AT', TIMESTAMP),
            schema = 'tm2024')

if not metadata.tables.get(leagues.name):
    metadata.create_all(engine)

league_df.to_sql('leagues', schema='tm2024', con=engine, if_exists='replace', index=False)