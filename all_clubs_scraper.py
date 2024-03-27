#This code iterates through links obtained from an SQL database, connects to each country page one by one, and scrapes club-related data. 
#Then, it processes this data into a DataFrame and uploads it to an SQL database as a table named 'clubs' in the 'tm2024' schema.

from sqlalchemy import create_engine, MetaData, Table, select, Column, Integer, String, Numeric, TIMESTAMP
import pandas as pd 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
from bs4 import BeautifulSoup
import pyautogui
import datetime

pd.set_option('display.float_format', lambda x: '%.1f' % x)

server_name = 'XXX\SQLEXPRESS'
database_name = 'TransferMarkt2024'
connection_string = f'mssql+pyodbc://{server_name}/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

engine = create_engine(connection_string)
metadata = MetaData()

league_tables = Table('leagues', metadata, autoload_with=engine, schema='tm2024')

stmt = select(league_tables.c.LINK, league_tables.c.COUNTRY_ID)
league_tables_df = pd.read_sql(stmt, engine)
league_links = league_tables_df['LINK'].to_list()
league_ids = league_tables_df['COUNTRY_ID'].to_list()

service = Service()
op = webdriver.ChromeOptions()
driver = webdriver.Chrome(service = service, options = op)

countries = []
countries_ids = []
club_links = []
club_names = []
club_squad_sizes = []
club_squad_ages = []
club_foreigners_nos = []
club_avg_values = []
club_overal_values = []

def club_scraper(row_type):
    
    for club in row_type:
        
        club_data_z = club.find_all('td', class_ = 'zentriert')
        club_data_r = club.find_all('td', class_ = 'rechts')
        
        countries.append(country)
        countries_ids.append(league_ids[x])
        club_links.append('https://www.transfermarkt.com' + club.find('a').get('href'))
        club_names.append(club.find('a').get('title'))
        club_squad_sizes.append(club_data_z[1].text)
        club_squad_ages.append(club_data_z[2].text)
        club_foreigners_nos.append(club_data_z[3].text)
        club_avg_values.append(club_data_r[0].text[1:])
        club_overal_values.append(club_data_r[1].text[1:])

x = 0
 
for league in league_links:

    driver.get(league)
    time.sleep(2)

    if x == 0:
        x_point_of_webpage = 286
        y_point_of_webpage = 467
        pyautogui.click(x_point_of_webpage, y_point_of_webpage)
        time.sleep(2)

    league_html_code = BeautifulSoup(driver.page_source, 'lxml')
    country = league_html_code.find('div', class_ = 'data-header__club-info').find('a').text.strip()
    league_table = league_html_code.find('table', class_ = 'items')
    clubs_lines_odds = league_table.find_all('tr', class_ = 'odd')
    clubs_lines_evens = league_table.find_all('tr', class_ = 'even')

    club_scraper(clubs_lines_odds)
    club_scraper(clubs_lines_evens)
    
    x += 1
    time.sleep(2)

for value in range(len(club_avg_values)):
    if club_avg_values[value][-1] == 'n':
        club_avg_values[value] = float(club_avg_values[value][:-2]) * 1000
    elif club_avg_values[value][-1] == 'm':
        club_avg_values[value] = float(club_avg_values[value][:-1]) * 1
    elif club_avg_values[value][-1] == 'k':
        club_avg_values[value] = float(club_avg_values[value][:-1]) / 1000
        
    if club_overal_values[value][-1] == 'n':
        club_overal_values[value] = float(club_overal_values[value][:-2]) * 1000
    elif club_overal_values[value][-1] == 'm':
        club_overal_values[value] = float(club_overal_values[value][:-1]) 
        
clubs_df = pd.DataFrame({'COUNTRY_ID' : countries_ids, 'COUNTRY' : countries, 'TEAM' : club_names, 'NO_OF_PLAYERS' : club_squad_sizes, 'AVG_AGE' : club_squad_ages,
                       'NO_OF_FOREIGNERS' : club_foreigners_nos, 'AVG_VALUE' : club_avg_values, 'OVERAL_VALUE' : club_overal_values,  'LINK' : club_links})

clubs_df.insert(2,'TEAM_ID', clubs_df['LINK'].str.split('/').str[-3])
clubs_df = clubs_df.astype({'AVG_AGE' : float, 'TEAM_ID' : int, 'NO_OF_FOREIGNERS' : int, 'NO_OF_PLAYERS' : int, 'OVERAL_VALUE' : float, 'AVG_VALUE' : float})
clubs_df['CREATED_AT'] = datetime.datetime.now()
clubs_df['AVG_VALUE'] = clubs_df['AVG_VALUE'].round(1)
clubs_df['OVERAL_VALUE'] = clubs_df['OVERAL_VALUE'].round(1)

clubs =     Table('clubs', metadata,
            Column('COUNTRY_ID', String(10)),
            Column('COUNTRY', String(25)),
            Column('TEAM_ID', Integer),
            Column('TEAM', String(50)),
            Column('NO_OF_PLAYERS', Integer),
            Column('AVG_AGE', Numeric(precision=10, scale=2)),
            Column('NO_OF_FOREIGNERS', Integer),
            Column('AVG_VALUE', Numeric(precision=10, scale=2)),
            Column('OVERAL_VALUE', Numeric(precision=10, scale=2)),
            Column('LINK', String(100)),
            Column('CREATED_AT', TIMESTAMP),
            schema = 'tm2024')

if not metadata.tables.get(clubs.name):
    metadata.create_all(engine)

clubs_df.to_sql('clubs', schema='tm2024', con=engine, if_exists='replace', index=False)