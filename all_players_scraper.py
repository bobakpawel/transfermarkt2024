
#This code utilizes nested 'for' loops to gather data from various club pages. 
#The outer loop iterates through a list of countries, queries a SQL database to obtain links to all clubs within each country. 
#While the inner loop then connects to each club page using Chrome WebDriver, scrapes the required data, and stores it in two separate DataFrames. 
#One DataFrame contains biometric data such as birth date, nationality, and preferred foot. 
#The other DataFrame holds formal information like contract duration and player value. 
#After processing, the data is uploaded to an SQL database as tables named 'players_bio' and 'players_form' in the 'tm2024' schema.

from sqlalchemy import create_engine, MetaData, Table, select, Column, Integer, String, Numeric, DateTime, func
import pandas as pd 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
from bs4 import BeautifulSoup

pd.set_option('display.float_format', lambda x: '%.1f' % x)

server_name = 'XXX\SQLEXPRESS'
database_name = 'TransferMarkt2024'
connection_string = f'mssql+pyodbc://{server_name}/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

engine = create_engine(connection_string)
metadata = MetaData()

top_clubs = Table('clubs', metadata, autoload_with=engine, schema='tm2024')

player_numbers = []
players = []
player_links = []
player_ids = []
player_clubs = []
player_clubs_ids = []
player_positions = []
age = []
year_birth = []
month_birth = []
day_birth = []
nationality_1 = []
nationality_2 = []
heights = []
main_foot = []
year_joined = []
previous_club = []
previous_club_ids = []
contract_expiry_year = [] 
contract_expiry_month = []
player_values = []

def player_scraper(row_type):

    for player in row_type:
        
        player_details = player.find_all('td', class_ = 'zentriert')
        
        try:
            player_number = player.find('div', class_ = 'rn_nummer').text 
            if player_number == '-':
                player_number = '-1'
            player_numbers.append(player_number)
        except:
            player_numbers.append('-1')
            
        try:
            players.append(player.find('td', class_ = 'hauptlink').find('a').text.strip())
        except:
            players.append('n/a')
            
        try:
            player_links.append('https://www.transfermarkt.com' + player.find('td', class_ = 'hauptlink').find('a').get('href'))
        except:
            player_links.append('n/a')
            
        try:
            player_ids.append(int(player.find('td', class_ = 'hauptlink').find('a').get('href').split('/')[-1]))
        except:
            player_ids.append(-1)
            
        player_clubs.append(team_name)
        player_clubs_ids.append(int(clubs_links[club_no].replace('startseite','kader').split('/')[-3]))
            
        try:
            player_positions.append(player.find('table', class_ = 'inline-table').find_all('td')[2].text.strip())
        except:
            player_positions.append('n/a')

        try:
            age.append(int(player_details[1].text[:-1].split('(')[1]))
        except:
            age.append(-1)
            
        try:
            year_birth.append(int(player_details[1].text[:-1].split('(')[0].split(',')[1].strip()))
        except:
            year_birth.append(-1)
            
        try:
            month_birth.append(player_details[1].text[:-1].split('(')[0].split(',')[0].strip().split(' ')[0])
        except:
            month_birth.append('n/a')
            
        try:
            day_birth.append(int(player_details[1].text[:-1].split('(')[0].split(',')[0].strip().split(' ')[1]))
        except:
            day_birth.append(-1)
            
        try:
            nationality_1.append(player_details[2].find_all('img')[0].get('title'))
        except:
            nationality_1.append('n/a')
            
        try:
            nationality_2.append(player_details[2].find_all('img')[1].get('title'))
        except:
            nationality_2.append('n/a')
        
        try:
            height = player_details[3].text
            
            if height == '-':
                height = -1
            if height[-1] == 'm':
                heights.append(float(height[:-1].replace(',','.')))
                
            else:
                heights.append(float(height.replace(',','.')))
        except:
            heights.append(float(-1.00))
            
        try:
            lead_foot = player_details[4].text
            
            if lead_foot == '' or lead_foot == '\xa0':
                lead_foot = 'n/a'
                
            main_foot.append(lead_foot)
        except:
            main_foot.append('n/a')
        
        try:
            year_joined.append(int(player_details[5].text.split(',')[1].strip()))
        except:
            year_joined.append(-1)
            
        try:
            previous_club_name = player_details[6].find('a').get('title')
            
            if ':' in previous_club_name:
                previous_club_name = previous_club_name.split(':')[0].strip()
            elif '€' in previous_club_name:
                previous_club_name = previous_club_name.split('€')[0].strip()
            
            previous_club.append(previous_club_name)
        except:
            previous_club.append('n/a')
            
        try:
            previous_club_ids.append(int(player_details[6].find('a').get('href').split('/')[-3]))
        except:
            previous_club_ids.append(-1)
            
        try:
            contract_expiry_year.append(int(player_details[7].text.split(',')[1]))
        except:
            contract_expiry_year.append(-1)
        
        try:
            contract_expiry_month.append(player_details[7].text.split(',')[0].split(' ')[0])
        except:
            contract_expiry_month.append('n/a')
            
        try:
            player_values.append(player.find('td', class_ = 'rechts hauptlink').find('a').text[1:])
        except:
            player_values.append('-1')

service = Service()
op = webdriver.ChromeOptions()
driver = webdriver.Chrome(service = service, options = op)

countries = ['England', 'Spain', 'Italy', 'Germany', 'France', 'Portugal', 'Netherlands', 'Türkiye', 'Belgium', 'Russia', 'Greece', 'Austria', 'Scotland',
              'Switzerland', 'Ukraine', 'Denmark', 'Poland', 'Serbia', 'Czech Republic', 'Croatia', 'Sweden', 'Romania', 'Norway', 'Bulgaria', 'Cyprus']

for country in countries: 

    stmt = select(top_clubs.c.LINK, top_clubs.c.TEAM).where(top_clubs.c.COUNTRY == country)
    clubs_df = pd.read_sql(stmt, engine)
    clubs_links = clubs_df['LINK'].to_list()
    club_names = clubs_df['TEAM'].to_list()

    for club_no in range(len(clubs_links)):

        driver.get(clubs_links[club_no].replace('startseite','kader') + '/plus/1')

        team_page = BeautifulSoup(driver.page_source, 'lxml')
        team_table = team_page.find('table', class_ = 'items')
        odd_players = team_table.find_all('tr', class_ = 'odd')
        even_players = team_table.find_all('tr', class_ = 'even')
        team_name = team_page.find('h1', class_ = 'data-header__headline-wrapper data-header__headline-wrapper--oswald').text.strip()

        player_scraper(odd_players)
        player_scraper(even_players)

        time.sleep(2)
            
for value in range(len(player_values)):
    if player_values[value][-1] == 'm':
        player_values[value] = float(player_values[value][:-1]) * 1.0
    elif player_values[value][-1] == 'k':
        player_values[value] = float(player_values[value][:-1]) / 1000.0
        
players_bio_df = pd.DataFrame({     'PLAYER_ID' : player_ids, 'NAME' : players, 'TEAM_ID' : player_clubs_ids, 'TEAM' : player_clubs, 
                                    'AGE' : age, 'BIRTH_YEAR' : year_birth, 'BIRTH_MONTH' : month_birth, 'BIRTH_DAY' : day_birth,
                                    'NUMBER' : player_numbers,  'POSITION' : player_positions, 
                                    'NATIONALITY_1' : nationality_1, 'NATIONALITY_2' : nationality_2, 'HEIGHT' : heights, 
                                    'MAIN_FOOT' : main_foot, 'LINK' : player_links,} )

players_formal_df = pd.DataFrame({  'PLAYER_ID' : player_ids, 'NAME' : players, 'TEAM_ID' : player_clubs_ids, 'TEAM' : player_clubs,
                                    'JOINED_YEAR' : year_joined, 'PREVIOUS_TEAM_ID' : previous_club_ids, 'PREVIOUS_TEAM' : previous_club, 
                                    'CONTRACT_EXPIRY_YEAR' : contract_expiry_year, 'CONTRACT_EXPIRY_MONTH' : contract_expiry_month, 
                                    'VALUE' : player_values})

month_dictionary = {'Jan' : 1, 'Feb' : 2, 'Mar' : 3, 'Apr' : 4, 'May' : 5, 'Jun' : 6, 'Jul' : 7, 'Aug' : 8,
                    'Sep' : 9, 'Oct' : 10, 'Nov' : 11, 'Dec' : 12, '-' : -1, 'n/a' : -1}
positions_dictionary = {'Goalkeeper' : 'GK', 'Centre-Back' : 'DEF', 'Defender' : 'DEF', 'Left-Back' : 'DEF', 'Right-Back' : 'DEF',
                        'Defensive Midfield' : 'MID', 'Attacking Midfield' : 'MID', 'Right Midfield' : 'MID', 'Central Midfield' : 'MID', 'Left Midfield' : 'MID', 'Midfielder' : 'MID', 'midfield' : 'MID',
                        'Centre-Forward' : 'FOR', 'Left Winger' : 'FOR', 'Right Winger' : 'FOR', 'Second Striker' : 'FOR', 'Striker' : 'FOR'}

players_bio_df['BIRTH_MONTH'] = players_bio_df['BIRTH_MONTH'].map(month_dictionary)
players_bio_df.insert(10, 'OVERAL_POSITION', players_bio_df['POSITION'].map(positions_dictionary))
players_formal_df['CONTRACT_EXPIRY_MONTH'] = players_formal_df['CONTRACT_EXPIRY_MONTH'].map(month_dictionary)
players_formal_df['VALUE'] = players_formal_df['VALUE'].astype('float').round(1)

players_bio =  Table('players_bio', metadata,
                Column('PLAYER_ID', Integer),
                Column('NAME', String(100)),
                Column('TEAM_ID', Integer),
                Column('TEAM', String(50)),
                Column('AGE', Integer),
                Column('BIRTH_YEAR', Integer),
                Column('BIRTH_MONTH', Integer),
                Column('BIRTH_DAY', Integer),
                Column('NUMBER', String(5)),
                Column('POSITION', String(25)),
                Column('OVERAL_POSITION', String(5)),
                Column('NATIONALITY_1', String(50)),
                Column('NATIONALITY_2', String(50)),
                Column('HEIGHT', Numeric(precision=10, scale=2)),
                Column('MAIN_FOOT', String(10)),
                Column('LINK', String(100)),
                Column('CREATED_AT', DateTime, server_default=func.current_timestamp()),
                schema = 'tm2024')

players_form =  Table('players_form', metadata,
                Column('PLAYER_ID', Integer),
                Column('NAME', String(100)),
                Column('TEAM_ID', Integer),
                Column('TEAM', String(100)),
                Column('JOINED_YEAR', Integer),
                Column('PREVIOUS_TEAM_ID', Integer),
                Column('PREVIOUS_TEAM', String(50)),
                Column('CONTRACT_EXPIRY_YEAR',  Integer),
                Column('CONTRACT_EXPIRY_MONTH',  Integer),
                Column('VALUE', Numeric(precision=10, scale=2)),
                Column('CREATED_AT', DateTime, server_default=func.current_timestamp()),
                schema = 'tm2024')

metadata.reflect(bind=engine)
if not metadata.tables.get(players_bio.name):
    metadata.create_all(engine)

players_bio_df.to_sql('players_bio', schema='tm2024', con=engine, if_exists='append', index=False)
players_formal_df.to_sql('players_form', schema='tm2024', con=engine, if_exists='append', index=False)

