#In the program's last phase, it creates three datasets encompassing various aspects of club and player information for all 25 countries in one execution. 
#These datasets cover club details like stadium capacity, player performances including goals and assists, and extra statistics for players not initially included in the database. 
#Subsequently, the program stores these datasets into respective tables within the database for future analysis.

from sqlalchemy import create_engine, MetaData, Table, select, Column, String, Integer, Numeric, DateTime, func
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
import time
from bs4 import BeautifulSoup
from traitlets import default

pd.set_option('display.float_format', lambda x: '%.1f' % x)

server_name = 'XXX\SQLEXPRESS'
database_name = 'TransferMarkt2024'
connection_string = f'mssql+pyodbc://{server_name}/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'

engine = create_engine(connection_string)
metadata = MetaData()
top_clubs = Table('clubs', metadata, autoload_with=engine, schema='tm2024')
players = Table('players_bio', metadata, autoload_with=engine, schema='tm2024')

stmt = select(players.c.PLAYER_ID, players.c.NAME)
players_ids_sql = pd.read_sql(stmt, engine)
players_ids_list = players_ids_sql['PLAYER_ID'].to_list()

service = Service()
op = webdriver.ChromeOptions()
driver = webdriver.Chrome(service = service, options = op)

teams_id_t = [] 
countries_id_t = []
teams_names_t = [] 
in_league_since_t = [] 
foreigners_share_t = []
national_players_t = [] 
stadiums_t = [] 
seats_t = []

players_ids_ni = []
countries_id_ni = []
potential_club_ni = []
potential_club_id_ni = []
names_ni = []
positions_ni = []
ages_ni = []
nationality_1_ni = []
nationality_2_ni = []
on_pitch_ni = []
minutes_ni = []

player_ids_s = []
player_names_s = []
countries_id_s = []
club_ids_s = []
teams_names_s = []
current_flg_s = []
in_team_s = [] 
on_pitch_s = [] 
goals_s = []
assists_s = [] 
yellow_cards_s = [] 
double_yellow_cards_s = []
red_cards_s = [] 
from_bench_s = [] 
to_bench_s = [] 
point_per_game_s = [] 
time_played_s = []

def row_handler(row_type):
    
    for player in row_type:
        
        player_id = int(player.find('td', class_ = 'hauptlink').find('a').get('href').split('/')[-1])
        statistics = player.find_all('td', class_ = 'zentriert')

        player_ids_s.append(player_id)
        player_names_s.append(player.find('div', class_ = 'di nowrap').find('span', class_ = 'hide-for-small').text)
        countries_id_s.append(country_id)
        club_ids_s.append(int(clubs_links[link_no].split('/')[-3]))
        teams_names_s.append(team_details_page.find('h1', class_ = 'data-header__headline-wrapper data-header__headline-wrapper--oswald').text.strip())
        
        if player_id not in players_ids_list:
            current_flg_s.append(-1)
        elif player_id not in players_ids_club_list: 
            current_flg_s.append(0)
        else:
            current_flg_s.append(1)

        in_team_s.append(statistic_handler(statistics,3))
        on_pitch_s.append(statistic_handler(statistics,4))
        goals_s.append(statistic_handler(statistics,5))
        assists_s.append(statistic_handler(statistics,6))
        yellow_cards_s.append(statistic_handler(statistics,7))
        double_yellow_cards_s.append(statistic_handler(statistics,8))
        red_cards_s.append(statistic_handler(statistics,9))
        from_bench_s.append(statistic_handler(statistics,10))
        to_bench_s.append(statistic_handler(statistics,11))
        
        players_ppg = statistic_handler(statistics,12,float,1)
        if players_ppg > 3.0:
            players_ppg = -1.0
        
        point_per_game_s.append(float(players_ppg))

        try:
            minutes = player.find('td', class_ = 'rechts').text[:-1].replace('.','')
            if minutes == '-' or minutes == 'Not used during this season':
                minutes = 0
            time_played_s.append(int(minutes))
        except:
            minutes = 0
            time_played_s.append(int(minutes))
        
        if player_id not in players_ids_list:
            players_ids_ni.append(int(player_id))
            countries_id_ni.append(country_id)
            potential_club_ni.append(team_details_page.find('h1', class_ = 'data-header__headline-wrapper data-header__headline-wrapper--oswald').text.strip())
            potential_club_id_ni.append(int(clubs_links[link_no].split('/')[-3]))
            names_ni.append(player.find('div', class_ = 'di nowrap').find('span', class_ = 'hide-for-small').text)
            positions_ni.append(player.find('table', class_ = 'inline-table').find_all('tr')[1].text)
            ages_ni.append(statistic_handler(statistics,1,int,-1))
            nationality_1_ni.append(player.find_all('td', class_ = 'zentriert')[2].find_all('img')[0].get('title'))
            
            try:
                nationality_2_ni.append(player.find_all('td', class_ = 'zentriert')[2].find_all('img')[1].get('title'))
            except:
                nationality_2_ni.append('n/a')
                
            on_pitch_ni.append(statistic_handler(statistics,4))
            minutes_ni.append(int(minutes))

def statistic_handler(list,no, type=int,default_value=0):
    try:
        metric = list[no].text
        if metric == '-' or metric == 'Not used during this season' or metric == 'Not in squad during this season':
            metric = default_value
        elif metric == '0,00':
            metric = float(default_value)
    except:
        metric = 0
    return type(metric)

countries = ['England', 'Spain', 'Italy', 'Germany', 'France', 'Portugal' , 'Netherlands', 'Türkiye', 'Belgium', 'Russia', 'Greece', 'Austria', 'Scotland',
              'Switzerland', 'Ukraine', 'Denmark', 'Poland', 'Serbia', 'Czech Republic', 'Croatia', 'Sweden', 'Romania', 'Norway', 'Bulgaria', 'Cyprus']

for country in countries:

    stmt = select(top_clubs.c.LINK, top_clubs.c.TEAM, top_clubs.c.COUNTRY_ID).where(top_clubs.c.COUNTRY == country)
    clubs_df = pd.read_sql(stmt, engine)
    clubs_links = clubs_df['LINK'].to_list()
    club_names = clubs_df['TEAM'].to_list()
    country_id = clubs_df['COUNTRY_ID'].to_list()[0]

    for link_no in range(len(clubs_links)):
        driver.get(clubs_links[link_no].replace('startseite', 'leistungsdaten').replace('saison_id/2023', 'reldata/%262023/plus/1'))
        team_games_page = BeautifulSoup(driver.page_source, 'lxml')
        
        team_details_page = team_games_page.find('header', class_= 'data-header')
        other_date = team_details_page.find_all('span', class_ = 'data-header__content')
        
        teams_id_t.append(int(clubs_links[link_no].split('/')[-3]))
        countries_id_t.append(country_id)
        teams_names_t.append(team_details_page.find('h1', class_ = 'data-header__headline-wrapper data-header__headline-wrapper--oswald').text.strip())
        
        try:
            exp = team_details_page.find('div', class_ = 'data-header__box--big').find_all('span', class_ = 'data-header__content')[2].text.strip()
            if exp[-5:] == 'years':
                exp = exp[:-6]
            elif exp[-4:] == 'year':
                exp = exp[:-5]
            in_league_since_t.append(int(exp))
        except:
            in_league_since_t.append(-1)
        
        try:
            foreign_players_share = other_date[5].text.strip().split('\xa0')[2].replace(' %','')
            if foreign_players_share == '':
                foreign_players_share = 0
            foreigners_share_t.append(float(foreign_players_share))
        except:
            foreigners_share_t.append(float(-1))
        
        try:
            national_players_t.append(int(other_date[6].text.strip()))
        except:
            national_players_t.append(-1)
        
        try:
            stadium_name = other_date[7].text.strip().split('\xa0')[0]
            if stadium_name == '-':
                stadium_name = 'n/a'
            stadiums_t.append(stadium_name)
        except:
            stadiums_t.append('n/a')    
        
        try:    
            seats_t.append(int(other_date[7].text.strip().split('\xa0')[2].replace('.','',)[:-6]))
        except:
            seats_t.append(-1)

        
        
        stmt = select(players.c.PLAYER_ID, players.c.NAME).where(players.c.TEAM_ID == clubs_links[link_no].split('/')[-3])
        players_ids_club_sql = pd.read_sql(stmt, engine)
        players_ids_club_list = players_ids_club_sql['PLAYER_ID'].to_list()
        
        games_details_page = team_games_page.find('div', class_= 'responsive-table')
        games_details_rows_even = games_details_page.find_all('tr', class_ = 'even')
        games_details_rows_odd = games_details_page.find_all('tr', class_ = 'odd')
        
        row_handler(games_details_rows_even)
        row_handler(games_details_rows_odd)
        time.sleep(2)

club_details_df = pd.DataFrame({        'COUNTRY_ID' : countries_id_t, 'TEAM_ID' : teams_id_t, 'TEAM' : teams_names_t, 'IN_LEAGUE_SINCE' : in_league_since_t,
                                        'FOREIGNERS_SH' : foreigners_share_t, 'NATIONAL_PLAYERS' : national_players_t, 'STADIUM' : stadiums_t, 
                                        'SEATS' : seats_t})

players_performances_df = pd.DataFrame({'PLAYER_ID' : player_ids_s, 'NAME' : player_names_s, 'COUNTRY_ID' : countries_id_s,
                                        'TEAM_ID' : club_ids_s, 'TEAM' : teams_names_s, 'CURRENT_FLG' : current_flg_s, 
                                        'IN_TEAM' : in_team_s, 'ON_PITCH' : on_pitch_s, 'GOALS' : goals_s, 'ASSISTS' : assists_s,
                                        'YELLOW_CARDS' : yellow_cards_s, 'DOUBLE_YELLOW_CARDS' : double_yellow_cards_s,
                                        'RED_CARDS' : red_cards_s, 'FROM_BENCH' : from_bench_s, 'TO_BENCH' : to_bench_s,
                                        'PTS_PER_GAME' : point_per_game_s, 'TIME_PLAYED' : time_played_s})

players_not_inclued_df = pd.DataFrame({'PLAYER_ID' : players_ids_ni, 'NAME' : names_ni, 'COUNTRY_ID' : countries_id_ni,
                                       'POTENTIAL_TEAM_ID' : potential_club_id_ni, 'POTENTIAL_TEAM' : potential_club_ni,
                                       'POSITION' : positions_ni, 'AGE' : ages_ni, 
                                       'NATIONALITY_1' : nationality_1_ni, 'NATIONALITY_2' : nationality_2_ni,
                                       'ON_PITCH' : on_pitch_ni, 'TIME_PLAYED' : minutes_ni})

positions_dictionary = {'Goalkeeper' : 'GK', 'Centre-Back' : 'DEF', 'Defender' : 'DEF', 'Left-Back' : 'DEF', 'Right-Back' : 'DEF',
                        'Defensive Midfield' : 'MID', 'Attacking Midfield' : 'MID', 'Right Midfield' : 'MID', 'Central Midfield' : 'MID', 'Left Midfield' : 'MID', 'Midfielder' : 'MID', 'midfield' : 'MID',
                        'Centre-Forward' : 'FOR', 'Left Winger' : 'FOR', 'Right Winger' : 'FOR', 'Second Striker' : 'FOR', 'Striker' : 'FOR'}
players_not_inclued_df.insert(6, 'OVERAL_POSITION', players_not_inclued_df['POSITION'].map(positions_dictionary))



clubs_extra_det =   Table('clubs_details', metadata,
                    Column('COUNTRY_ID', String(10)),
                    Column('TEAM_ID', Integer),
                    Column('TEAM', String(50)),
                    Column('IN_LEAGUE_SINCE', Integer),
                    Column('FOREIGNERS_SH', Numeric(precision=10, scale=2)),
                    Column('NATIONAL_PLAYERS', Integer),
                    Column('STADIUM', String(100)),
                    Column('SEATS', Integer),
                    Column('CREATED_AT', DateTime, server_default=func.current_timestamp()),
                    schema = 'tm2024')

performances =      Table('performances', metadata,
                    Column('PLAYER_ID', Integer),
                    Column('NAME', String(50)),
                    Column('COUNTRY_ID', String(10)),
                    Column('TEAM_ID', Integer),
                    Column('TEAM', String(50)),
                    Column('CURRENT_FLG', Integer),
                    Column('IN_TEAM', Integer),
                    Column('ON_PITCH', Integer),
                    Column('GOALS', Integer),
                    Column('ASSISTS', Integer),
                    Column('YELLOW_CARDS', Integer),
                    Column('DOUBLE_YELLOW_CARDS', Integer),
                    Column('RED_CARDS', Integer),
                    Column('FROM_BENCH', Integer),
                    Column('TO_BENCH', Integer),
                    Column('PTS_PER_GAME', Numeric(precision=10, scale=2)),
                    Column('TIME_PLAYED', Integer),
                    Column('CREATED_AT', DateTime, server_default=func.current_timestamp()),
                    schema = 'tm2024')

players_ni =        Table('players_ni', metadata,
                    Column('PLAYER_ID', Integer),
                    Column('NAME', String(50)),
                    Column('COUNTRY_ID', String(10)),
                    Column('POTENTIAL_TEAM_ID', Integer),
                    Column('POTENTIAL_TEAM', String(50)),
                    Column('POSITION', String(25)),
                    Column('OVERAL_POSITION', String(5)),
                    Column('AGE', Integer),
                    Column('NATIONALITY_1', String(50)),
                    Column('NATIONALITY_2', String(50)),
                    Column('ON_PITCH', Integer),
                    Column('TIME_PLAYED', Integer),
                    Column('CREATED_AT', DateTime, server_default=func.current_timestamp()),
                    schema = 'tm2024')

if not metadata.tables.get(players_ni.name):
    metadata.create_all(engine)

club_details_df.to_sql('clubs_details', schema='tm2024', con=engine, if_exists='append', index=False)
players_performances_df.to_sql('performances', schema='tm2024', con=engine, if_exists='append', index=False)
players_not_inclued_df.to_sql('players_ni', schema='tm2024', con=engine, if_exists='append', index=False)