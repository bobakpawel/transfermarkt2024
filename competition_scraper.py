#The script retrieves match data, including dates, results, and team performance, by iterating through club links stored in a database. 
#It divides countries into three groups to minimize potential errors. 
#It creates 3x DFs: one focusing on match dates and times, another on match results and team standings, and the last one on team performance. 
#The script conducts various computations, such as converting time to a 24-hour format, and exports the DataFrames to CSV files for further analysis.

from sqlalchemy import create_engine, MetaData, Table, select
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

service = Service()
op = webdriver.ChromeOptions()
driver = webdriver.Chrome(service = service, options = op)

competitions = []
country_ids = []
competitions_round = []
source_team_id = []
competitions_year = []
competitions_dayofweek = []
competitions_month = []
competitions_day = []
competitions_time = []
home_teams_name = []
home_teams_id = []
home_teams_position = []
away_teams_name = []
away_teams_id = []
away_teams_position = []
formations = []
coaches = []
attendances = []
games_results = []
games_links = []

countries_a = ['England', 'Spain', 'Italy', 'Germany', 'France', 'Portugal', 'Netherlands', 'Türkiye', 'Belgium']
countries_b = ['Russia', 'Greece', 'Austria', 'Scotland', 'Switzerland', 'Ukraine', 'Denmark', 'Poland']
countries_c = ['Serbia', 'Czech Republic', 'Croatia', 'Sweden', 'Romania', 'Norway', 'Bulgaria', 'Cyprus']

letter = 'c'

for country in countries_c:

    stmt = select(top_clubs.c.LINK, top_clubs.c.TEAM, top_clubs.c.COUNTRY_ID).where(top_clubs.c.COUNTRY == country)
    clubs_df = pd.read_sql(stmt, engine)
    clubs_links = clubs_df['LINK'].to_list()
    club_names = clubs_df['TEAM'].to_list()
    country_id = clubs_df['COUNTRY_ID'].to_list()[0]

    for link_no in range(len(clubs_links)):

        driver.get(clubs_links[link_no].replace('startseite','vereinsspielplan') + '/heim_gast/plus/plus/1')
        team_games_page = BeautifulSoup(driver.page_source, 'lxml')
        team_games_table = team_games_page.find('div', class_ = 'responsive-table')
        rows = team_games_table.find_all('tr')
        for row in rows[1:]:
            cells = row.find_all('td')
            not_first_team_excluder = cells[1].find('a').text
            if not_first_team_excluder == '':
                country_ids.append(country_id)
                competitions.append(cells[0].find('img').get('title'))
                competitions_round.append(cells[2].text.strip())
                
                source_team_id.append(int(clubs_links[link_no].split('/')[-3]))
                
                try:
                    competitions_year.append(int(cells[3].text.strip().split(',')[1].strip()))
                    competitions_dayofweek.append(cells[3].text.strip().split(',')[0].split(' ')[0])
                    competitions_month.append(cells[3].text.strip().split(',')[0].split(' ')[1])
                    competitions_day.append(int(cells[3].text.strip().split(',')[0].split(' ')[2]))
                except:
                    competitions_year.append(-1)
                    competitions_dayofweek.append('-1')
                    competitions_month.append('-')
                    competitions_day.append(-1)
                
                try:
                    game_time = cells[4].text.strip()
                    if game_time == 'Unknown' or game_time == '':
                        game_time = 'n/a'
                    competitions_time.append(game_time)
                except:
                    competitions_time.append('n/a')
                
                home_teams_name.append(cells[5].find('a').get('title'))
                home_teams_id.append(cells[5].find('a').get('href').split('/')[-3])
                
                try:
                    home_teams_position.append(cells[6].find('span', class_ = 'tabellenplatz').text[1:][:-2])
                except:
                    home_teams_position.append(-1)

                away_teams_name.append(cells[7].find('a').get('title'))
                away_teams_id.append(cells[7].find('a').get('href').split('/')[-3])

                try:
                    away_teams_position.append(cells[8].find('span', class_ = 'tabellenplatz').text[1:][:-2])
                except:
                    away_teams_position.append(-1)
                
                try:
                    formation = cells[9].text
                    if formation == '?' or formation == "":
                        formation = 'n/a'
                    formations.append(formation)
                except:
                    formations.append('n/a')
                
                try:
                    coach = cells[10].text
                    if coach == '':
                        coach = 'n/a'
                    coaches.append(coach)
                except:
                    coaches.append('n/a')
                
                try:
                    attendance = cells[11].text.replace('.','')
                    if attendance == '' or attendance == 'x':
                        attendance = -1
                    attendances.append(attendance)
                except:
                    attendances.append(-1)
                    
                try:
                    games_results.append(cells[12].find('a').text)
                except:
                    games_results.append('n/a')
        
                try:
                    games_links.append(cells[12].find('a').get('href'))
                except:
                    games_links.append('n/a')
        
        time.sleep(2)

games_date_df = pd.DataFrame({  'COUNTRY_ID' : country_ids, 'HOME_TEAM_ID' : home_teams_id, 'HOME_TEAM' : home_teams_name,
                                'AWAY_TEAM_ID' : away_teams_id, 'AWAY_TEAM' : away_teams_name, 'GAME_YEAR' : competitions_year,
                                'GAME_DOW' : competitions_dayofweek, 'GAME_MONTH' : competitions_month, 'GAME_DOM' : competitions_day,
                                'GAME_TIME' : competitions_time, 'ATTENDANCE' : attendances, 'GAME_LINK' : games_links,
                                'COMPETITION' :  competitions   })

month_dictionary = {'Jan' : 1, 'Feb' : 2, 'Mar' : 3, 'Apr' : 4, 'May' : 5, 'Jun' : 6, 'Jul' : 7, 'Aug' : 8,
                    'Sep' : 9, 'Oct' : 10, 'Nov' : 11, 'Dec' : 12, '-' : -1}
dow_dictionary = {'Mon' : 1, 'Tue' : 2, 'Wed' : 3, 'Thu' : 4, 'Fri' : 5, 'Sat' : 6, 'Sun' : 7, '-1' : -1}
games_date_df['GAME_MONTH'] = games_date_df['GAME_MONTH'].map(month_dictionary)
games_date_df['GAME_DOW'] = games_date_df['GAME_DOW'].map(dow_dictionary)
games_date_df['GAME_TIME'] = games_date_df['GAME_TIME'].apply(lambda x: x if x == 'n/a' else
                                        (x[:-3] if x[-2:] == 'AM' else 
                                         str(int(x[:-3].split(':')[0]) % 12 + 12) + ":" + x[:-3].split(':')[1]))

games_date_df.insert(0, 'GAME_ID', games_date_df['GAME_LINK'].apply(lambda x: x.split('/')[-1]))
games_date_df.loc[((games_date_df['COMPETITION'].str[0:4] == 'Euro') | (games_date_df['COMPETITION'].str[0:4] == 'UEFA')), 'COUNTRY_ID'] = 'UEFA'
games_date_df.drop(columns = 'COMPETITION', inplace = True)
games_date_df.drop_duplicates(subset='GAME_ID', keep='first', inplace = True)
games_date_df = games_date_df.astype({'GAME_ID' : int, 'HOME_TEAM_ID' : int, 'AWAY_TEAM_ID' : int, 'GAME_YEAR' : int, 'GAME_DOM' : int,
                                      'ATTENDANCE' : int})



games_results_df = pd.DataFrame({   'COUNTRY_ID' : country_ids, 'HOME_TEAM_ID' : home_teams_id, 'HOME_TEAM' : home_teams_name,
                                    'AWAY_TEAM_ID' : away_teams_id, 'AWAY_TEAM' : away_teams_name, 'COMPETITION' :  competitions,
                                    'MATCHDAY' : competitions_round, 'HOME_TEAM_PLACE' : home_teams_position,
                                    'AWAY_TEAM_PLACE' : away_teams_position, 'FORMATION' : formations, 'COACH' : coaches, 
                                    'RESULT' : games_results, 'GAME_LINK' : games_links, 'SOURCE_TEAM_ID' : source_team_id  })

games_results_df.insert(0, 'GAME_ID', games_results_df['GAME_LINK'].apply(lambda x: x.split('/')[-1]))
games_results_df.loc[((games_results_df['COMPETITION'].str[0:4] == 'Euro') | (games_results_df['COMPETITION'].str[0:4] == 'UEFA')), 'COUNTRY_ID'] = 'UEFA'

games_results_df['LEAGUE_FLG'] = games_results_df['MATCHDAY'].apply(lambda x : 1 if str(x).isdigit() else 0 )
games_results_df['>90_FLG'] = games_results_df['RESULT'].apply(lambda x: 1 if x[-7:] == 'on pens' or x[-3:] == 'AET' else 0)

games_results_df['HOME_GOALS'] = games_results_df['RESULT'].apply(lambda x : x.split(':')[0]).apply(lambda x : -1 if x == '-' else x)
games_results_df['AWAY_GOALS'] = games_results_df['RESULT'].apply(lambda x : x.split(':')[1].split(' ')[0]).apply(lambda x : -1 if x == '-' else x)

games_results_df.loc[games_results_df['LEAGUE_FLG'] == 1,'COMPETITION'] = 'NL'
games_results_df.loc[games_results_df['COMPETITION'].str[:7] == 'UEFA Ch','COMPETITION'] = 'ECh'
games_results_df.loc[games_results_df['COMPETITION'].str[:4] == 'Euro','COMPETITION'] = 'ELg'
games_results_df.loc[games_results_df['COMPETITION'].str[:7] == 'UEFA Eu','COMPETITION'] = 'ECo'

games_results_df.drop(columns = ['GAME_LINK', 'LEAGUE_FLG'] ,inplace = True)
games_results_df = games_results_df.astype({'GAME_ID' : int, 'HOME_TEAM_ID' : int, 'AWAY_TEAM_ID' : int, 'HOME_TEAM_PLACE' : int,
                                            'AWAY_TEAM_PLACE' : int, 'HOME_GOALS' : int, 'AWAY_GOALS' : int})



team_performance_df = pd.DataFrame({    'GAME_ID' : [], 'COUNTRY_ID' : [], 'HOME_FLG' : [], 'TEAM_ID' : [], 'TEAM' : [], 'OPPONENT_ID'  : [],
                                        'OPPONENT' : [],'COMPETITION' : [], 'MATCHDAY' : [], 'TEAM_PLACE' : [],'OPPONENT_PLACE' : [], 
                                        'GOALS_SCORED' : [], 'GOALS_LOST' : [], 'FORMATION' : [], 'COACH' : []  })                                 

for team in games_results_df.loc[games_results_df['COMPETITION'] == 'NL','HOME_TEAM_ID'].unique():
    team_performance_df_home = (games_results_df.loc[(games_results_df['HOME_TEAM_ID'] == team) & (games_results_df['SOURCE_TEAM_ID'] == team)]
                                [['GAME_ID', 'COUNTRY_ID', 'HOME_TEAM_ID', 'HOME_TEAM', 'AWAY_TEAM_ID', 'AWAY_TEAM', 
                                  'COMPETITION', 'MATCHDAY', 'HOME_TEAM_PLACE','AWAY_TEAM_PLACE','HOME_GOALS', 
                                  'AWAY_GOALS', 'FORMATION', 'COACH']])
    team_performance_df_home.insert(2,'HOME_FLG',1)
    team_performance_df_home.columns = ['GAME_ID', 'COUNTRY_ID', 'HOME_FLG', 'TEAM_ID', 'TEAM', 'OPPONENT_ID', 'OPPONENT'
                                        ,'COMPETITION', 'MATCHDAY', 'TEAM_PLACE','OPPONENT_PLACE', 'GOALS_SCORED', 
                                        'GOALS_LOST', 'FORMATION', 'COACH']

    team_performance_df_away = (games_results_df.loc[(games_results_df['AWAY_TEAM_ID'] == team) & (games_results_df['SOURCE_TEAM_ID'] == team)]
                                [['GAME_ID', 'COUNTRY_ID', 'AWAY_TEAM_ID', 'AWAY_TEAM', 'HOME_TEAM_ID', 'HOME_TEAM', 
                                  'COMPETITION', 'MATCHDAY', 'AWAY_TEAM_PLACE','HOME_TEAM_PLACE', 'AWAY_GOALS', 
                                  'HOME_GOALS', 'FORMATION', 'COACH']])
    team_performance_df_away.insert(2,'HOME_FLG',0)
    team_performance_df_away.columns = ['GAME_ID', 'COUNTRY_ID', 'HOME_FLG', 'TEAM_ID', 'TEAM', 'OPPONENT_ID', 'OPPONENT'
                                        ,'COMPETITION', 'MATCHDAY', 'TEAM_PLACE','OPPONENT_PLACE', 'GOALS_SCORED', 
                                        'GOALS_LOST', 'FORMATION', 'COACH']

    team_performance_df = pd.concat(objs = [team_performance_df, team_performance_df_home, team_performance_df_away]).sort_index()

team_performance_df = team_performance_df.astype({'GOALS_SCORED': int, 'GOALS_LOST': int, 'HOME_FLG' : int, 'GAME_ID' : int, 'TEAM_ID' : int,
                                                  'OPPONENT_ID' : int, 'TEAM_PLACE' : int, 'OPPONENT_PLACE' : int})
team_performance_df['POINTS'] = (team_performance_df.apply(lambda row: 3 if row['COMPETITION'] == 'NL' and row['GOALS_SCORED'] > 
                                                            row['GOALS_LOST'] and row['GOALS_SCORED'] != -1 else (1 if row['COMPETITION'] 
                                                            == 'NL' and row['GOALS_SCORED'] == row['GOALS_LOST'] and row['GOALS_SCORED'] != -1 
                                                            else 0), axis = 1))

team_performance_df['POINTS_SUM'] = team_performance_df.groupby('TEAM_ID')['POINTS'].cumsum()
team_performance_df['LEAGUE_GOALS_SCORED'] = team_performance_df.apply(lambda row : row['GOALS_SCORED'] if (row['COMPETITION'] == 'NL' and row['GOALS_SCORED'] != -1) else 0, axis = 1)
team_performance_df['LEAGUE_GOALS_LOST'] = team_performance_df.apply(lambda row : row['GOALS_LOST'] if (row['COMPETITION'] == 'NL' and row['GOALS_LOST'] != -1) else 0, axis = 1)
team_performance_df['GOALS_SCORED_ALL'] = team_performance_df.groupby('TEAM_ID')['LEAGUE_GOALS_SCORED'].cumsum()
team_performance_df['GOALS_LOST_ALL'] = team_performance_df.groupby('TEAM_ID')['LEAGUE_GOALS_LOST'].cumsum()
team_performance_df['GOALS_DIFF'] = team_performance_df.apply(lambda row : row['GOALS_SCORED_ALL'] - row['GOALS_LOST_ALL'], axis = 1)
team_performance_df.drop(columns = ['LEAGUE_GOALS_SCORED', 'LEAGUE_GOALS_LOST'], inplace = True)

games_results_df.drop_duplicates(subset='GAME_ID', keep='first', inplace = True)
columns_to_apply = ['GAME_ID', 'COUNTRY_ID', 'HOME_TEAM_ID', 'HOME_TEAM', 'AWAY_TEAM_ID', 'AWAY_TEAM', 'COMPETITION', 'MATCHDAY', 'RESULT', '>90_FLG', 'HOME_GOALS', 'AWAY_GOALS']
games_results_df_20 = games_results_df[columns_to_apply]



games_date_df.to_csv(f'C:/folder/games_date_df_{letter}.csv', index = False, sep = '|', encoding = 'UTF-8')
games_results_df_20.to_csv(f'C:/folder/games_results_df_20_{letter}.csv', index = False, sep = '|', encoding = 'UTF-8')
team_performance_df.to_csv(f'C:/folder/team_performance_df_{letter}.csv', index = False, sep = '|', encoding = 'UTF-8')