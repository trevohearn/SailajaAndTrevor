import config
import mysql.connector
from mysql.connector import errorcode
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt


#database string
endpoint = config.DatabaseEndpoint
name = config.DatabaseName
password = config.DatabasePassword
port = config.DatabasePort
cnx = mysql.connector.connect(
    host = endpoint,
    user = name,
    passwd = password,
)
cur = cnx.cursor()
#create tables
TABLES = {}
TABLES['MarketShareByGenre'] ="""
CREATE TABLE MarketShareByGenre (
  Ranking INT,
  Genre VARCHAR(255) PRIMARY KEY,
  Movies INT,
  TotalBoxOffice BIGINT,
  Tickets BIGINT,
  Shares DECIMAL(10, 2)
)
"""

TABLES['TVShows'] ="""
CREATE TABLE TVShows (
  TVSeries VARCHAR(255) PRIMARY KEY,
  Network VARCHAR(255),
  YearsOnAir INT,
  CostPerEpisode INT,
  Genre VARCHAR(255)
)
"""

TABLES['GenreMarketByYear'] ="""
CREATE TABLE GenreMarketByYear (
  Year INT,
  MoviesInRelease INT,
  MarketShare DECIMAL(10, 2),
  Gross INT,
  TicketsSold INT,
  InflationAdjustedGross INT,
  TopGrossingMovie VARCHAR(255),
  GrossThatYear INT,
  Genre VARCHAR(255),
  Id VARCHAR(255) PRIMARY KEY
)
"""

TVShows = ['TVSeries', 'Network', 'YearsOnAir', 'CostPerEpisode', 'Genre']

MarketShareByGenreColumns = ['Ranking', 'Genre', 'Movies',
                             'TotalBoxOffice', 'Tickets', 'Shares']

GenreMarketByYearColumns = ['Year', 'MoviesInRelease', 'MarketShare',
            'Gross', 'TicketsSold', 'InflationAdjustedGross',
                     'TopGrossingMovie', 'GrossThatYear', 'Genre','Id']


add_GenreMarketByYear = ("INSERT INTO Movies.GenreMarketByYear"
                "(Year, MoviesInRelease, MarketShare, Gross, TicketsSold, "
                "InflationAjustedGross, TopGrossingMovie, GrossThatYear, Genre, Id)"
                "VALUES (%s, %s, %s, %s ,%s, %s, %s, %s, %s, %s)")

add_MarketShareByGenreColumns = ("INSTER INTO Movies.MarketShareByGenre"
                                 "(Ranking, Genre, Movies, TotalBoxOffice, Tickets, Shares)"
                                "VALUES (%s. %s, %s, %s, %s, %s)"

)

cnx.database = 'Movies'
#create tables
#create tables from dictionary
def create_tables(tables, cursor):
    for table_name in tables:
        table_description = tables[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print('already exists')
            else:
                print(err.msg)
        else:
            print('OK')

create_tables(TABLES, cur)
cnx.commit()

#get data
html_page = requests.get('https://www.the-numbers.com/market/genres')
soup = BeautifulSoup(html_page.content,'html.parser')
table=soup.find('table')
df=pd.read_html(str(table))[0]

#get all data of different genres
for genre in df['Genre']:
    genre_url='https://www.the-numbers.com/market/genre/'+genre.replace('/','-or-').replace(' ','-')
    res=requests.get(genre_url)
    soup=BeautifulSoup(res.text,'html.parser')
    tables=soup.find_all('table')
    df2=pd.read_html(str(tables[1]))[0]
    df2['Genre']=genre
    all_df.append(df2)
for df in all_df:
        df['Id'] = df['Year'].apply(str).add(df['Genre'])


#clean
for df in all_df:
    df['MarketShare'] = df['MarketShare'].replace({'%': ''}, regex=True)
    df['Gross'] = df['Gross'].replace({'\$' : ''}, regex=True)
    df['Inflation-AdjustedGross'] = df['Inflation-AdjustedGross'].replace({'\$' : ''}, regex=True)
    df['Gross that Year'] = df['Gross that Year'].replace({'\$' : ''}, regex=True)
    df['Gross'] = df['Gross'].replace({',':''}, regex = True)
    df['Inflation-AdjustedGross'] = df['Inflation-AdjustedGross'].replace({',':''}, regex = True)
    df['Gross that Year'] = df['Gross that Year'].replace({',':''}, regex = True)
    df['Year'] = df['Year'].astype(int)
    df['Movies inRelease'] = df['Movies inRelease'].astype(int)
    df['Gross'] = df['Gross'].astype(int)
    df['Tickets Sold'] = df['Tickets Sold'].astype(int)
    df['Inflation-AdjustedGross'] = df['Inflation-AdjustedGross'].astype('int32')
    df['Gross that Year'] = df['Gross that Year'].astype(int)

insert_GenreMarketByYear = (
                "INSERT INTO Movies.GenreMarketByYear"
                "(Year, MoviesInRelease, MarketShare, Gross, TicketsSold, "
                "InflationAdjustedGross, TopGrossingMovie, GrossThatYear, Genre, Id)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )
#finish clean and insert row

for df in all_df:
    for idx, row in df.iterrows():
        #clean row
        year = df.iloc[idx][0] #int
        mir = df.iloc[idx][1] #int
        ms = df.iloc[idx][2] #decimal
        gross = df.iloc[idx][3] #int
        tS = df.iloc[idx][4] #int
        iAG = df.iloc[idx][5] #int
        tGM = df.iloc[idx][6] #string
        gTY = df.iloc[idx][7] #int
        G = df.iloc[idx][8] #string
        Id = df.iloc[idx][9] #string
        year = np.int64(year).item()
        mir = np.int64(mir).item()
        gross = np.int64(gross).item()
        tS = np.int64(tS).item()
        iAG = np.int64(iAG).item()
        gTY = np.int64(gTY).item()

        #push row
        data = [year, mir, ms, gross, tS, iAG, tGM, gTY, G, Id]
        cur.execute(insert_GenreMarketByYear, data)
        cnx.commit()



#pull data
cur.execute("""
SELECT *
FROM Movies.GenreMarketByYear
;
""")
df8 = pd.DataFrame(cur.fetchall())
df8.columns = [x[0] for x in cur.description]

#data testing and visualization
df9 = df8.drop(['Year', 'InflationAdjustedGross', 'GrossThatYear'], axis=1)

df9_by_genre = df9.groupby('Genre').std().sort_values('Gross', ascending=False)
df9_by_genre
df10_by_year = df8.groupby('Genre')['GrossThatYear'].std()

g = sns.barplot(x='Genre', y='GrossThatYear', data=df8)
g.set_xticklabels(g.get_xticklabels(),rotation=70)
plt.title('Total Money Made')

t = sns.barplot(x='Genre', y='MoviesInRelease', data=df8)
t.set_xticklabels(t.get_xticklabels(),rotation=70)
plt.title('Total Movies Released')

y = sns.barplot(x='Year', y='TicketsSold', data=df8)
y.set_xticklabels(y.get_xticklabels(),rotation=70)
plt.title('Total Tickets Sold')

### tvshows.py for more data ###
