#all imports
import requests
import pandas as pd
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
%matplotlib inline
import seaborn as sns
import mysql.connector
from mysql.connector import errorcode
import config

html_page = requests.get('https://www.the-numbers.com/market/genres')
soup = BeautifulSoup(html_page.content,'html.parser')
table=soup.find('table')
genres=pd.read_html(str(table))[0]

#clean
genres['Total Box Office'] = genres['Total Box Office'].replace({'\$': ''}, regex=True)
genres['Share'] = genres['Share'].replace({'%': ''}, regex=True)
genres['Total Box Office'] = genres['Total Box Office'].replace({',': ''}, regex=True)
genres['Total Box Office'] = genres['Total Box Office'].astype(int)

#make money per Movie
genres['Money Per Movie']= genres['Total Box Office']//genres['Movies']


#graphs
g=sns.barplot(x='Genre',y='Money Per Movie',data=genres)
g.set_xticklabels(g.get_xticklabels(),rotation=70)
plt.title('Cost Per Movie')

# convert dframe to tuple
tuple_genres=list(genres.itertuples(index=False,name=None))

#database

cnx = mysql.connector.connect(
    host = config.my_cred['host'],
    user = config.my_cred['user'],
    passwd = config.my_cred['passwd']
)
cnx.database = 'Movies'
cursor = cnx.cursor()

def create_database(cursor, database):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(database))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
#database exists check
try:
    cursor.execute("USE {}".format(db_name))

#if the previous line fails because there isn't a db by that name run this line

except mysql.connector.Error as err:
    print("Database {} does not exists.".format(db_name))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor, db_name)
        print("Database {} created successfully.".format(db_name))
        cnx.database = db_name
    else:
        print(err)
        exit(1)

#table set up
DB_NAME = 'movies'

TABLES = {}
TABLES['genre'] = (
    "CREATE TABLE genre ("
    "  R_id varchar(20) NOT NULL,"
    "  Genre varchar(190) NOT NULL,"
    "  Movies int(190) NOT NULL,"
    "  Total_Box_Office varchar(50) NOT NULL,"
    "  Tickets int(50) NOT NULL,"
    "  Share double(10,2) NOT NULL,"
    "  PRIMARY KEY (R_id)"
    ") ENGINE=InnoDB")


#table creation
def create_table(cursor, TABLES):
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

#insert into tables
def insertData(cursor, tuples):
    cursor.executemany(
      """INSERT INTO genre(R_id, Genre, Movies, Total_Box_Office, Tickets, Share)
      VALUES (%s, %s, %s, %s, %s, %s)""",
      tuples)
      cnx.commit()

#all genres in one database
list_gener = []

for genre in genres['Genre']:
    genre_url='https://www.the-numbers.com/market/genre/'+genre.replace('/','-or-').replace(' ','-')
    res=requests.get(genre_url)
    soup=BeautifulSoup(res.text,'html.parser')
    tables=soup.find_all('table')
    df2=pd.read_html(str(tables[1]))[0]
    df2['Genre']=genre
    list_gener.append(df2)
for df in list_gener:
        df['Id'] = df['Year'].apply(str).add(df['Genre'])

df_genre=pd.concat(list_gener)
df_genre

df_genre_sub = df_genre[['Year','Genre','MarketShare']]
df_genre_sub['MarketShare'] = df_genre_sub['MarketShare'].str.replace('%', '')
df_genre_sub['MarketShare'] = pd.to_numeric(df_genre_sub['MarketShare'], errors='coerce')
df_genre_sub['MarketShare']=df_genre_sub['MarketShare']/100

#visualizations
# Use the 'hue' argument to provide a factor variable
sns.lmplot(x='Year',y='MarketShare',data=df_genre_sub,height=9, aspect=10/7,
           fit_reg=True, hue='Genre', legend=True)
plt.title("Market Share by Year")
