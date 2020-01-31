#imports
import pandas as pd
import requests
from bs4 import BeautifulSoup
from fuzzywuzzy import process
import seaborn as sns


#url to get data
url = 'https://qz.com/1735700/apple-and-disney-are-creating-an-explosion-of-tv-series-budgets/'

#request and push to soup
res = requests.get(url)
soup = BeautifulSoup(res.text,'html.parser')
#find table
table = soup.find(name='table',attrs={'class':'c188c'})

df_table = pd.read_html(str(table))[0]

#add genre column with fuzzywuzzy
choices = ['Action','Adventure', 'Drama', 'Comedy', 'Thriller/Suspense',
       'Horror', 'Romantic Comedy', 'Musical', 'Documentary',
       'Black Comedy', 'Western', 'Concert/Performance',
       'Multiple Genres', 'Reality']
df_table['Genre']=''
#https://www.rottentomatoes.com/tv/the_morning_show/
drama_url = 'https://www.rottentomatoes.com/tv/'
j=0
for i in df_table['TV series']:
    formated_i = i.lower().replace(' ','_')
    full_url = drama_url+formated_i
    #print(full_url)
    res = requests.get(full_url)

    if res.ok:
        series_soup = BeautifulSoup(res.text,'html.parser')
        rows = series_soup.find_all(name='td',attrs={'class':'fgm'})
        table_right=rows[0].findParent().findParent() #td->tr->tbody
        table_right_data = table_right.find_all(name='td')
        genre = table_right_data[5].text

    else:
        genre = 'Other'

    final_genre = process.extractOne(genre, choices)[0]
    df_table.at[j,'Genre']=final_genre
    j+=1

#clean data

df_table['Estimated cost per episode'] = df_table['Estimated cost per episode'].replace({'million': ''}, regex=True)
df_table['Estimated cost per episode'] = df_table['Estimated cost per episode'].replace({'\$': ''}, regex=True)
df_table['Estimated cost per episode'] = df_table['Estimated cost per episode'].replace({'\$': ''}, regex=True)
df_table['Estimated cost per episode'][1] = 16
df_table['Estimated cost per episode'][13] = 5
df_table['Estimated cost per episode'][16] = 4
df_table['Estimated cost per episode'][19] = 3
df_table['Estimated cost per episode'][23] = 2
numbers = [0,1,1,1,8,4,4,10,2,3,4,4,21,5,6,6,10,5,7,8,5,9,9,5]
df_table['Years on air'] = numbers
i = 0
for s in df_table['Estimated cost per episode']:
    df_table['Estimated cost per episode'][i] = int(s)
    i += 1

df_table.drop(0, inplace=True)

#visualizations
sns.barplot(x='Genre', y='Estimated cost per episode',
           hue='Genre', data=df_table)
