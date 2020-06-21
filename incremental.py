from selenium import webdriver
from selenium.webdriver import ChromeOptions
import time
import pandas as pd
import os
import glob

options = ChromeOptions()
preferences = {"download.default_directory":os.getcwd()+'\Incremental_data'}
options.add_experimental_option('prefs',preferences)

driver = webdriver.Chrome(os.getcwd()+'\chromedriver.exe',options=options)

driver.get('https://www.nseindia.com/api/corporates-pit?index=equities&csv=true')
time.sleep(5)
driver.close()
file = glob.glob(os.getcwd()+'\Incremental_data\\'+'CF*.csv')

df = pd.read_csv(file[0])
os.remove(str(file[0]))
os.rmdir(os.getcwd()+'\Incremental_data')

df = df[['SYMBOL \n', 'COMPANY \n', 'NAME OF THE ACQUIRER/DISPOSER \n', 'CATEGORY OF PERSON \n',
       'TYPE OF SECURITY (PRIOR) \n', 'NO. OF SECURITY (PRIOR) \n',
       '% SHAREHOLDING (PRIOR) \n',
       'NO. OF SECURITIES (ACQUIRED/DISPLOSED) \n',
       'VALUE OF SECURITY (ACQUIRED/DISPLOSED) \n',
       'ACQUISITION/DISPOSAL TRANSACTION TYPE \n',
       'NO. OF SECURITY (POST) \n', '% POST \n',
       'MODE OF ACQUISITION \n',
       'BROADCASTE DATE AND TIME \n']]

df.columns=['SYMBOL','COMPANY','NAME OF THE ACQUIRER/DISPOSER','CATEGORY OF PERSON',
       'TYPE OF SECURITY (PRIOR)', 'NO. OF SECURITY (PRIOR)',
       '% SHAREHOLDING (PRIOR)',
       'NO. OF SECURITIES (ACQUIRED/DISPLOSED)',
       'VALUE OF SECURITY (ACQUIRED/DISPLOSED)',
       'ACQUISITION/DISPOSAL TRANSACTION TYPE',
       'NO. OF SECURITY (POST)', '% POST',
       'MODE OF ACQUISITION',
       'BROADCASTE DATE AND TIME']

import sqlite3
conn = sqlite3.connect('Insider Trading.db')
c= conn.cursor()


df1 = pd.read_sql_query("select * from Insider_Trading", conn)
df1 = df1.append(df, ignore_index=True)
df1.drop_duplicates(inplace=True)


df1.to_sql('Insider_Trading',conn,if_exists='replace',index=False)
