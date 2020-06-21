from selenium import webdriver
from selenium.webdriver import ChromeOptions
import time
import pandas as pd
import os
import glob

options = ChromeOptions()
preferences = {"download.default_directory":os.getcwd()+'\Historical_data'}
options.add_experimental_option('prefs',preferences)

driver = webdriver.Chrome(os.getcwd()+'\chromedriver.exe',options=options)

driver.get('https://www1.nseindia.com/content/equities/EQUITY_L.csv')
time.sleep(5)

df = pd.read_csv(os.getcwd()+'\Historical_data\EQUITY_L.csv')

symbol = df['SYMBOL'].values
symbol = symbol[:1615]
company_name_temp = df['NAME OF COMPANY'].values
company_name_temp = company_name_temp[:1615]
os.remove(os.getcwd()+'\Historical_data\EQUITY_L.csv')
company_name=[]
for data in company_name_temp:
    name=[]
    temp=data.split()
    for dat in temp:
        name+=dat
        if dat==temp[-1]:
            break
        name+='%20'
    s1=''
    company_name.append(s1.join(name))


for i in range(len(company_name)):
    driver.get('https://www.nseindia.com/api/corporates-pit?index=equities&symbol='+symbol[i]+'&issuer='+company_name[i]+'&csv=true')
    time.sleep(1)
time.sleep(5)

driver.close()

files = glob.glob(os.getcwd()+'\Historical_data\\'+'CF*.csv')
df_list = []

for file in files:
    data = pd.read_csv(file)
    df_list.append(data)
    os.remove(str(file))

os.rmdir(os.getcwd()+'\Historical_data')

df = pd.concat(df_list)
df = df[['SYMBOL \n', 'COMPANY \n', 'NAME OF THE ACQUIRER/DISPOSER \n', 'CATEGORY OF PERSON \n',
       'TYPE OF SECURITY (PRIOR) \n', 'NO. OF SECURITY (PRIOR) \n',
       '% SHAREHOLDING (PRIOR) \n',
       'NO. OF SECURITIES (ACQUIRED/DISPLOSED) \n',
       'VALUE OF SECURITY (ACQUIRED/DISPLOSED) \n',
       'ACQUISITION/DISPOSAL TRANSACTION TYPE \n',
       'NO. OF SECURITY (POST) \n', '% POST \n',
       'MODE OF ACQUISITION \n',
       'BROADCASTE DATE AND TIME \n']]

df.columns=[['SYMBOL','COMPANY','NAME OF THE ACQUIRER/DISPOSER','CATEGORY OF PERSON',
       'TYPE OF SECURITY (PRIOR)', 'NO. OF SECURITY (PRIOR)',
       '% SHAREHOLDING (PRIOR)',
       'NO. OF SECURITIES (ACQUIRED/DISPLOSED)',
       'VALUE OF SECURITY (ACQUIRED/DISPLOSED)',
       'ACQUISITION/DISPOSAL TRANSACTION TYPE',
       'NO. OF SECURITY (POST)', '% POST',
       'MODE OF ACQUISITION',
       'BROADCASTE DATE AND TIME']]

import sqlite3
conn = sqlite3.connect('Insider Trading.db')
c= conn.cursor()


c.execute('''CREATE TABLE Insider_Trading(
'SYMBOL' VARCHAR(20),
'COMPANY' VARCHAR(50),
'NAME OF THE ACQUIRER/DISPOSER' VARCHAR(100),
'CATEGORY OF PERSON' VARCHAR(50),
'NO. OF SECURITY (PRIOR)' INT,
'% SHAREHOLDING (PRIOR)' FLOAT,
'NO. OF SECURITIES (ACQUIRED/DISPLOSED)' INT,
'VALUE OF SECURITY (ACQUIRED/DISPLOSED)' FLOAT,
'ACQUISITION/DISPOSAL TRANSACTION TYPE' VARCHAR(40),
'NO. OF SECURITY (POST)' INT,
'% POST' FLOAT,
'MODE OF ACQUISITION' VARCHAR(40),
'BROADCASTE DATE AND TIME' DATETIME)''')

conn.commit()

df.to_sql('Insider_Trading',conn,if_exists='replace',index=False)
