# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 13:21:59 2020

@author: juan.jimenez01
"""


import pandas as pd
import psycopg2 as psc
import pandas.io.sql as sql
import json
import datetime as dt
import sklearn as skt


data = pd.read_csv(r'C:/Users/juan.jimenez01/Desktop/Ds4-Project/Data/Chromedrive/Bases/Base_Scraping_Empleo.csv',encoding = 'utf-8')


conn = psc.connect(user = 'Juanchito',
                   password = 'Juanchito',
                   host = '',  
                   port = 5432,
                   database = 'postgres')

cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    
    if cur:
        cur.close()
        



        
conn = psc.connect(user = 'Juanchito',
                   password = 'Juanchito',
                   host = 'databasebi.cyqul1bogmtp.us-east-2.rds.amazonaws.com',  
                   port = 5432,
                   database = 'postgres')
        
cur = conn.cursor()
        
        
cur.execute('''CREATE TABLE JOBS(
    ID serial PRIMARY KEY,
    CARGO varchar(300), 
    EMPRESA varchar(100),
    AREA varchar(100),
    SALARIO varchar(50),
    UBICACION varchar(50),
    PROFESION varchar(100),
    SECTOR varchar(100),
    EDUCACION varchar(200),
    DESCRIPCION varchar(2000)
    );''')

conn.commit()

if cur:
    cur.close()

data_query = pd.read_sql('SELECT * FROM JOBS', conn)

#--------------------- push a la informacion

print(data_query.columns)


query = """ 
INSERT INTO JOBS 
(
CARGO,
EMPRESA,
AREA,
SALARIO,
UBICACION,
PROFESION,
SECTOR,
EDUCACION,
DESCRIPCION
)
VALUES

"""
print(data.columns)
data = data[[
    'Cargo',
     'Empresa',
     'Area',
     'Salario',
     'Ubicacion',
     'Profesion',
     'Sector',
     'Educacion',
     'Descripcion'
    
    ]]


dic = {
       "á":"a",
       "à":"a",
       "é":"e",
       "è":"e",
       "í":"i",
       "ì":"i",
       "ó":"o",
       "ò":"o",
       "ú":"u",
       "ù":"u",
       "ñ":"n"
       }


for columna in data.columns:
    for key in dic.keys(): 
        data[columna] = data[columna].str.replace(key,dic[key]) 




print(data.columns)

values = list(zip(
    data['Cargo'].tolist(),
    data['Empresa'].tolist(),
    data['Area'].tolist(),
    data['Salario'].tolist(),
    data['Ubicacion'].tolist(),
    data['Profesion'].tolist(),
    data['Sector'].tolist(),
    data['Educacion'].tolist(),
    data['Descripcion'].tolist()
    ))




values = json.dumps(values)
values = values[1:-1].replace('[', '(').replace(']', ')').replace('"',"'")

set 

