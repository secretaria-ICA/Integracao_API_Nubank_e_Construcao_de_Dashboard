
<h1> API Nubank <h1>


```python
################################
#### IMPORTANDO BIBLIOTECAS ####
################################

import json
import pandas as pd
import numpy as np
import time
import os
import sys
import argparse
import psutil
import psycopg2
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from datetime import date, timedelta
from pynubank import Nubank
from sqlalchemy import create_engine, exc
from pywinauto.application import Application
from pywinauto import timings
from io import StringIO
```


```python
########################
#### FUNÇÕES STRING ####
########################

def find_str(s, char):
    index = 0

    if char in s:
        c = char[0]
        for ch in s:
            if ch == c:
                if s[index:index+len(char)] == char:
                    return index

            index += 1

    return -1
```


```python
############################
#### FUNÇÕES API NUBANK ####
############################

# Login Nubank
def login_nubank():
    nu = Nubank()
    uuid, qr_code = nu.get_qr_code()

    # Nesse momento será printado o QRCode no console
    # Você precisa escanear pelo o seu app do celular

    # Esse menu fica em NU > Perfil > Acesso pelo site
    qr_code.print_ascii(invert=True)
    input('Após escanear o QRCode pressione enter para continuar')

    # Somente após escanear o QRCode você pode chamar a linha abaixo
    nu.authenticate_with_qr_code(***, ***, uuid)
    
    return nu

# pegando dados Nubank
def nu_crawler():
    # saldo NuConta
    saldo = nu.get_account_balance()

    # Lista de dicionários contendo todas as faturas do seu cartão de crédito
    bills = nu.get_bills()
    
    # Lista de dicionários contendo todas as transações de seu cartão de crédito
    cc_transactions = nu.get_card_statements()

    # lista de dicionários contendo todas as transações de débito
    order = nu.get_account_statements()
    
    return saldo, bills, cc_transactions, order
```


```python
############################
#### FUNÇÕES TRAT. JSON ####
############################

# Tratamento JSON bills
def json_bills(bills):
    # transforma lista em json
    json_bills = json.dumps(bills)

    # transforma json em dataframe
    df_bills = pd.read_json(StringIO(json_bills))
    
    # pegando o json summary
    list_summary = df_bills.summary.values.tolist()
    
    # transforma lista em json
    json_summary = json.dumps(list_summary)

    # transforma json em dataframe
    df_summary = pd.read_json(StringIO(json_summary))
    
    # corrigindo index
    df_summary = df_summary.sort_values(by=['close_date']).reset_index().drop(['index'], axis=1)
    
    return df_summary

# Tratamento JSON transaction
def json_transactions(transactions):
    # transforma lista em json
    json_transactions = json.dumps(transactions)

    # transforma json em dataframe
    df_transactions = pd.read_json(StringIO(json_transactions)).drop(['_links', 'href', 'details'], axis=1)
    
    # tratando hora de criação GMT-3
    df_transactions['time'] = pd.to_datetime(df_transactions['time']) - timedelta(hours=3)
    
    # arrumando a ordem das colunas
    df_transactions = df_transactions[['id', 'time', 'account', 'amount', 'amount_without_iof', 'category', 'description',
                                       'source', 'title', 'tokenized']]
    
    # removendo aspas
    df_transactions['description'] = df_transactions['description'].str.replace("'", "")
    
    return df_transactions

def json_statements(statements):
    # transforma lista em json
    json_statements = json.dumps(statements)

    # transforma json em dataframe
    df_statements = pd.read_json(StringIO(json_statements))

    # arrumando a ordem das colunas
    df_statements = df_statements[['id', 'postDate', '__typename', 'amount', 'destinationAccount', 'detail', 
                                   'originAccount', 'title']]

    # tratando campos destinationAccount & originAccount
    df_destination = df_statements[df_statements['destinationAccount'].notnull()]
    df_destination = df_destination.drop(columns = ['postDate', '__typename', 'amount', 'detail', 'originAccount', 
                                                    'title'])
    df_destination = df_destination.sort_values(by=['id']).reset_index().drop(['index'], axis=1)
    df_destination = df_destination.join(pd.read_json(json.dumps(df_destination.destinationAccount.values.tolist())))
    df_destination = df_destination.drop(columns = ['destinationAccount'])

    df_origin = df_statements[df_statements['originAccount'].notnull()]
    df_origin = df_origin.drop(columns = ['postDate', '__typename', 'amount', 'detail', 'destinationAccount', 'title'])
    df_origin = df_origin.sort_values(by=['id']).reset_index().drop(['index'], axis=1)
    df_origin = df_origin.join(pd.read_json(json.dumps(df_origin.originAccount.values.tolist())))
    df_origin = df_origin.drop(columns = ['originAccount'])
    df_origin.columns = ['id', '__originAccount']

    # cruzando com a master
    df_statements = df_statements.set_index('id').join(df_destination.set_index('id'))
    df_statements = df_statements.join(df_origin.set_index('id'))
    df_statements = df_statements.drop(columns = ['originAccount', 'destinationAccount'])
    df_statements['id'] = df_statements.index
    df_statements = df_statements[['id', 'postDate', '__typename', 'amount', 'name', 'detail', '__originAccount', 'title']]
    df_statements.columns = ['id', 'postDate', '__typename', 'amount', '__destinationAccount', 'detail', 
                             '__originAccount', 'title']
    
    # removendo aspas
    df_statements['__originAccount'] = df_statements['__originAccount'].str.replace("'", "")
    df_statements['__destinationAccount'] = df_statements['__destinationAccount'].str.replace("'", "")
    df_statements['__destinationAccount'] = df_statements['__destinationAccount'].str.replace("'", "")
    
    return df_statements

def json_to_df(bills, cc_transactions, order):
    # insere em dataframe todas faturas do cartão de crédito
    df_bills = json_bills(bills)

    # insere em dataframe todas faturas do cartão de crédito
    df_cc_transactions = json_transactions(cc_transactions)

    # insere em dataframe todas transações de débito
    df_order = json_statements(order)
    
    return df_bills, df_cc_transactions, df_order
```


```python
#####################
#### FUNÇÕES SQL ####
#####################

# SQL connection
def sql_conn():
    # Definindo Variáveis de Conexão
    hostname = 'localhost'
    username = 'postgres'
    password = 'postgres'
    database = 'db_nubank'
    port = '5432'

    # Criando a Engine
    engine = create_engine('postgresql+psycopg2://' + str(username) + ':' + str(password) 
                           + '@' + str(hostname) + ':' + str(port) + '/' + str(database))
    
    return hostname, username, password, database, port, engine

# delete table
def exec_query(query):
    # Conexão SQL
    hostname, username, password, database, port, engine = sql_conn()
    myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = myConnection.cursor()
    
    # deletando tabela existente
    cur.execute(query)
    
    # Encerrando conexão com banco de dados
    myConnection.commit()
    myConnection.close()
    return

# executar query em df
def df_query(query):
    # Conexão SQL
    hostname, username, password, database, port, engine = sql_conn()
    myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = myConnection.cursor()
    
    # deletando tabela existente
    df = pd.read_sql(query, engine)
    
    # Encerrando conexão com banco de dados
    myConnection.commit()
    myConnection.close()
    
    return df
    
# update tb_order
def update_tb_cc_trans(df_transactions):
    # Conexão SQL
    hostname, username, password, database, port, engine = sql_conn()
    myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = myConnection.cursor()
    
    # deletando tabela existente
    exec_query('DELETE FROM tb_cc_transaction')
    
    # tratando nan
    df_transactions['amount_without_iof'] = df_transactions['amount_without_iof'].replace(np.nan, 0)
    df_transactions['tokenized'] = df_transactions['tokenized'].replace(np.nan, 0)

    # iniciando contador
    rows = 0
    qtd_linhas = df_transactions.shape[0]

    while rows < qtd_linhas:
        str_insert = ("SELECT '" + str(df_transactions.iloc[rows, 0]) + "', '" + str(df_transactions.iloc[rows, 1]) 
                      + "', '" + str(df_transactions.iloc[rows, 2]) + "', '" + str(df_transactions.iloc[rows, 3]) 
                      + "', '" + str(df_transactions.iloc[rows, 4]) + "', '" + str(df_transactions.iloc[rows, 5]) 
                      + "', '" + str(df_transactions.iloc[rows, 6]) + "', '" + str(df_transactions.iloc[rows, 7]) 
                      + "', '" + str(df_transactions.iloc[rows, 8]) + "', '" + str(df_transactions.iloc[rows, 9]) + "'")

        # Executando query para adicionar PRIMARY KEY
        cur.execute("INSERT INTO tb_cc_transaction " + str_insert)

        rows = rows + 1

    # Encerrando conexão com banco de dados
    myConnection.commit()
    myConnection.close()
    return

def update_tb_bills(df_bills):
    # Conexão SQL
    hostname, username, password, database, port, engine = sql_conn()
    myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = myConnection.cursor()
    
    # deletando tabela existente
    exec_query('DELETE FROM tb_bills')
    
    # tratando nan
    df_bills['late_fee'] = df_bills['late_fee'].replace(np.nan, 0)
    df_bills['late_interest_rate'] = df_bills['late_interest_rate'].replace(np.nan, 0)
    df_bills['remaining_balance'] = df_bills['remaining_balance'].replace(np.nan, 0)
    df_bills['remaining_minimum_payment'] = df_bills['remaining_minimum_payment'].replace(np.nan, 0)
    df_bills['minimum_payment'] = df_bills['minimum_payment'].replace(np.nan, 0)

    # iniciando contador
    rows = 0
    qtd_linhas = df_bills.shape[0]

    while rows < qtd_linhas:
        str_insert = ("SELECT '" + str(df_bills.iloc[rows, 1]) + "', '" # close_date
                  + str(df_bills.iloc[rows, 0]) + "', '" # due_date
                  + str(df_bills.iloc[rows, 3]) + "', '"  # effective_due_date
                  + str(df_bills.iloc[rows, 6]) + "', '" # interest
                  + str(df_bills.iloc[rows, 5]) + "', '"  # interest_rate
                  + str(df_bills.iloc[rows, 12]) + "', '" # late_fee
                  + str(df_bills.iloc[rows, 11]) + "', '" # late_interest_rate
                  + str(df_bills.iloc[rows, 9]) + "', '" # minimum_payment
                  + str(df_bills.iloc[rows, 10]) + "', '" # open_date
                  + str(df_bills.iloc[rows, 8]) + "', '" # paid
                  + str(df_bills.iloc[rows, 2]) + "', '" # past_balance
                  + str(df_bills.iloc[rows, 13]) + "', '" # remaining_balance
                  + str(df_bills.iloc[rows, 14]) + "', '" # remaining_minimum_payment
                  + str(df_bills.iloc[rows, 4]) + "', '" # total_balance
                  + str(df_bills.iloc[rows, 7]) + "'") # total_cumulative

        # Executando query para adicionar PRIMARY KEY
        cur.execute("INSERT INTO tb_bills " + str_insert)

        rows = rows + 1

    # Encerrando conexão com banco de dados
    myConnection.commit()
    myConnection.close()
    return

def update_tb_order(df_statements, saldo):
    # Conexão SQL
    hostname, username, password, database, port, engine = sql_conn()
    myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
    cur = myConnection.cursor()
    
    # deletando tabela existente
    exec_query('DELETE FROM tb_order')
    
    # inserindo df_nan
    df_nan = df_statements[df_statements['amount'].isnull()]
    df_nan = df_nan.drop(columns = ['postDate', '__typename', 'amount', '__destinationAccount', '__originAccount', 
                                    'title'])
    # iniciando contador
    rows = 0
    qtd_linhas = df_nan.shape[0]

    # tratando coluna de string --> float
    while rows < qtd_linhas:
        # função substirng index
        index = find_str(str(df_nan.iloc[rows, 1]), 'R$')
        
        # atualizando campo e tratando
        df_nan.iloc[rows, 1] = str(df_nan.iloc[rows, 1])[index+3:]

        rows = rows + 1
    
    df_nan['detail'] = df_nan['detail'].str.replace('.', '')
    df_nan['detail'] = df_nan['detail'].str.replace(',', '.').astype(float)
    df_nan.columns = ['id', '__detail']
    
    df_statements = df_statements.join(df_nan.set_index('id'))
    df_statements['vl_total'] = df_statements.loc[:,['amount','__detail']].sum(axis=1)
    df_statements = df_statements.drop(columns = ['amount', '__detail'])
    df_statements = df_statements[['id', 'postDate', '__typename', 'vl_total', '__destinationAccount', 'detail', 
                                   '__originAccount', 'title']]
    df_statements.columns = ['id', 'postDate', '__typename', 'amount', '__destinationAccount', 'detail', 
                             '__originAccount', 'title']
    
    # tratando nan
    df_statements['amount'] = df_statements['amount'].replace(np.nan, 0)
    df_statements['__destinationAccount'] = df_statements['__destinationAccount'].replace(np.nan, '')
    df_statements['__originAccount'] = df_statements['__originAccount'].replace(np.nan, '')
    
    # inserindo dataframe na tb_order
    # iniciando contador
    rows = 0
    qtd_linhas = df_statements.shape[0]

    while rows < qtd_linhas:
        str_insert = ("SELECT '" + str(df_statements.iloc[rows, 0]) + "', '" + str(df_statements.iloc[rows, 1]) + "', '"
                      + str(df_statements.iloc[rows, 2]) + "', '" + str(df_statements.iloc[rows, 3]) + "', '"
                      + str(df_statements.iloc[rows, 4]) + "', '" + str(df_statements.iloc[rows, 5]) + "', '"
                      + str(df_statements.iloc[rows, 6]) + "', '" + str(df_statements.iloc[rows, 7]) + "'")

        # Executando query para adicionar PRIMARY KEY
        cur.execute("INSERT INTO tb_order " + str_insert)

        rows = rows + 1
        
    # data de hoje
    agora = datetime.today().strftime('%Y-%m-%d')
    
    # inserindo saldo na tabela de saldo
    try:
        exec_query("INSERT INTO tb_saldo SELECT '" + agora + "'," + "'" + str(saldo) + "'")
    except:
        pass
    
    # Encerrando conexão com banco de dados
    myConnection.commit()
    myConnection.close()
    return

def update_postgresql(df_bills, df_cc_transactions, df_order, saldo):    
    # update df_transaction --> tb_cc_transaction (Python --> SQL)
    update_tb_cc_trans(df_cc_transactions)

    # update df_bills --> tb_bills (Python --> SQL)
    update_tb_bills(df_bills)

    # update df_order & saldo --> tb_order & saldo (Python --> SQL)
    update_tb_order(df_order, saldo)
    
    return
```


```python
################
#### MASTER ####
################

# login Nubank
nu = login_nubank()
```

    █████████████████████████████████████
    █████████████████████████████████████
    ████ ▄▄▄▄▄ ██▄▄█  ▀ █▀▀▄▀█ ▄▄▄▄▄ ████
    ████ █   █ █ ▀ █▀▄▀ ▄▀▀█ █ █   █ ████
    ████ █▄▄▄█ █ ▀▄▄ ██▀ ▄▄▄▀█ █▄▄▄█ ████
    ████▄▄▄▄▄▄▄█ ▀▄▀▄█ ▀▄█▄▀▄█▄▄▄▄▄▄▄████
    ████▄█▄ ▄▄▄▀▀▀ ▄▄▀▀▄▀ ▄▀▄█ ▄ ▄▄▀▀████
    ████ █▄▄ ▀▄▀▀ ▄█▀ ▄▄▀█▄▀█▀█ ▀▀▄██████
    ████ ▄▄▄▀▄▄█   ▄▄█▄▄▀▄▀▀▀ ▀▄▀█▄▀▀████
    ████ ▄█   ▄█ █▄▀ █▀ ▄▄▀█ █▄▀ ▄█▀▀████
    ████ ▀▄███▄▀ ▀▀ ▄▀▀▄▀ ▄ █▄▀▀▀▄ ▀▀████
    ████ █▀▀▄▀▄▄▀▀ █▀ ▄ ██ █▄▀█▄ ██▀▀████
    ████▄█▄███▄▄▀ ████▄▄▄▄▀  ▄▄▄ ▄▄▀▀████
    ████ ▄▄▄▄▄ █▀▀██ █▀ ▄█▀▄ █▄█ ██▀█████
    ████ █   █ █ ▄ █ █▀▄▀▄▄▀  ▄▄▄ ▄▀▀████
    ████ █▄▄▄█ █▄▀ ██ ▀ ▄▄▄ ▀▀▄  █▄ █████
    ████▄▄▄▄▄▄▄█▄█▄███▄▄█▄███▄█▄██▄██████
    █████████████████████████████████████
    ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
    

    Após escanear o QRCode pressione enter para continuar 
    


```python
#order
#display(df_order)
```


```python
################
#### MASTER ####
################

# pega dados na Nuconta
saldo, bills, cc_transactions, order = nu_crawler()

# insere todos os dados em dataframe
df_bills, df_cc_transactions, df_order = json_to_df(bills, cc_transactions, order)

# update sql
update_postgresql(df_bills, df_cc_transactions, df_order, saldo)
```

<h1> SQL <h1>


```python
###############################
#### CREATE TABLE POSTGRES ####
###############################

#%%sql
#
#create table tb_cc_transaction(
#	order_id varchar(50) primary key,
#	created_at timestamp,
#	wallet_id varchar(50),
#	amount numeric,
#	amount_without_iof numeric,
#	category varchar(50),
#	description varchar(100),
#	source varchar(50),
#	title varchar(50),
#	tokenized numeric
#);

#create table tb_order(
#	id varchar(50) primary key,
#	created_at date,
#	type_name varchar(50),
#	amount numeric,
#	destination_acc varchar(200),
#	detail varchar(100),
#	origin_acc varchar(200),
#	title varchar(50)
#);

#create table tb_order_aux(
#	id varchar(50) primary key,
#	detail varchar(100),
#	amount numeric
#);
#

#create table tb_bills(
#	close_date date primary key,
#	due_date date,
#	effective_due_date date,
#	interest numeric,
#	interest_rate numeric,
#	late_fee numeric,
#	late_interest_rate numeric,
#	minimum_payment numeric,
#	open_date date,
#	paid numeric,
#	past_balance numeric,
#	remaining_balance numeric,
#	remaining_minimum_payment numeric,
#	total_balance numeric,
#	total_cumulative numeric
#);

#create table tb_saldo(
#	created_at date primary key,
#	amount numeric
#);
```
