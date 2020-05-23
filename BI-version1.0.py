import pandas as pd
import sys
import gspread
import math
import json
from datetime import datetime
from sqlalchemy import create_engine
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, ForeignKeyConstraint, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
meta = MetaData()


def convertColumns(liveDataDf):
    liveDataDf.columns = liveDataDf.columns.str.replace(' ','_') 
    liveDataDf.columns = liveDataDf.columns.str.replace('-','_') 
    liveDataDf.columns = liveDataDf.columns.str.replace('"','')
    liveDataDf.columns = liveDataDf.columns.str.replace('7','seven')
    liveDataDf.columns = liveDataDf.columns.str.replace('1','one')
    liveDataDf.columns = liveDataDf.columns.str.replace('/','_or_')
    liveDataDf.columns = map(str.lower, liveDataDf.columns)

def convert_history_columns(histDataDf): 
    histDataDf.columns = histDataDf.columns.str.replace(' ','_') 
    histDataDf.columns = histDataDf.columns.str.replace('-','_') 
    histDataDf.columns = histDataDf.columns.str.replace('"','')
    histDataDf.columns = histDataDf.columns.str.replace('7','seven')
    histDataDf.columns = histDataDf.columns.str.replace('1','one')
    histDataDf.columns = histDataDf.columns.str.replace('/','_or_')
    histDataDf.columns = map(str.lower, histDataDf.columns)
    histDataDf=histDataDf.rename(columns={'company': 'account'})
    print('columns converted')
    return histDataDf
# connecting to yugabyte DB can be changed to postgres later on.
def connectToPostgres():
    engine = create_engine('postgresql://yugabyte@localhost:5433/bi_automation')
    conn = engine.connect()
    print('Connected to yugabyte instance')
    return conn,engine

# connecting to spreadsheets and load all required sheets. Named then LiveData and KeyAcc for now.
def connectToSheets():
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('Secret.json', scope)
    sheetClient = gspread.authorize(creds)
    liveDataSheet = sheetClient.open("LiveData").sheet1
    keyAccSheet = sheetClient.open("KeyAcc").sheet1
    historysheet = sheetClient.open("history_data").sheet1
    print('read of spreadsheets successful')
    return liveDataSheet, keyAccSheet,historysheet
#histDataSheet = sheetClient.open("HistoryData").sheet1

def getDataFrames(liveDataSheet, keyAccSheet,historysheet):
    # creating a dictionary of all the data
    accountDf = pd.DataFrame(data = None, columns=['accountid','account','keyaccount'])
    keyAccDict = keyAccSheet.get_all_records()
    liveDataDict = liveDataSheet.get_all_records()
    histDataDict = historysheet.get_all_records()
    #histDataDict = histDataSheet.get_all_records()
 
    # converting dict to dataframe
    liveDataDf = pd.DataFrame(liveDataDict)
    keyAccDf = pd.DataFrame(keyAccDict)
    historyDataDf = pd.DataFrame(histDataDict)
    
    return liveDataDf, keyAccDf, accountDf,historyDataDf

def getTables():
    accountData = Table(
       'account_data', meta,
       Column('accountid', Integer,primary_key = True, autoincrement = False), 
       Column('account', String), 
       Column('keyaccount', Integer), 
    )

    liveData = Table(
       'live_data', meta,
        Column('accountid', Integer,ForeignKey('account_data.accountid')),
        Column('campaign_or_priority', String),
        Column('industry_vertical', String),
        Column('category', String),
        Column('headquarters', String),
        Column('seven_day_heat_index', Integer),
        Column('seven_day_docs_views', Integer),
        Column('seven_day_quickstart_views', Integer),
        Column('seven_day_crdb_views', Integer),
        Column('seven_day_page_views', Integer),
        Column('all_time_heat_index', Integer),
        Column('all_time_docs_views', Integer),
        Column('all_time_quickstart_views', Integer),
        Column('all_time_crdb_views', Integer),
        Column('all_time_page_views', Integer),
        Column('outreach_sequences', Integer),
        Column('community_rewards', Integer),
        Column('seven_day_installs', Integer),
        Column('one_day_installs', Integer),
        Column('yb_slack_users', Integer),
        Column('yb_github_issues', Integer),
        Column('yb_github_stars', Integer),
        Column('yb_forum_issues', Integer),
        Column('crdb_slack_users', Integer),
        Column('crdb_github_issues', Integer),
        Column('crdb_github_stars', Integer),
        Column('crdb_forum_issues', Integer),
        Column('it_or_eng_headcount', Integer),
        Column('sql', Integer),
        Column('nosql', Integer),
        Column('aws', Integer),
        Column('google_cloud', Integer),
        Column('azure', Integer),
        Column('kubernetes', Integer),
        Column('docker', Integer),
        Column('cassandra', Integer),
        Column('mongodb', Integer),
        Column('oracle', Integer),
        Column('postgresql', Integer),
        Column('google_spanner', Integer),
        Column('rds', Integer),
        Column('aurora', Integer),
        Column('dynamodb', Integer),
        Column('cockroachdb', Integer),
        Column('import_id', Integer),
    )
    return accountData, liveData

def createTables(engine):
# create the necessary tables if they do not exist
    print('Creating necessary tables')    
    accountData, liveData = getTables()
    # History data should be created similarly
    meta.create_all(engine)
    print('Tables created successfully')
    return accountData, liveData

def insertIntoAccountTable(dataframe, engine):
    Acc_list=list(set(dataframe['account']))
    accountDf  = pd.read_sql_query('select * from account_data',con=engine)
    accIndex = accountDf['accountid'].max() + 1
    if math.isnan(accIndex) :
        accIndex = 1
    newEntryDf = pd.DataFrame(data = None, columns=['accountid','account','keyaccount'])
    for accName in Acc_list:
        if accName not in (list(accountDf['account'])):
            accountDf = accountDf.append({'accountid':accIndex,'account':accName, 'keyaccount':0},ignore_index=True)
            newEntryDf = newEntryDf.append({'accountid':accIndex,'account':accName, 'keyaccount':0},ignore_index=True)
            accIndex = accIndex + 1
    newEntryDf.to_sql('account_data', con = engine, if_exists='append', index = False)
    print('new accounts added')
    return accountDf

def keyAccountFlagging(keyAccDf,accountDf,accountData, conn):
    x=set(keyAccDf['keyaccount'])
    y=set(accountDf['account'])
    inter =list(set(x).intersection(y))
    conn.execute(accountData.update().values(keyaccount = 0))
    for acc in inter:  
        key=accountDf[accountDf['account'] == acc].index  
        x=int(accountDf.loc[key[0],'accountid'])
        update_sql=accountData.update().where(accountData.c.accountid == x).values(keyaccount = 1)
        conn.execute(update_sql)
        
    print('Updated the required keyAccounts to 1')
    return accountDf

def dropRowsFromLiveData(liveDataDf, accountDf) :
    for x in liveDataDf.columns[5:]:
        liveDataDf = liveDataDf.astype(str)
        liveDataDf[x] = liveDataDf[x].str.replace(',', '')
    change=(liveDataDf.columns[5:])
    liveDataDf[change]=liveDataDf[change].apply(pd.to_numeric)
    liveDataDf = pd.merge(liveDataDf, accountDf, on ='account', how='outer')
    liveDataDf = liveDataDf.drop(['account','keyaccount'], axis=1)
    print('Drop account and keyaccount from live data')
    return liveDataDf

def id_generator(conn):
    def check(new_id):
        if str(new_id[4:6]) == str(datetime.now().month).zfill(2):
            new_id=int(new_id)+1
            return new_id
        else:
            import_id=newimport_id()
            return import_id      
            
    newimport_id = lambda : str(datetime.now().year)+(str(datetime.now().month).zfill(2))+'01'
    # select from db
    getid = 'select max(import_id) from live_data ;'
    import_id = conn.execute(getid)
    for row in import_id:
        if row[0]:
            import_id=check(str(int(row[0])))
            return import_id
        else:
            import_id=newimport_id()
    return import_id


def insert_live_data(liveDataDf, engine, conn):
    import_id = int(id_generator(conn))  
    liveDataDf['import_id']=import_id
    liveDataDf.to_sql('live_data', con=engine,if_exists='append',index=False)
    print('live data added to the table successfully')

def dropColumnsFromHisoryData(histDataDf,accountDf):
    histDataDf = pd.merge(histDataDf, accountDf, on ='account', how='outer')
    histDataDf = histDataDf.drop(['keyaccount','account',''], axis=1)
    print('Drop account and keyaccount from history data')
    return histDataDf

def insert_history_data(histDataDf,engine):     
    histDataDf.to_sql('history_data', con=engine,if_exists='replace',index=False)
    print('History data live data added to the table successfully')
    return histDataDf

def closeDbConnection(conn):
    conn.close()

def clearMetaData(meta):
    meta.clear()

def reArrangeLiveDataCols(liveDataDf):
    cols = liveDataDf.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    liveDataDf = liveDataDf[cols]

def delete_live_data(delete_id):
    conn,engine = connectToPostgres()
    accountData, liveData = getTables()
    del_stmt = liveData.delete().where(liveData.c.import_id == delete_id)
    conn.execute(del_stmt)
    closeDbConnection(conn)

def getMaxImportId():
    conn,engine = connectToPostgres()
    getid = 'select max(import_id) from live_data;'
    import_id = conn.execute(getid)
    for row in import_id:
        print (row[0])
        importId = {"maxImportId" : row[0]}
        return json.dumps(importId)
        closeDbConnection(conn)
    importId = {"maxImportId": 0}
    return json.dumps(importId)
    closeDbConnection(conn)

def main():
    try:
        conn,engine = connectToPostgres()
        liveDataSheet, keyAccSheet,historysheet = connectToSheets()
        liveDataDf, keyAccDf, accountDf,historyDataDf = getDataFrames(liveDataSheet, keyAccSheet,historysheet)
        convertColumns(liveDataDf)
        accountData, liveData = createTables(engine)
        historyDataDf=convert_history_columns(historyDataDf)
        accountDf = insertIntoAccountTable(historyDataDf,engine)
        accountDf=accountDf = insertIntoAccountTable(liveDataDf, engine)
        accountDf = keyAccountFlagging(keyAccDf,accountDf,accountData, conn)
        liveDataDf = dropRowsFromLiveData(liveDataDf, accountDf)
        cols = liveDataDf.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        liveDataDf = liveDataDf[cols]
        insert_live_data(liveDataDf, engine, conn)
        historyDataDf = dropColumnsFromHisoryData(historyDataDf, accountDf)
        historyDataDf = insert_history_data(historyDataDf,engine)
        clearMetaData(meta)
        closeDbConnection(conn)
    except:
        print(sys.exc_info())    

main()
#delete_live_data(20200510)
#getMaxImportId()
