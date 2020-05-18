import pandas as pd
import gspread
import math
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
    print('read of spreadsheets successful')
    return liveDataSheet, keyAccSheet
#histDataSheet = sheetClient.open("HistoryData").sheet1

def getDataFrames():
    # creating a dictionary of all the data
    accountDf = pd.DataFrame(data = None, columns=['accountid','account','keyaccount'])
    keyAccDict = keyAccSheet.get_all_records()
    liveDataDict = liveDataSheet.get_all_records()
    #histDataDict = histDataSheet.get_all_records()
 
    # converting dict to dataframe
    liveDataDf = pd.DataFrame(liveDataDict)
    keyAccDf = pd.DataFrame(keyAccDict)
    
    return liveDataDf, keyAccDf, accountDf

def createTables(engine):
    # create the necessary tables if they do not exist
    print('Creating necessary tables')
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
    # History data should be created similarly
    meta.create_all(engine)
    print('Tables created successfully')
    return accountData, liveData


def insertIntoAccountTable(accountDf, keyAccDf, liveDataDf, accountData) :
    accountDf = new_df = pd.read_sql_query('select * from account_data',con=engine)
    accIndex = accountDf['accountid'].max() + 1
    if math.isnan(accIndex) :
        accIndex = 1
    
    # adding a new dataframe so that we add just the new entries on the live data table in one single
    # sql query this is needed in more in successive loads.
    
    newEntryDf = pd.DataFrame(data = None, columns=['accountid','account','keyaccount'])
    for index, row in liveDataDf.iterrows():
        accName = row['Account']
        if accName in (list(accountDf['account'])):
            pass
        else:
            accountDf = accountDf.append({'accountid':accIndex,'account':accName, 'keyaccount':0},ignore_index=True)
            newEntryDf = newEntryDf.append({'accountid':accIndex,'account':accName, 'keyaccount':0},ignore_index=True)
            accIndex = accIndex + 1

    newEntryDf.to_sql('account_data', con = engine, if_exists='append', index = False)
    print('Done inserting data')

    # get the common account names from both the accountsTable and keyAccounts Table to set the key acc to 1
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
    liveDataDf.columns = map(str.lower, liveDataDf.columns)
    liveDataDf = pd.merge(liveDataDf, accountDf, on ='account', how='outer')
    liveDataDf = liveDataDf.drop(['account','keyaccount'], axis=1)
    print('Drop account and keyaccount from live data')
    return liveDataDf

def id_generator():
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

def insert_live_data(liveDataDf):
    for x in liveDataDf.columns[5:]:
        liveDataDf = liveDataDf.astype(str)
        liveDataDf[x] = liveDataDf[x].str.replace(',', '')
    change=(liveDataDf.columns[5:])
    liveDataDf[change]=liveDataDf[change].apply(pd.to_numeric)         
    liveDataDf['import_id']=import_id
    liveDataDf.to_sql('live_data', con=engine,if_exists='append',index=False)
    print('live data add to the table successfully')
    return liveDataDf
        
    

conn,engine = connectToPostgres()
liveDataSheet, keyAccSheet = connectToSheets()
liveDataDf, keyAccDf, accountDf = getDataFrames()
convertColumns(liveDataDf)
accountData, liveData = createTables(engine)
accountDf = insertIntoAccountTable(accountDf, keyAccDf, liveDataDf, accountData)
liveDataDf = dropRowsFromLiveData(liveDataDf, accountDf)
import_id = int(id_generator())
cols = liveDataDf.columns.tolist()
cols = cols[-1:] + cols[:-1]
liveDataDf = liveDataDf[cols]
liveDataDf = insert_live_data(liveDataDf)


'''
def delete_live_data(delete_id):
    del_stmt = accountData.delete().where(import_id.c.id = delete_id)
    conn.execute(del_stmt)

#get the importid to be deleted as delete_id and pass it to the function
delete_live_data(delete_id)

'''