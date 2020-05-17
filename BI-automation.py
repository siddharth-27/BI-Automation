import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from sqlalchemy import create_engine

# Reading from google sheets

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('Secret.json', scope)
client = gspread.authorize(creds)
# Name of the sheet that we want to open
sheet = client.open("load1").sheet1
# read all the rows and create a list of dictonary.
data_dict = sheet.get_all_records()
sheet_new=client.open("key").sheet1
key_dict = sheet_new.get_all_records()
#dataframe for live_data and keyaccounts
df = pd.DataFrame(data_dict)
keyacc_df= pd.DataFrame(key_dict)

print('read successful')

# DB Connection
engine = create_engine('postgresql://yugabyte@localhost:5433/bi_automation')
conn = engine.connect()

# Dataframe for account table with data is loaded from Database
new_df = pd.read_sql_query('select * from account_data',con=engine)

# Clearing the column names
df.columns = df.columns.str.replace(' ','_') 
df.columns = df.columns.str.replace('-','_') 
df.columns = df.columns.str.replace('"','')
df.columns = df.columns.str.replace('7','seven')
df.columns = df.columns.str.replace('1','one')
df.columns = df.columns.str.replace('/','_or_')

i=1
# If the account table already has some values we set "i" to (max value +1).
if not(new_df.empty):
       i=new_df['accountid'].max() +1
# Looping through the live data and adding a new account entry to account table and DF / else Pass
for index, row in df.iterrows():
    a = row['Account']
    if a in (list(new_df['account'])):
        pass
    else:
        new_df=new_df.append({'account':a,'accountid':i},ignore_index=True)
        extra_df = pd.DataFrame(data=None, columns=new_df.columns)
        extra_df = extra_df.append({'account':a,'accountid':i},ignore_index=True)
        extra_df.to_sql('account_data', con=engine,if_exists='append',index=False)
        i+=1

# Flaging key accounts
x=set(keyacc_df['keyaccount'])
y=set(new_df['account'])
inter =list(set(x).intersection(y))
for acc in inter:  
    key=new_df[new_df['account'] == acc].index  
    update_sql = 'update account_data set keyaccount = 1 where accountid = '"'" +str(key[0])+ "'"
    conn.execute(update_sql)

    
dif= list(set(y).difference(set(inter)))
for acc in dif:
    key=new_df[new_df['account'] == acc].index  
    update_sql = 'update account_data set keyaccount = 0 where accountid = '"'" +str(key[0])+ "'"
    conn.execute(update_sql)

new_df = pd.read_sql_query('select * from account_data',con=engine)


# Merging the live and account dataframe and droping the required columns
df.columns = map(str.lower, df.columns)
df=pd.merge(df,new_df ,on ='account',how='outer')
df=df.drop(['account','keyaccount'], axis=1)


# Appending the live dataframe to the DB table
df.to_sql('live_data', con=engine,if_exists='append',index=False)