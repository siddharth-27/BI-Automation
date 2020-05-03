from peewee import *
from playhouse.postgres_ext import PostgresqlExtDatabase
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import schedule,datetime,time


# DB URL and table model
db = PostgresqlExtDatabase('bi_automation',user='yugabyte',
password='',host='localhost',port='5433')
class Dataload(Model):
    Account = TextField()
    Industry_Vertical = TextField()
    Category = TextField()
    Headquarters = TextField()
    Day_Heat_Index = IntegerField()
    Day_Docs_Views = IntegerField()
    class Meta:
        database = db
        db_table = 'dataload'
Dataload.create_table()

# dataload function
def load_data():
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('Secret.json', scope)
    client = gspread.authorize(creds)

    # Name of the sheet that we want to open
    sheet = client.open("BI test").sheet1

    # read all the rows and create a list of dictonary.
    list_of_hashes = sheet.get_all_records()
    print('sheet read successfull')
    #print(list_of_hashes)

# Data loading 
    with db.atomic():
        query = Dataload.insert_many(list_of_hashes)
        query.execute()
        print('load successfull')

#load_data()
# Scheduling part
schedule.every(5).seconds.do(load_data) 

# may be the actual code to run every tue
# schedule.every().tuesday.at("18:00").do(load_data)    

while True: 
    # Checks whether a scheduled task  
    # is pending to run or not 
    schedule.run_pending() 
    time.sleep(1) 
