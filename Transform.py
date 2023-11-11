#Importing Modules
import snowflake.connector
import pandas as pd
import boto3
import json
import datetime
from snowflake.connector.pandas_tools import write_pandas
from tqdm import tqdm
import os


class SnowFlakeDataIngest:
    def __init__(self,) -> None:
        #Creating a connection
        sfconn = snowflake.connector.connect(
              user = "karthik",
            password = "Karthiksara@2123",
            account  = 'vpb37715.us-east-1',
            )
        self.sfconn = sfconn
        self.sf = sfconn.cursor()
        #Creating an s3 client
        s3_client = boto3.client('s3')
        self.s3_client = s3_client
        self.username = "karthiksnowflake"
        self.ETL_INSERT_BY = self.username
        self.t_series_dataframe= pd.DataFrame(columns=['Title',"LikeCount","CommentCount","ViewCount","publishedAt","ETL_INSERT_DATE","ETL_INSERT_BY"])
    
    def run(self,Clean_Bucket_name, start_date , end_date):
        
        print("Starting Script 3")
        date_range = pd.date_range(start_date, end_date, freq='H')
        for itr_date in  tqdm(date_range):
        
            itr_start_year = itr_date.year
            itr_date_month = itr_date.month
            itr_date_day = itr_date.day
            itr_date_hour = itr_date.hour
      
                            
            file_name = f"{str(itr_start_year)}/{str(itr_date_month)}/{str(itr_date_day)}/{str(itr_date_hour)}/clean_data_from_{str(itr_start_year)}-{str(itr_date_month)}-{str(itr_date_day)}-{str(itr_date_hour)}"   
            
            try:
           
                res = self.s3_client.get_object(Bucket = Clean_Bucket_name,Key = file_name)
                data = res['Body'].read()
                t_series = self.clean_data(data=data)
                
            
            except Exception as e:
            
                continue
        self.snowflaketableingestion("sample_wh","TSeries","Tseries_table")
        print("Ending Script 3")


    def snowflaketableingestion(self,Warehouse_name , Database_name , Table_Name):
        try:
            use_warehouse_command = f"USE WAREHOUSE {Warehouse_name};"
            self.sf.execute(use_warehouse_command)

            database_create = f"USE DATABASE {Database_name}"
            self.sf.execute(database_create)
        

            database_create = "USE SCHEMA PUBLIC"
            self.sf.execute(database_create)
            write_pandas(self.sfconn , self.t_series_dataframe , table_name=Table_Name,auto_create_table=True) 
        except Exception as e:
            print("Error in Snowflakeingestion:- ", str(e))

    def clean_data(self , data):
      
        data = json.loads(data)
        ViewCount = data['viewCount']
        LikeCount = data['likeCount']
        CommentCount = data['commentCount']
        publishdate = data['publishedAt']
        Title = str(data['Title']) 
        ETL_INSERT_DATE = datetime.datetime.now()
        ETL_INSERT_BY = self.ETL_INSERT_BY

        new_row = {'Title': Title, 'LikeCount':LikeCount , 'CommentCount' :CommentCount,"ViewCount" :ViewCount,"publishedAt":publishdate,"ETL_INSERT_DATE":str(ETL_INSERT_DATE), "ETL_INSERT_BY":str(ETL_INSERT_BY)}
        
        self.t_series_dataframe.loc[len(self.t_series_dataframe)] = new_row
        
        return self.t_series_dataframe

if __name__ == "__main__":
    Clean_Bucket_name ="cleanbucket2521"
    start_date = datetime.datetime(2023, 10, 1 , 11)
    end_date = datetime.datetime(2023,10,8,11)

    SnowFlakeDataIngestObj = SnowFlakeDataIngest() 
    SnowFlakeDataIngestObj.run(Clean_Bucket_name , start_date , end_date)