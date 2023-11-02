#Importing Modules
import pandas as pd
import boto3
import json
import datetime
import os


class SnowFlakeDataIngest:
    def __init__(self,) -> None:
    
      
        s3_client = boto3.client('s3')
        self.s3_client = s3_client
        self.username = "karthiksnowflake"
        self.ETL_INSERT_BY = self.username
        self.t_series_dataframe= pd.DataFrame(columns=['Title',"LikeCount","CommentCount","ViewCount","publishedAt","ETL_INSERT_DATE","ETL_INSERT_BY"])
    
    def run(self,Clean_Bucket_name, start_date , end_date):
        
        print("Starting Script 3 ......")
        date_range = pd.date_range(start_date, end_date, freq='H')
        file_count = 0 
        for itr_date in  date_range:
        
            itr_start_year = itr_date.year
            itr_date_month = itr_date.month
            itr_date_day = itr_date.day
            itr_date_hour = itr_date.hour
      
            
            file_name = f"{str(itr_start_year)}/{str(itr_date_month)}/{str(itr_date_day)}/{str(itr_date_hour)}/clean_data_from_{str(itr_start_year)}-{str(itr_date_month)}-{str(itr_date_day)}-{str(itr_date_hour)}"   
            
            try:
           
                res = self.s3_client.get_object(Bucket = Clean_Bucket_name,Key = file_name)
                data = res['Body'].read()
                t_series = self.clean_data(data=data)
                json_string = json.dumps(t_series.to_dict(orient='records'))
                # Upload the buffer to S3.
                # Create a buffer to store the JSON data.
                json_buffer = bytes(json_string, 'utf-8')
                
                self.s3_client.put_object(Bucket="snowpipebucket2521", Key="new_file", Body=json_buffer)
                
            


            
            except Exception as e:
                file_count = file_count + 1
                continue
        print("Data loading into the snowflake bucket successfully")
        print(f"error found in {file_count}")
        metadata = {"last_update_date":str(end_date)}
        # metadata = {"last_update_date":"2023-10-28 04:21:43.829869"}
        metadata_json = json.dumps(metadata)
        self.s3_client.put_object(Bucket="snowpipebucket2521", Key="Bookmark", Body=metadata_json)
      
        print("Ending Script 3")



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
