import snowflake.connector

sfconn = snowflake.connector.connect(
              user = "karthik",
            password = "Karthiksara@2123",
            account  = 'vpb37715.us-east-1',
            )
sfconn = sfconn
sf = sfconn.cursor()
use_warehouse_command = f"USE WAREHOUSE sample_wh;"
sf.execute(use_warehouse_command)

database_create = f"USE DATABASE TSeries"
sf.execute(database_create)
        

database_create = "USE SCHEMA PUBLIC"
sf.execute(database_create)
use_warehouse_command = f'SELECT max("publishedAt") FROM "Tseries_table";'
sf.execute(use_warehouse_command)
results = sf.fetchall()
a_week = results[0][0]
from script4 import FullLoadDataToS3
from Script2 import RawToProcess
from Script3 import SnowFlakeDataIngest
import datetime

if __name__ == "__main__":

        #Youtube API Key 
    api_key = "AIzaSyCgxCRumhmu7F6Z2NDXA4vShDSkZ7ba6lY"

    """
    This is T-series channel id
    How to get Channel_id of any other Youtube channel ?
    open the url of the youtube channel
    Press ctrl+U
    Then Press ctrl + F then search keyword channel_id
    """
    channel_id = 'UCq-Fj5jknLsUf-MWSy4_brA'

    #Creating an instance of our class FullLoadDataToS3 
    FullLoadDataToS3_obj = FullLoadDataToS3(api_key,channel_id)
    Bucket_name = "my-bucket-2521"
    now = datetime.datetime.now()
    input_format="%Y-%m-%dT%H:%M:%SZ"
    output_format="%Y-%m-%d %H:%M:%S.%f"
    date_object = datetime.datetime.strptime(a_week, input_format)

    # Format the datetime object to the desired format
    a_week = date_object.strftime(output_format)
    a_week = datetime.datetime.strptime(a_week, output_format)
    
    # a_week = datetime.datetime.strptime(a_week, output_format)
    

    raw_file_name = FullLoadDataToS3_obj.run(End_date = a_week , Bucket_name = Bucket_name)
    raw_bucket_name = "my-bucket-2521"
    clean_bucket_name = "cleanbucket2521"
    # raw_file_name = "put_data_from2023-9-29-7_to_2023-10-6-7"

    RawToProcess = RawToProcess(raw_bucket_name , clean_bucket_name)
    RawToProcess.run(raw_file_name = raw_file_name)

    Clean_Bucket_name ="cleanbucket2521"
    start_date = a_week
    end_date = now

    SnowFlakeDataIngestObj = SnowFlakeDataIngest() 
    SnowFlakeDataIngestObj.run(Clean_Bucket_name , start_date , end_date)