import boto3
import json
def Increamental_Load():
    s3_client = boto3.client('s3')
    # self.s3_client = s3_client
    response = s3_client.get_object(Bucket="snowpipebucket2521", Key="Bookmark")
    # Deserialize the JSON object to a dictionary
    metadata = json.loads(response['Body'].read().decode('utf-8'))
    a_week = metadata['last_update_date']
    print(a_week)
    from Load import FullLoadDataToS3
    from Preprocess import RawToProcess
    from Transform import SnowFlakeDataIngest
    import datetime
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
    # input_format="%Y-%m-%dT%H:%M:%SZ"
    # output_format="%Y-%m-%d %H:%M:%S.%f"
    # date_object = datetime.datetime.strptime(a_week, input_format)
    date_object = a_week
    a_week = datetime.datetime.strptime(date_object, '%Y-%m-%d %H:%M:%S.%f')
    
    # Format the datetime object to the desired format
    # a_week = date_object.strftime(output_format)
    # a_week = datetime.datetime.strptime(a_week, output_format)
    
    # a_week = datetime.datetime.strptime(a_week, output_format)
    

    raw_file_name = FullLoadDataToS3_obj.run(End_Date = a_week , Bucket_Name = Bucket_name)
    print(raw_file_name[1])
    if raw_file_name[1]!=0:
        raw_bucket_name = "my-bucket-2521"
        clean_bucket_name = "cleanbucket2521"
        # raw_file_name = "put_data_from2023-9-29-7_to_2023-10-6-7"
        
        RawToProcess = RawToProcess(raw_bucket_name , clean_bucket_name)
        RawToProcess.run(raw_file_name = raw_file_name[0])
        
        Clean_Bucket_name ="cleanbucket2521"
        start_date = a_week
        end_date = now
        
        SnowFlakeDataIngestObj = SnowFlakeDataIngest() 
        SnowFlakeDataIngestObj.run(Clean_Bucket_name , start_date , end_date)
    else:
        print("Data execution stoped due no videos")
