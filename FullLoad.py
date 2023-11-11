import datetime

def full_load_fun() -> str:
    try:
        #Importing modules
        from Load import FullLoadDataToS3
        from Preprocess import RawToProcess
        from Transform import SnowFlakeDataIngest
        
        print("Full Load function started .... /n  Here we take data of past 7 days")
        
        """ Parameter required for the classes """
    
        #Youtube API Key 
        Api_Key = "AIzaSyCgxCRumhmu7F6Z2NDXA4vShDSkZ7ba6lY"
        """
        This is T-series channel id
        How to get Channel_id of any other Youtube channel ?
        open the url of the youtube channel
        Press ctrl+U
        Then Press ctrl + F then search keyword channel_id
        """

        Channel_Id = 'UCq-Fj5jknLsUf-MWSy4_brA'        
        
        #Create a date that is 7 days before now
        Today_Date = datetime.datetime.now()
        Seven_Day_Delta_Time = datetime.timedelta(days=7)
        # Subtract Seven day from the current datetime
        Week_Past_Date = Today_Date - Seven_Day_Delta_Time
    
    
        #Bucket_name of aws S3 that we have created manually
        Raw_Data_Loading_Bucket_Name = "my-bucket-2521"
        Clean_Bucket_Name = "cleanbucket2521"
        
        Extracting_Date_Range = f"Extracting Data from {Seven_Day_Delta_Time} to {Today_Date}"
        print(Extracting_Date_Range)
        
        
        """Calling Classes and its Methods"""
        #Creating an instance of our class FullLoadDataToS3 
        FullLoadDataToS3_obj = FullLoadDataToS3(Api_Key,Channel_Id)
        
        #Call the `FullLoadDataToS3_obj.run()` function with the date 7 days ago as the argument and bucket_name 
        Raw_File_Name = FullLoadDataToS3_obj.run(End_Date = Week_Past_Date , Bucket_Name = Raw_Data_Loading_Bucket_Name)
        
        if Raw_File_Name[1]!=0:
            
            #Call the class RawToProcess
            RawToProcess = RawToProcess(Raw_Data_Loading_Bucket_Name , Clean_Bucket_Name)
            RawToProcess.run(raw_file_name = Raw_File_Name[0])
            
            start_date = Week_Past_Date
            end_date = Today_Date
            
            #Call the Class snowflakedataingest
            SnowFlakeDataIngestObj = SnowFlakeDataIngest() 
            SnowFlakeDataIngestObj.run(Clean_Bucket_Name , start_date , end_date)
            
            return "Data uploaded into Snowflake Sucessfully"
        
        else:
            print("Data execution stoped due no videos")
    
    except Exception as e:
        
        return "Error in Fullload :- ",str(e)