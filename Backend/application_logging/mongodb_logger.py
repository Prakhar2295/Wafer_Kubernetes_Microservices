import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import os

class mongodb_logger:
    
    """
    
                This class will be used to log the messages in the mongo db database
                and all the exceptions inside the project.
                
                 
    """
    def insert_records_into_collection(self,db_name,collection,message):
        try:
            host_url = os.environ.get("MONGO_DB_HOST")
            #host_url = "mongodb://localhost:27017"
            default_connection_url = host_url
            client = pymongo.MongoClient(default_connection_url)
            database = client[db_name]
            collection = database[collection]
            now = datetime.now()
            date = now.date()
            time = now.strftime('%H:%M:%S')
            record = {
                "log_updated_date": str(date),
                "log_updated_time": str(time),
                "message": message,
                "project": "wafer_sensor",
                "updated date and time": str(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                
                
            }
            return collection.insert_one(record)
        except Exception as e:
            raise e
                
            
        
                    
        
            
            
                
                
        
        
        
      
        
        
      
        
       
        
        
        



