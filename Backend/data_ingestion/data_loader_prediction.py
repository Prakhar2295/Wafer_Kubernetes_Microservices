import pandas as pd
from application_logging.logger import App_Logger
from application_logging.mongodb_logger import mongodb_logger





class data_getter_prediction:
    
    """
         This class shall be used to load the data from the source 
         
         Written by: JSL
         Version: 1.0
         Revision: None
    
    
    """
    def __init__(self):
        self.logger_object = mongodb_logger()
        #self.file_object = "prediction_logs/data_getter_predictions.txt"
        self.prediction_file = "PredictionFileFromDB/InputFile.csv"
        
    def data_loader(self):
        """
           
           Method Name: data_loader
           Description: This method will be used to load the from source into the pandas dataframe.
           
           Output: A dataframe
           On failure: Exceptions will be raised
        
        
        """
        #file = open(self.file_object,'a+')
        self.logger_object.insert_records_into_collection("wafer","data_loader","Entered inside the data_loader method inside the data_getter_prediction")
        #file.close()
        try:
            df = pd.read_csv(self.prediction_file)
            #file = open(self.file_object,'a+')
            self.logger_object.insert_records_into_collection("wafer","data_loader","Loaded the prediction dataframe succesfully")
            #file.close()
            return df
        except OSError:
            #file = open(self.file_object,'a+')
            self.logger_object.insert_records_into_collection("wafer","data_loader","Error loading the prediction dataframe.Exception message:: %s"%OSError)
            raise OSError
        except Exception as e:
             #file = open(self.file_object,'a+')
             self.logger_object.insert_records_into_collection("wafer","data_loader","Exception occurred loading the prediction dataframe.Exception message:: %s"%e)
             #file.close()
             raise e
            
            
                
    
            
            
               
