from datetime import datetime
from prediction_raw_data_validation.prediction_raw_data_validation import prediction_data_validation
#from data_ingestion.data_loader_prediction import data_getter_prediction
from application_logging.logger import App_Logger
from application_logging.mongodb_logger import mongodb_logger
from prediction_data_transformation.Datatransformationprediction import DataTransformpredict
from DataTypeValidation_Insertion_Prediction.Datatypevalidationprediction2 import dboperation
import shutil

class pred_validation:
    def __init__(self,path):
        self.logger_object = mongodb_logger()
        self.data_transform = DataTransformpredict()
        self.raw_data_validation = prediction_data_validation(path)
        self.dboperation = dboperation()
        #self.file_object = open("prediction_logs/Prediction_logs.txt",'a+')
        
        
    def prediction_validation(self):
        
        try:
            
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Start of Validation on files for prediction!!")
            ###Extrating values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns=self.raw_data_validation.values_from_schema()
            ###Creatinf the regex pattern for name validation
            regex = self.raw_data_validation.regex_creation()
            ###creating the goo-bad data directory for raw prediction files
            ####validating the names of raw prediction files
            self.raw_data_validation.raw_file_name_validation(regex,LengthOfDateStampInFile)
            ##validating the colimn length of the raw prediction files
            self.raw_data_validation.validateColumnLength(NumberofColumns)
            ###Validating the null values in whole column
            self.raw_data_validation.validatemissingvaluesinwholecolumn()
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs", "Raw File Validation completed")
            
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs", "Starting Data Transformation!!")
            #replacing blanks in the csv file with "Null" values to insert in table
            self.data_transform.replaceMissingwithNull()
            
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs", "Data Transformation Completed")
            
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Creating Prediction database and Tables on the basis of given given schema!!")
            
            ##create database with the given name and if present open the connection.Create the table inside database with the given column from the schema.
            self.dboperation.createtabledb("prediction",column_names)
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Creation of database done.Cration of tabel complted successfully!!")
            
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Inserting of data into table started !!!!")
            ##insert csv files into the table
            self.dboperation.insertIntoTableGoodData("prediction")
            
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Inserting data into table completed !!!!")
            
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Deleteing the good data directory !!!")
            
            self.raw_data_validation.deletedirectoryforGooddata()
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Deleted the good data directory !!!")
            
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Moving the bad data to the Archive bad directory!!!")
            ###Moving the bad data to Archive baad folder
            self.raw_data_validation.moveBadDatatoArchivebad()
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Bad data files moved to the Archive bad folder!!! Bad data folder Deleted!!!")
            
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","File Validation Operation completed!!!")
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Extracting CSV files from the Database table")
            ###Export data from to CSV file
            
            self.dboperation.selectingDatafromtableintocsv("prediction")
            shutil.rmtree("Prediction_Archive_Bad_data")
            self.logger_object.insert_records_into_collection("wafer","Prediction_logs","Expoerting the data from table to csv file Completed succesfully!! File Saved!!")

        
        except Exception as e:
            raise e    
            
#path = "default_file"
#p = pred_validation(path)
#p.prediction_validation()
#print("done")
            
            
            
             
             
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
        
       