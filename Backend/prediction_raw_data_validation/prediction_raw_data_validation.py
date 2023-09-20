import os
from datetime import datetime
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger
from application_logging.mongodb_logger import mongodb_logger



class prediction_data_validation:
    """
               This class shall be used for handling all the validation done on the Raw Prediction Data!!.

               Written By: JSL
               Version: 1.0
               Revisions: None

               """

    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger = mongodb_logger()


    def values_from_schema(self):
        """
                                Method Name: values_from_schema
                                Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                                Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                                On Failure: Raise ValueError,KeyError,Exception

                                Written By: JSL
                                Version: 1.0
                                Revisions: None

                                        """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            #file = open("prediction_logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.insert_records_into_collection("wafer","values_from_schema",message)

            #file.close()



        except ValueError:
            #file = open("prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","values_from_schema","ValueError:Value not found inside schema_training.json")
            #file.close()
            raise ValueError

        except KeyError:
            #file = open("prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","values_from_schema", "KeyError:Key value error incorrect key passed")
            #file.close()
            raise KeyError

        except Exception as e:
            #file = open("prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","values_from_schema", str(e))
            #file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns
    
    def regex_creation(self):
        """
              Method: regex_creation
              Description: This method will create a new regex which will be used 
              to validate the prediction file names.

              Output: Regex
              On Failure: None

              Written By: JSL
              Version: 1.0
              Revisions: None
 
        
        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createdirectoryforGoodBadpredictiondata(self):

        """

             Method Name: createdirectoryforGoodBadpredictiondata
             Description: This method is used to create directories for good and bad prediction data.
             Output: Directory
             On failure: OS Error,Exception

              Written By: JSL
              Version: 1.0
              Revisions: None


        """
        try:
            #file = open("prediction_logs/General_logs.txt",'a+')
            self.logger.insert_records_into_collection("wafer","General_logs","Entered inside the createdirectoryforGoodBadpredictiondata method inside prediction raw data class")
            #file.close()
            Good_data_path = "Raw_prediction_data/Good_data"
            Bad_data_path = "Raw_prediction_data/Bad_data"

            if not os.path.isdir(Good_data_path):
                os.makedirs(Good_data_path)

            if not os.path.isdir(Bad_data_path):
                os.makedirs(Bad_data_path)
            #file = open("prediction_logs/General_logs.txt",'a+')
            self.logger.insert_records_into_collection("wafer","General_logs","Created Directory for good data and bad data:: %s" %Good_data_path)
            #file.close()
        except OSError:
            #file = open("prediction_logs/General_logs.txt",'a+')
            self.logger.insert_records_into_collection("wafer","General_logs", "Error Occurred while creating directory %s" %OSError )
            raise OSError

    def deletedirectoryforGooddata(self):
        """
              Method Name:  deletedirectoryforGooddata
              Description: This method will be used to delete the good data directory
              after moving the data to prediction db for space optimization.

              Output:None
              On Failure: OS Error

              Written By: JSL
              Version: 1.0
              Revisions: None

        """
        #file = open("prediction_logs/General_logs.txt",'a+')
        self.logger.insert_records_into_collection("wafer","General_logs","Entered inside deletedirectoryforGooddata inside prediction_raw_data class")
        #file.close()

        #Bad_data_path = "Raw_prediction_data/Bad_data"
        try:
            Good_data_path = "Raw_prediction_data/Good_data"
            if os.path.isdir(Good_data_path):
                shutil.rmtree(Good_data_path)

            #file = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","General_logs", "Deleted good data directory Successfully!!")
            #file.close()

        except OSError as e:
            #file = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","General_logs", "Error Occurred while deleting the good data dat directory.Exception Message::" + str(e))
            #file.close()
            raise OSError


    def deletedirectoryforBaddata(self):
        """
              Method Name:  deletedirectoryforBaddata
              Description: This method will be used to delete the bad data directory
              after moving the data to prediction db for space optimization.

              Output:None
              On Failure: OS Error

              Written By: JSL
              Version: 1.0
              Revisions: None

        """
        #file = open("prediction_logs/General_logs.txt", 'a+')
        self.logger.insert_records_into_collection("wafer","General_logs", "Entered inside deletedirectoryforBaddata method inside prediction_raw_data class")
        #file.close()
        #Good_data_path = "Raw_prediction_data/Good_data"

        try:
            Bad_data_path = "Raw_prediction_data/Bad_data"
            if os.path.isdir(Bad_data_path):
                shutil.rmtree(Bad_data_path)

            #file = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","General_logs", "Deleted bad data directory Successfully!!")
            #file.close()

        except OSError as e:
            #file = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","General_logs", "Error Occurred while deleting the bad data directory.Exception Message::" + str(e))
            #file.close()
            raise OSError

    def moveBadDatatoArchivebad(self):

        """
             Method Name:  moveBadDatatoArchivebad
              Description: This method will be used to delete the bad data directory
              after moving the data to archive bad directory for notifying the client regarding the
              invalid data issue.

              Output:None
              On Failure: OS Error

              Written By: JSL
              Version: 1.0
              Revisions: None

        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            #file = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","General_logs", "Entered inside moveBadDatatoArchivebad method inside prediction_raw_data class")
            #file.close()
            Bad_data_path = "Raw_prediction_data/Bad_data"
            Archive_bad_path = "Prediction_Archive_Bad_data"
            if not os.path.isdir(Archive_bad_path):
                os.makedirs(Archive_bad_path)
            src_path = Bad_data_path
            dest_path = "Prediction_Archive_Bad_data/ArchiveBad_" +str(date) + '_' + str(time)
            if not os.path.isdir(dest_path):
                os.makedirs(dest_path)
            #f = open("prediction_logs/General_logs.txt", 'a+')
            for file in os.listdir(src_path):
                if file not in os.listdir(dest_path):
                    shutil.move(src_path + "/" + file,dest_path)
            #f = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","General_logs", "Successfully moved the Bad files to Archive bad Directory!!")
            #f.close()
            if os.path.isdir(Bad_data_path):
                shutil.rmtree(Bad_data_path)
            #f = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","General_logs", "Successfully Deleted the Bad data directory!!")
            #f.close()
        except OSError as e:
            #file = open("prediction_logs/General_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","General_logs", "Error occurred while moving the bad data files to archive bad directory!!.Exception Message:: %s" %e)
            #file.close()
            raise e

    def raw_file_name_validation(self,regex,LengthOfDateStampInFile):


        """
             Method Name:raw_file_name_validation
             Description: This method will be used to validate the raw file names wfor the prediction, if
             the file name is valid it will be moved to good data and if invalid move to bad directory:

             Output: Good data and Bad data
             On failure: OS error

              Written By: JSL
              Version: 1.0
              Revisions: None


        """
        ###Deleting the previously store bad data and good data directory in case last run was  unsuccessfull and files were not deleted.
        self.deletedirectoryforBaddata()
        self.deletedirectoryforGooddata()

        ###creating the directories for good and bad data
        self.createdirectoryforGoodBadpredictiondata()

        try:
            #file = open("prediction_logs/name_validation_logs.txt", 'a+')
            #self.logger.insert_records_into_collection("name_validation_logs","Entered the raw_file_name_validation method of prediction_data_validation")
            #file.close()
            Bad_data_path = "Raw_prediction_data/Bad_data"
            Good_data_path = "Raw_prediction_data/Good_data"
            for file in os.listdir(self.Batch_Directory):
                file_path = self.Batch_Directory + '/' + file
                if re.match(regex,file):
                    file_name = re.split(".csv",file)
                    file_name = re.split('_',file_name[0])
                    if len(file_name[1]) == LengthOfDateStampInFile:
                        if file not in os.listdir(Good_data_path):
                            shutil.copy(file_path,Good_data_path)
                            #f = open("prediction_logs/name_validation_logs.txt", 'a+')
                            self.logger.insert_records_into_collection("wafer","name_validation_logs","Copied the valid file to good_data folder %s" %file)
                            #f.close()
                        else:
                            pass
                    elif file not in os.listdir(Bad_data_path):
                        shutil.copy(file_path,Bad_data_path)
                        #f = open("prediction_logs/name_validation_logs.txt", 'a+')
                        self.logger.insert_records_into_collection("wafer","name_validation_logs", "Copied the invalid file to bad_data folder %s" % file)
                        #f.close()

                    else:
                        pass

                elif file not in os.listdir(Bad_data_path):
                    shutil.copy(file_path ,Bad_data_path)
                    #f = open("prediction_logs/name_validation_logs.txt", 'a+')
                    self.logger.insert_records_into_collection("wafer","name_validation_logs", "Copied the invalid file to bad_data folder %s" % file)
                    #f.close()

                else:
                    pass

        except OSError:
            #f = open("prediction_logs/name_validation_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","name_validation_logs", "Error occurred while moving files to good data and bad data folder %s" %OSError)
            #f.close()
            raise OSError
        except Exception as e:
            #f = open("prediction_logs/name_validation_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","name_validation_logs", "Exception occurred while moving files to good data and bad data folder %s" %e)
            #f.close()
            raise e

    def validateColumnLength(self,NumberofColumns):
        """

             Method Name: validateColumnLength
             Description: This method will be used to validate the length of columns of the prediction csv files.
             The invalid files will be to bad data folder and valid files will be to good data folder.
             The csv files are missing the first column name so renaming it as "wafer".

             Output: Good data and Bad data
             On failure: OS error

              Written By: JSL
              Version: 1.0
              Revisions: None

        """
        #f = open("prediction_logs/column_validation_logs.txt", 'a+')
        self.logger.insert_records_into_collection("wafer","column_validation_logs", "Entered the validateColumnLength method of prediction_data_validation")
        #f.close()
        try:
            Good_data_path = "Raw_prediction_data/Good_data"
            Bad_data_path = "Raw_prediction_data/Bad_data"
            for file in os.listdir(Good_data_path):
                if file.endswith(".csv"):
                    file_path = Good_data_path + '/' + file
                    df = pd.read_csv(file_path)
                    if df.shape[1] == NumberofColumns:
                        df.rename(columns = {"Unnamed: 0": "Wafer"},inplace = True)
                        df.to_csv(file_path,index = None,header = True)
                        #f = open("prediction_logs/column_validation_logs.txt", 'a+')
                        self.logger.insert_records_into_collection("wafer","column_validation_logs", "The file names with valid column lengths are :: %s" %file)
                        #self.logger.insert_records_into_collection("column_validation_logs", "The column name has been renamed Successfully" % file)
                        #f.close()
                    elif file not in os.listdir(Bad_data_path):
                        shutil.move(file_path,Bad_data_path)
                    else:
                        pass
        except OSError as e:
            raise e
            #f = open("prediction_logs/column_validation_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","column_validation_logs", "Error occurred while validating the columns length %s" %e)
            #f.close()
        except Exception as e:
            raise e
            #f = open("prediction_logs/column_validation_logs.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","column_validation_logs", "Exception occurred while validating the columns length %s" % e)
            #f.close()

    def deletepredictionfile(self):

        if os.path.exists('Prediction_Output_File/Predictions.csv'):
            os.remove('Prediction_Output_File/Predictions.csv')

    def validatemissingvaluesinwholecolumn(self):
        """

              Method Name: validatemissingvaluesinwholecolumn
              Description: This method will be used to validate the given prediction csv
              if there is any column with having missing values throughout the column.
              This method will also change the unnamed column name in csv file "wafer".

              Output: None
              On failure: Exception

              Written By: JSL
              Version: 1.0
              Revisions: None


        """
        Good_data_path = "Raw_prediction_data/Good_data"
        Bad_data_path = "Raw_prediction_data/Bad_data"
        try:
            #f = open("prediction_logs/missingvaluesincolumn.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","missingvaluesincolumn", "Entered the validatemissingvaluesinwholecolumn method of prediction_data_validation")
            for file in os.listdir(Good_data_path):
                file_path = Good_data_path + '/' + file
                if file.endswith(".csv"):
                    # file_path = Good_data_path + '/' + file
                    df = pd.read_csv(file_path)
                    count = 0
                    for cols in df:
                        if (df[cols].count()) == 0:
                            # print(cols)
                            count += 1
                            if file not in os.listdir(Bad_data_path):
                                shutil.move(file_path, Bad_data_path)
                                #f = open("prediction_logs/missingvaluesincolumn.txt", 'a+')
                                self.logger.insert_records_into_collection("wafer","missingvaluesincolumn","Invalid files moved from good data to bad data %s" %file)
                                #f.close()
                            else:
                                pass
                    if count == 0:
                        df.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                        df.to_csv(file_path, index=None, header=True)
                    #f = open("prediction_logs/missingvaluesincolumn.txt", 'a+')
                    self.logger.insert_records_into_collection("wafer","missingvaluesincolumn", "Unnamed column name changed successfully")
                    #f.close()

        except OSError:
            #f = open("prediction_logs/missingvaluesincolumn.txt", 'a+')
            self.logger.insert_records_into_collection("wafer","missingvaluesincolumn", "Error occurred while validating the column length. %s" %OSError)
            raise OSError
        except Exception as e:
            self.logger.insert_records_into_collection("wafer","missingvaluesincolumn", "Exception occurred while validating the column length. %s" % OSError)
            #f.close()
            raise e

















































    





            
        
            
            
           
		
        
	
	   

	












   
	
	
	

		

	
	   

	












