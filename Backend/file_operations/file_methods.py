import os
import pickle
import shutil
from datetime import datetime

class File_operation:

    """
            This Class Shall be used to save the model to a file after training and
            load the saved model for prediction.

            Written By: JSL
            Version: 1.0
            Revisions: None

    
    
    """
    def __init__(self,logger_object):
        #self.file_path = file_path
        self.logger_object = logger_object
        self.model_directory = "models"

    def save_model(self,model,filename):

                         
       """
               Method Name: save_model
               Description: Save the model file to specific directory
               Outcome: None
               On failure: Raises an Exception


                Written By: JSL
                Version: 1.0
                Revisions: None       

        
        """
       #self.file_path = "Training_Logs/ModelTrainingLog.txt"
       #self.file_object = open(self.file_path,'a+')
       self.logger_object.insert_records_into_collection("wafer","ModelTrainingLog","Entered the save model method inside the file_operation class")
       #self.file_object.close()

       try:
           path = self.model_directory + "/" + filename
           if not os.path.isdir(path):
               os.makedirs(path)
           else:
               pass
           with open(path +'/' + filename + '.sav','wb') as f:
                   pickle.dump(model,f)
           #self.file_object = open(self.file_path, 'a+')
           self.logger_object.insert_records_into_collection("wafer","ModelTrainingLog",'Model File' +filename +'saved.Exited the saved_model method of the file operation class')
           #self.file_object.close()
           return "success"
       except Exception as e:
           #self.file_object = open(self.file_path, 'a+')
           self.logger_object.insert_records_into_collection("wafer","ModelTrainingLog","Exiting the saved_model method of the file operation class")
           self.logger_object.insert_records_into_collection("wafer","ModelTrainingLog","Exception occurred while saving the model.Exception message:: %s"%e)
           #self.file_object.close()
           raise Exception()
       
    def load_model(self,filename):

        """
             Method Name: load_model
             Description: This method will load the trained model saved in a pickle file.
             Output: Load the trained model into memory
             On failure: Raises an exception

             Written By: JSL
             Version: 1.0
             Revisions: None

        """
        #self.file_object = open(self.file_path, 'a+')
        self.logger_object.insert_records_into_collection("wafer","ModelTrainingLog", 'Entered the load_model method of the file_operation class')

        try:
            with open(self.model_directory +"/" + filename + '/' + filename + '.sav','rb') as f:

                self.logger_object.insert_records_into_collection("wafer","ModelTrainingLog",'Model File' + filename+ 'loaded.Exited The load_model method of the file_oipration class')
                #self.file_object.close()
                return pickle.load(f)
        except Exception as e:
            #self.file_object = open(self.file_path, 'a+')
            self.logger_object.insert_records_into_collection("wafer","ModelTrainingLog"," Exception occurred model loading unsuccessfull.Exception meassage ::%s" %e)
            self.logger_object.insert_records_into_collection("wafer","ModelTrainingLog","Model loading failed.Exiting the load_model method from file_operation class")
            #self.file_object.close()
            raise Exception() 
        
    def find_correct_model_file(self,cluster_number):

        """
             Method Name: find_correct_model_file
             Description: Selects the correct model file for the given cluster number.

             Output: The model file.
             On failure: Raises an Exception

             Written By: JSL
             Version: 1.0
             Revisions: None

    
        """

        self.logger_object.insert_records_into_collection("wafer","Find-correct-model","Entered inside the find_correct_model_file inside file_operations class")

        try:
            self.cluster_number = cluster_number
            self.folder_name = self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if self.file[-1] == str(self.cluster_number):
                        self.model_name = self.file
                except:
                    continue
            self.model_name = self.model_name.split('.')[0]
            self.logger_object.insert_records_into_collection("wafer","Find-correct-model","Exited the find_correct_model_file method of the file opration class")

            return self.model_name

        except Exception as e:
            self.logger_object.insert_records_into_collection("wafer","Find-correct-model","Failed to finds the the correct model file")
            self.logger_object.insert_records_into_collection("wafer","Find-correct-model","Exception occurred.Exception message :: %s" %e)

            raise Exception()
    def move_old_models_to_archive(self):
        """
              Method Name: move_old_models_to_archive
              Desciprtion: This method will be used to move the previously trained models to
              to archive directory.

              Output: None
              On failure: Raises an Exception

              Written By: JSL
              Version: 1.0
              Revisions: None

        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        path = "Archived_Models"
        try:
            if not os.path.isdir(path):
                os.makedirs(path)
                #print("The archive folder has been created")
            #print("The archive folder already exists")
            src_path = path + "/" + "models_" + str(date) + "_" +str(time)
            if os.path.exists(self.model_directory):
                if not os.path.isdir(src_path):
                    os.makedirs(src_path)
                    for file in os.listdir(self.model_directory):
                        shutil.move(self.model_directory + '/' + file, src_path)
            #print("All the previous files moved to archive")
        except Exception as e:
            raise e





                     


            
            

       
       
               
             
               
        
               
                  


                   
               
         