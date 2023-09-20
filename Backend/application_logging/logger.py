from datetime import datetime


class App_Logger:


    """"
    This is the Logger class which will be used to log messages to the file.

    Written by: JSL
    Version: 1.0
    Revisions: None

    """


    

    def __init__(self):
        pass

    def log(self,file_object,log_message:str):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        file_object.write(str(self.date) + "/" + str(self.current_time) + "\t\t" + log_message + "\n")




