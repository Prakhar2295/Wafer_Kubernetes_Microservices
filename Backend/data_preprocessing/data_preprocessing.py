import pandas as pd
from sklearn.impute import KNNImputer
import numpy as np
from application_logging.logger import App_Logger
from application_logging.mongodb_logger import mongodb_logger

class Preprocessing:

	"""
	      This class will be used to clean & transform the data before training.

	      Written By: JSL
	      Version: 1.0
	      Revisions: None

	  """

	def __init__(self):
		self.logger_object = mongodb_logger()
		#self.file_path = "Training_logs/Main_Training_log.txt"


	def remove_columns(self, data, column_names: list):

		"""
		     Method Name: remove_columns
		     Description: This method removes the given columns from the pandas dataframe.
		     Output: A dataframe after removing the specified dataframe.
		     On failure: An Exception is raised


		     Written By: JSL
		     Version: 1.0
		     Revisions: None

		"""

		try:
			df_new = data.drop(labels=column_names, axis=1)
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing", "Removed columns successfully inside remove_columns method")
			#self.file_object.close()
			return df_new
		except Exception as e:
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing","Error occurred while removing columns.Exception message::%s"%e)
			#self.file_object.close()
			raise e

	def separate_label_features(self, data, label_column_names):

		"""
		             Method: separate_label_features
		             Description: This method separates the features and the label column.
		             Output: Returns two separate dataframes, one containing the features and the other containing labels.
		             On failure : Raises Exception


		             Written By: JSL
		             Version: 1.0
		             Revisions: None

		   """


		try:
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing","Entered inside the separate label features method inside the preprocessing class.")
			#self.file_object.close()
			self.X = data.drop(labels=label_column_names, axis=1)
			self.Y = data[label_column_names]
			return self.X, self.Y
		except Exception as e:
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing","Exception occurred in separating label features.Seapationg features failed ::%s"%e)
			#self.file_object.close()
			raise e

	def is_null_present(self, data):

		"""
		            Method Name: is_null_present
		            Description: This method is used to check whether the given dataframe
		            is having any null_values presnt in it.This method will further crate the dataframe
		            of all the columns with null_values present in the dataframe.

		            Output: Returns a Boolean value True if the null values are present in the dataframe
		            and FALSE if the null values are not present in the dataframe.

		            On Failure: Raises an Exception

		           Written By: JSL
		           Version: 1.0
		           Revisions: None

		       """


		try:
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing","Entered inside the is_null_present method inside the preprocessing class")
			#self.file_object.close()
			#self.data = data
			is_null_present = False
			for i in (data.isnull().sum()):
				if i > 0:
					is_null_present = True
					break
			if is_null_present:
				df_null_values = pd.DataFrame()
				df_null_values["column_name"] = data.columns
				df_null_values["missing_values_count"] = np.array(data.isnull().sum())
				df_null_values.to_csv("preprocessing_data/null_values.csv", index=False)
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing","Finding null values inside the column completed successfully")
			#self.file_object.close()
			return is_null_present
		except Exception as e:
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing", "Exception occurred inside is_null_present method inside preprocessing class:: %s"%e)
			#self.file_object.close()
			raise e

	def missing_value_imputation(self, data):

		"""
		       Method Name: impute_missing_values
		       Description: This method impute all the missing values using KNN imputer.

		       Output:A dataframe which has all the missing values imputed.
		       On failure: Raises an Exception

		       Written By: JSL
		       Version: 1.0
		       Revisions: None


		"""

		try:
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing","Entered inside the missing_value_imputation method inside the preprocessing class")
			#self.file_object.close()
			self.data = data
			imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values=np.nan)
			self.data_new = imputer.fit_transform(self.data)
			self.data_imputed = pd.DataFrame(self.data_new, columns=self.data.columns)
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing", "Missing values imputation completed successfully !!")
			#self.file_object.close()
			return self.data_imputed
		except Exception as e:
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing","Exception occurred inside missing_value_imputation method inside preprocessing class:: %s" % e)
			#self.file_object.close()
			raise e

	def cols_with_zero_std_deviation(self, data):

		"""
		        Method Name: get_columns_with_zero_std_deviation
		        Description: This method will be used to compute the compute columns with zero
		        standard deviation.

		        Output: List of columns with zero standard deviation
		        On failure: Raises an exception

		        Written By: JSL
		        Version: 1.0
		        Revisions: None

		"""

		try:
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing","Entered inside the cols_with_zero_std_deviation method inside the preprocessing class")
			#self.file_object.close()
			self.data = data
			self.data_desc = self.data.describe()
			self.data_std = self.data_desc.loc[["std"]]     ####Creating a pandas dataframe of the standard deviation row with all columns
			self.index_cols_drop = []         ###indexes of columns to be dropped!!Declaring an empty list variable!!
			for i in range(len(self.data_std.columns)):  ####appendind the index of columns to be dropped
				if (self.data_std.iloc[:, i] == 0).all():
					self.index_cols_drop.append(i)
			# index_cols_drop
			cols_list = list(self.data_std.columns)
			self.drop_cols_list = []  ####list of column names to be dropped
			for i in self.index_cols_drop:
				self.drop_cols_list.append(cols_list[i])
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing", "Found column names with_zero_std_deviation Successfully")
			#self.file_object.close()
			return self.drop_cols_list
		except Exception as e:
			#self.file_object = open(self.file_path, 'a+')
			self.logger_object.insert_records_into_collection("wafer","preprocessing","Exception occurred inside column with zero std deviation method inside preprocessing class:: %s" % e)
			#self.file_object.close()
			raise e