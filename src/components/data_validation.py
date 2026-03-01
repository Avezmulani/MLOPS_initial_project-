import json
import os
import sys 

import pandas as pd 
from pandas import DataFrame

from src.exception import MyException
from src.logger import logging
from src.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from src.entity.config_entity import DataValidationConfig
from src.constants import SCHEMA_FILE_PATH
from src.utils.main_utils import read_yaml_file, write_yaml_file

class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise MyException(e,sys)
    
    def validate_number_of_columns(self,dataframe:DataFrame) -> bool:
        try:
            status = len(dataframe.columns) == len(self.schema_config['columns'])
            logging.info(f'Is required colum present : [{status}]')
            return status
        except Exception as e:
            raise MyException(e,sys)
        
    def is_column_exist(self,df:DataFrame) -> bool:
        try:
            dataframe_colums = df.columns
            missing_numerical_columns = []
            missing_categorical_columns =[]

            for column in self.schema_config['numerical_columns']:
                if column not in dataframe_colums:
                    missing_numerical_columns.append(column)
            
            if len(missing_numerical_columns) > 0:
                logging.info(f'Missing numerical column : {missing_numerical_columns}')
            
            for column in self.schema_config['categorical_columns']:
                if column not in dataframe_colums:
                    missing_categorical_columns.append(column)
            
            if(len(missing_categorical_columns) > 0):
                logging.info(f' Missing Categorical Column : {missing_categorical_columns}')
            
            return False if len(missing_categorical_columns) >0 or len(missing_numerical_columns)>0 else True
        except Exception as e:
            raise MyException(e,sys) from e

    @staticmethod
    def read_data(file_path) -> DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise MyException(e,sys)
    
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            validation_error_msg ="" 
            logging.info('Starting data validation')
            train_df,test_df = (DataValidation.read_data(file_path=self.data_ingestion_artifact.trained_file_path),
                                DataValidation.read_data(file_path=self.data_ingestion_artifact.test_file_path))
            
            status = self.validate_number_of_columns(dataframe=train_df)

            if not status:
                validation_error_msg += f'columns are missing in training dataframe'
            else:
                logging.info('All required columns are present in training dataframe :{status}')
            
            status = self.validate_number_of_columns(dataframe=test_df)

            if not status :
                validation_error_msg += f'columns are missing in the test dataframe'
            else:
                logging.info(f'All required columns are present in testing dataframe : {status}')
            
            status = self.is_column_exist(df = train_df)

            if not status:
                validation_error_msg += f'Columns are missing in training dataframe'
            else:
                logging.info(f'All categorical/int columns presennt in training dataframe: {status}')
            
            status  = self.is_column_exist(df = test_df)

            if not status:
                validation_error_msg += f'Columns are missing in test dataframe'
            else:
                logging.info(f'All categorical/int columns present in test dataframe: {status}')

            validation_status = len(validation_error_msg) == 0

            data_validation_artifact = DataValidationArtifact(
                validation_status= validation_status,
                validation_report_file_path= self.data_validation_config.validation_report_file_path,
                message= validation_error_msg
            )
            
            report_dir = os.path.dirname(self.data_validation_config.validation_report_file_path)
            os.makedirs(report_dir,exist_ok=True)

            validaion_report ={
                'validation_status' : validation_status,
                'message': validation_error_msg.strip()
            }

            with open(self.data_validation_config.validation_report_file_path,"w") as report_file:
                json.dump(validaion_report,report_file,indent=4)
            
            logging.info('Data Validation artifact created and saved to JSON file')
            logging.info(f'data validation artifact : {data_validation_artifact}')
            return data_validation_artifact
        
        except Exception as e:
            raise MyException(e,sys) from e


            
            



                

        


