#import yaml 
import os
#from helper.config_load import ConfigYMLLoad
import pandas as pd 
import logging 
from abc import ABC, abstractmethod
from azure.storage.blob import BlobServiceClient
from helper.commonutil import CommonUtil
import io 

# class DataLoader:

#     ### loading different types of data based on config 
#     def __init__(self, config_path : str):
#         self.config_fle = ConfigYMLLoad.config_load(config_path) 


class DataLoader(ABC) : 

    def __init__(self,file_path : str,file_type : str = None):
        self.file_path = file_path
        self.file_type = file_type

    @abstractmethod
    def load_data(self):
        pass 

class CsvLoader(DataLoader):

    def __init__(self,file_path : str,file_type:str = "csv",header:bool=True,encoder:str="utf-8"):
        super().__init__(file_path,file_type)
        self.header = 0 if header else None 
        self.encoder = encoder
    
    def load_data(self):
        try : 
            if os.path.exists(self.file_path) == False:
                raise FileNotFoundError(f" File not found : {self.file_path}")
            df = pd.read_csv(self.file_path,header = self.header,encoding= self.encoder)
            logging.info(f"File loaded successfully for : {self.file_path}")
            if df.shape[0]!=0:
                return df
            else:
                logging.error(f" {df} :  Dataframe is Empty ")
        except Exception as e :
            logging.exception(f" Error loading file from  {self.file_path}")
            raise e
        
class ExcelLoader(DataLoader):
    def __init__(self,file_path : str,file_type:str = "excel"):
        super().__init__(file_path,file_type)
    
    def load_data(self):
        try : 
            if os.path.exists(self.file_path) == False:
                raise FileNotFoundError(f" File not found : {self.file_path}")
            df = pd.read_excel(self.file_path)
            logging.info(f"File loaded successfully for : {self.file_path}")
            if df.shape[0]!=0:
                return df
            else:
                logging.error(f" {df} :  Dataframe is Empty ")
        except Exception as e :
            logging.exception(f" Error loading file from  {self.file_path}")
            raise e

    
class BlobStorageLoader(DataLoader):
    def __init__(self,secret_conn_str,container_name,blob_name,blob_path):
        self.secret_conn_str = secret_conn_str
        self.container_name = container_name 
        self.blob_name = blob_name
        self.blob_path = blob_path

    def load_data(self,keyvault_name: str):
        try:
            conn_str = CommonUtil.get_secrets(keyvault_name, self.secret_conn_str)
            blob_service_client = BlobServiceClient.from_connection_string(conn_str)
            container_client = blob_service_client.get_container_client(self.container_name)
            blob_client = container_client.get_blob_client(self.blob_name)
            download_file_path = os.path.join(self.blob_path, self.blob_name)

            downloader = blob_client.download_blob() 
            data = downloader.readall()
            stream = io.BytesIO(data)

            if self.blob_path.lower().endswith('.csv'):
                df = pd.read_csv(stream)
            elif self.blob_path.lower().endswith(('.xls', '.xlsx')):
                df = pd.read_excel(stream)
            elif self.blob_path.lower().endswith((".parquet", ".pq")):
                df = pd.read_json(stream)
            else:
                raise ValueError(f"Unsupported file format for blob: {self.blob_name}")
            return df

        except Exception as e:
            logging.exception(f"Error downloading blob {self.blob_name} from container {self.container_name}")
            raise e


    
        


        
        

        
        
