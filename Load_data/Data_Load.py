import yaml 
import os
#from helper.config_load import ConfigYMLLoad
import pandas as pd 
import logging 

# class DataLoader:

#     ### loading different types of data based on config 
#     def __init__(self, config_path : str):
#         self.config_fle = ConfigYMLLoad.config_load(config_path) 


class DataLoader : 

    def __init__(self,file_path : str,file_type : str = None):
        self.file_path = file_path
        self.file_type = file_type

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

    


    # def load_data(self):
    #     try : 
    #         input_file_path = self.config_fle["input"].get("input_file",None)
    #         header = 0 if self.config_fle["input"].get("header",True) else None 
    #         encoder = self.config_fle["input"].get("encoder","utf-8")
    #         ext = os.path.splitext(input_file_path)[1].lower()

    #         if ext == ".csv"  :
    #             df = pd.read_csv(input_file_path,header = header,encoding= encoder)
    #         elif ext == ".parquet":
    #             df = pd.read_parquet(input_file_path)
    #         elif ext == ".json" :
    #             df = pd.read_json(input_file_path)
    #         elif ext in [".xlsx",".xls"] : 
    #             df = pd.read_excel(input_file_path)
    #         else:
    #             raise (f"unsupported File Type : {ext}")
    #         logging.info(f"File loaded successfully for : {input_file_path}")

    #         if df.shape[0]!=0:
    #             return df
    #         else:
    #             logging.error(f" {df} :  Dataframe is Empty ")
    #     except Exception as e :
    #         logging.exception(f" Error loading file from  {self.config_fle["input"]["input_file"]}")
    #         raise e
        


        
        

        
        
