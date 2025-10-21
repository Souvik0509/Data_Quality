import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from sqlalchemy import create_engine    
from sqlalchemy.engine import Engine
import urllib 
import logging 
import pandas as pd 



class CommonUtil:

    @staticmethod
    def validate_file_extension(file_name, allowed_extensions):
        _, ext = os.path.splitext(file_name)
        return ext.lower() in allowed_extensions
    
    @staticmethod
    def get_secrets(keyvalut_name: str, secret_name: str) -> str:
        try:
            kvault_url = f"https://{keyvalut_name}.vault.azure.net"
            cred = DefaultAzureCredential()
            client = SecretClient(vault_url=kvault_url, credential=cred)
            secret = client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            logging.exception(f" Error retrieving secret {secret_name} from Key Vault {keyvalut_name}")
            raise e
        

    @staticmethod 
    def create_sql_server_engine(server,database,username,password,driver="ODBC Driver 17 for SQL Server"):
        """
        Creates a SQLAlchemy engine for connecting to a SQL Server database.
         """ 
        
        quoted_passwrd = urllib.parse.quote_plus(password)
        #Format = "mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
        connection_string =  (
        f'mssql+pyodbc://{username}:{quoted_passwrd}@{server}/'
        f'{database}?driver={driver}')
        try :
            engine = create_engine(connection_string)
            return engine 
        except Exception as e:
             logging.exception(" Error creating SQL Server engine")
             raise e

    @staticmethod
    def store_data_in_sqldb(engine, table_name: str, df:pd.DataFrame,schema: str = 'dbo',if_exists: str = "replace"):
        """ 
        pushing dataset generated from the data quality framework 
        """
        try:
            logging.info(f"Writing DataFrame to SQL Server table: {schema}.{table_name if schema else table_name}")
            df.to_sql(
                name = table_name,
                con = engine,
                schema = schema, 
                if_exists = if_exists,  ## want to overwrite for each entry 
                index = False,
                chunksize=2000,
                method = 'multi' 

            )
            logging.info(f" Succesfully written to {schema}.{table_name} if {schema} else {table_name}")
        except Exception as e : 
            logging.exception(f" failed to write dataframe to Sql Server : {e}")
            raise e 
        
    @staticmethod   
    def convert_to_dataframe(report : list) -> pd.DataFrame:
        try : 
            report_df = pd.DataFrame(report)
            if report_df.empty:
                logging.warning(" The report dataframe is empty ")
            return report_df
        except Exception as e :
            logging.exception(" Error in converting the report to dataframe")
            raise e
    @staticmethod    
    def save_report(report, folder_path : str , filename : str):
            try : 
                report_df = CommonUtil.convert_to_dataframe(report)
                os.makedirs(folder_path,exist_ok=True)
                new_flename = filename.split(".")[0] + "_data_quality_report.csv"
                file_path = os.path.join(folder_path,new_flename)

                if os.path.exists(file_path):
                    logging.warning(f" Output file already exists and will be overwritten : {file_path}")
                    os.remove(file_path)
                
                report_df.to_csv(file_path,index=False)
                logging.info(f" Data Quality Report saved successfully at {file_path}")
            except Exception as e :
                logging.exception(" Error in saving the report to csv")
                raise e
        