import yaml 
import os 
import glob
import logging 


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("etl_pipeline.log"), logging.StreamHandler()]
)


class ConfigYMLLoad: 


    @staticmethod
    def get_latest_file(folder_path,file_type="*.*"):
        files = glob.glob(os.path.join(folder_path,file_type))
        if not files : 
            logging.error("No File in {folder_path} with the {file_type} pattern")
            raise FileExistsError("Files not exists error")
        latest_file = max(files, key=os.path.getctime)
        logging.info(f"Latest file found: {latest_file}")
        return os.path.basename(latest_file)


    @staticmethod
    def config_load(config_path : str = "config.yml") -> dict :
        if not os.path.exists(config_path): 
            raise FileNotFoundError(f" Config file not found : {config_path}")
        
        else:
            try : 
                with open(config_path,"r") as f : 
                    config = yaml.safe_load(f)

                if "input" in config:
                    _input_folder_path = config["input"]["input_folder"]
                    _input_file_type = config["input"]["file_type"]
                    latest_file = ConfigYMLLoad.get_latest_file(_input_folder_path,_input_file_type)
                    config["input"]["input_file"] = os.path.join(_input_folder_path,latest_file)
                    return config
            except Exception as e : 
                     logging.exception("Error Loading the config")
                     raise e 

            
            
        
#class ConfigJsonLoad:
     
