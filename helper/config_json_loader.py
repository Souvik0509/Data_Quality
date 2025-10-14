import os 
import glob
import logging 
import json 
class ConfigJsonLoad: 

    @staticmethod
    def config_json_load(config_path : str = "config/config_exmp.json") -> dict :
        if not os.path.exists(config_path): 
            raise FileNotFoundError(f" Config file not found : {config_path}")
        
        else:
            try : 
                
                with open(config_path,"r") as f : 
                    config = json.load(f)
                    return config
            except Exception as e : 
                     logging.exception("Error Loading the config")
                     raise e