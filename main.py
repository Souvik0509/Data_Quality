import pandas as pd 
from Load_data.Data_Load import CsvLoader,ExcelLoader
from helper.config_json_loader import ConfigJsonLoad
from Qaulity.Data_quality import QualityReport
from helper.commonutil import CommonUtil
import logging
import argparse
import os 
from dotenv import load_dotenv
load_dotenv()
# loader = DataLoader("config.yml")
# dataset = loader.load_data()
# print(dataset.columns)
# print(dataset.head())
# config = ConfigJsonLoad.config_json_load("config/config_exmp.json")
# print(config['columns']) 


    
def main():
    output_folder_path = os.getenv('OUTPUT_PATH')
    print(f" Output folder path is : {output_folder_path}")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 
    argparser = argparse.ArgumentParser(description="Data Quality Check")
    argparser.add_argument("--config", type=str, required=True, help="Path to the config file")
    argparser.add_argument("--datafile", type=str, required=True, help="Path to the data file")
    args = argparser.parse_args()
    
    if not args.config or not args.datafile:
        raise ValueError(" Both config and datafile arguments are required")
    if not os.path.exists(args.config):
        raise FileNotFoundError(f" Config file not found : {args.config}")
    datafile = args.datafile
    data_loader = CsvLoader(datafile,"csv")
    config_fle = ConfigJsonLoad.config_json_load(args.config)
    file_name = config_fle['fileName']
    print(file_name)
    try:
        df = data_loader.load_data()
        if df is None or df.shape[0] == 0:
            logging.error("The dataset is empty.")
            raise ValueError("The loaded dataset is empty.")
        quality_report = QualityReport(df, args.config)
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise


    #print(df.head())s
    report = quality_report.rules_execution(file_name)
    #print(report)
    print(os.path.join(output_folder_path,file_name))
    if os.path.exists(os.path.join(output_folder_path,file_name)):
        print(f" Output file already exists and will be overwritten : {os.path.join(output_folder_path,file_name)}")
    else:
        print(f" PATH DOESNOT EXISTS")
    #quality_report.save_report(pd.DataFrame(report),output_folder_path,file_name)
    CommonUtil.save_report(report,output_folder_path,file_name)


if __name__ == "__main__":
    main() 



