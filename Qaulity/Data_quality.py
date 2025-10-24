import pandas 
from Load_data.Data_Load import DataLoader
from ValidationRule.Validations import QualityRules
from helper.config_json_loader import ConfigJsonLoad
import logging
import os 



class QualityReport:

    ## what will be having in this report , we will generate the total quality like 
    ## accuracy, completeness, validity ,timeliness for different columns as mentioned 
    ## will create different methods to jusge these and then store it in a excel for different columns 
    ## then will modify the config to have threshold for these quality metrics
    ## then will create a method to validate these metrics against threshold and generate a final report
    def __init__(self, df : pandas.DataFrame, config_path : str):
        self.df = df 
        self.config = ConfigJsonLoad.config_json_load(config_path)
        self.quality_rules = QualityRules(self.df)


    def rules_execution(self,file_name):
        filename = self.config["fileName"]
        report = []
        if filename == file_name:
           # print(" File name matched, proceeding with quality checks")
            try :
                quality_rules = self.quality_rules
                #print(quality_rules)
                for column , values in self.config["columns"].items(): 
                    rules = values.get("Rules",[])
                    #print(rules)
                    if values.get("rules_enabled",False):
                        print(f" Executing rules for column : {column}")
                    else:
                        print(f" Skipping rules for column : {column} as rules_enabled is set to False")
                        continue
                    if values.get("regex_pattern_flg",False):
                        print(f"Extracting regex pattern for column : {column}")
                    else:
                        print(" Regex pattern flag is not set, skipping regex pattern extraction")
                        
                    mandatory = values.get("mandatory",False)
                    if mandatory and column not in self.df.columns:
                        logging.error(f" Mandatory column {column} is missing in the dataframe")
                        raise ValueError(f" Mandatory column {column} is missing in the dataframe")
                    for rule in rules : 
                        if not hasattr(quality_rules,rule):
                            logging.warning(f" Rule {rule} is not implemented in QualityRules class.")
                            report.append({
                                "Column_Name" : column,
                                "rule_name" : rule,
                                "Status" : "CANNOT_BE_EXECUTED",
                                "Fail_reason" : f" Rule {rule} is not implemented in QualityRules class.",
                                "mandatory" : mandatory
                            })
                            continue
                        #print(f" Executing rule {rule} on column {column}")
                        func = getattr(quality_rules,rule)
                        args = self.get_rules_arg(rule,values,column)
                        #print("checking for args")
                        #print(args)
                        try :
                            result,reason = func(*args) 
                            print(f" Result for rule {rule} on column {column} is {result}")   
                            report.append({
                                "Column_Name" : column,
                                "rule_name" : rule,
                                "Status" : "PASS" if result else "FAIL",
                                "Fail_reason" : "No Fail Reason" if result else  reason or "Validation Failed",
                                "mandatory" : mandatory
                            })
                        except Exception as e :
                            logging.exception(f"Error Executing rule for {rule} on column {column}")
                            report.append({
                                "Column_Name" : column,
                                "rule_name" : rule,
                                "Status" : "ERROR",
                                "Fail_reason" : str(e),
                                "mandatory" : mandatory})
            except Exception as e :
                logging.exception(" Error in executing the quality rules")
                raise e
        return report  


    def get_rules_arg(self,rule,items,column):
        """ method created to dynamically map config params for each rule """

        param_map = {
            "data_type_check": [column, items.get("Data_Type", "")],
            "range_check": [column, items.get("min_value", None), items.get("max_value", None)],
            "valid_email_format": [column],
            "special_char_check": [column,items.get("regex_pattern","")],
            "null_check": [column],
            "unique_check": [column],
        }

        return param_map.get(rule, [column])

            
    # def convert_report_to_dataframe(self, report : dict) -> pandas.DataFrame:
    #         try : 
    #             report_df = pandas.DataFrame.from_dict({(i,j): report[i][j] 
    #                                                     for i in report.keys() 
    #                                                     for j in report[i].keys()},
    #                                                        orient='index',columns=["Result"])
    #             return report_df
    #         except Exception as e :
    #             logging.exception(" Error in converting the report to dataframe")
    #             raise e

    # def save_report(self, report_df : pandas.DataFrame, folder_path : str , filename : str):
    #     try : 

    #         os.makedirs(folder_path,exist_ok=True)
    #         new_flename = filename.split(".")[0] + "_data_quality_report.csv"
    #         file_path = os.path.join(folder_path,new_flename)

    #         if os.path.exists(file_path):
    #             logging.warning(f" Output file already exists and will be overwritten : {file_path}")
    #             os.remove(file_path)
            
    #         report_df.to_csv(file_path,index=False)
    #         logging.info(f" Data Quality Report saved successfully at {file_path}")
    #     except Exception as e :
    #         logging.exception(" Error in saving the report to csv")
    #         raise e
  