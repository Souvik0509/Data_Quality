import pandas as pd 
import re 
import logging



class QualityRules: 

    def __init__(self, df : pd.DataFrame): 
        self.df = df
    

    def null_check(self, column_name : str) -> bool:
        """ Check for null values in the specified column """
        if column_name not in self.df.columns:
            raise ValueError(f"Column {column_name} does not exist in DataFrame")
        logging.info(f"Null check results for column {column_name} is obtained")
        result = self.df[column_name].notnull()
        if not result.all():
            return False ,"Nulls Values Found"
        return True, "No Null Values"
    
    def unique_check(self, column_name : str) -> bool:
        """ Check for unique values in the specified column """
        if column_name not in self.df.columns:
            raise ValueError(f"Column {column_name} does not exist in DataFrame")
        logging.info(f"Unique check results for column {column_name} is obtained")
        result = self.df[column_name].is_unique
        if not result:
            return False, "Duplicate Values Found"
        return True, "All Values are Unique"
    
    def data_type_check(self, column_name : str, expected_type : str) -> bool:
        """ Check if the data type of the specified column matches the expected type """
        if column_name not in self.df.columns:
            raise ValueError(f"Column {column_name} does not exist in DataFrame")
        
        type_mapping = {
            "integer": pd.api.types.is_integer_dtype,
            "float": pd.api.types.is_float_dtype,
            "string": pd.api.types.is_string_dtype,
            "date": pd.api.types.is_datetime64_any_dtype,
            "boolean": pd.api.types.is_bool_dtype
        }
        
        if expected_type.lower() not in type_mapping:
            raise ValueError(f"Unsupported data type: {expected_type}")
        
        check_func = type_mapping[expected_type.lower()]
        result_Series = self.df[column_name].apply(lambda x: check_func(pd.Series([x])))
        logging.info(f"Data type check results for column {column_name} is obtained")
        if not result_Series.all():
            return False, f"Data type mismatch. Expected {expected_type}"
        return True, "Data type matches"
    
    def valid_email_format(self, column_name : str) -> bool:
        """ Check for valid email format in the specified column """
        if column_name not in self.df.columns:
            raise ValueError(f"Column {column_name} does not exist in DataFrame")
        
        email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        result_Series = self.df[column_name].apply(lambda x: bool(email_pattern.match(str(x))) if pd.notnull(x) else False)
        logging.info(f"Email format check results for column {column_name} is obtained")
        if result_Series.all():
            return True, "ALL email formats are valid"
        return False, "All email formats are not valid"
    
    def Range_check(self, column_name : str, min_value : float = None, max_value : float = None) -> pd.Series:
        """ Check if the values in the specified column fall within the given range """
        if column_name not in self.df.columns:
            raise ValueError(f"Column {column_name} does not exist in DataFrame")
        
        if min_value is None and max_value is None:
            raise ValueError("At least one of min_value or max_value must be specified")
        
        if min_value is not None and max_value is not None:
            result_Series = self.df[column_name].between(min_value, max_value)
        elif min_value is not None:
            result_Series = self.df[column_name] >= min_value
        else:  # max_value is not None
            result_Series = self.df[column_name] <= max_value
        logging.info(f"Range check results for column {column_name} is obtained")
        if result_Series.all():
            return True, "All values within range"
        return False , f"Values out of range. Expected between {min_value} and {max_value}"
        
    
    def special_char_check(self, column_name : str) -> pd.Series:
        """ Check for special characters in the specified column """
        if column_name not in self.df.columns:
            raise ValueError(f"Column {column_name} does not exist in DataFrame")
        
        pattern = r'[^a-zA-Z0-9\s]'
        result_Series = self.df[column_name].str.contains(pattern, regex=True)
        logging.info(f"Special character check results for column {column_name} is obtained")
        if not result_Series.all():
            return False, "Special characters found"
        return True, "No special characters found"
    