import os
import logging
import json
import json
import psycopg2
import sys
import io
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, "..", ".."))
sys.path.append(target_directory)

from Production.Update import GLEIF_Update_Helpers
from Production.Backfill import GLEIF_Backfill_Helpers

class GLEIFUpdateExceptions:
    def __init__(self , bool_log = True , str_db_name = "GLEIF_test_db" , bool_downloaded = True):
        self.obj_update_helpers = GLEIF_Update_Helpers.GLEIF_Update_Helpers(bool_Level_2_Reporting_Exceptions = True)
        self.obj_backfill_helpers = GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers(bool_Level_2_Reporting_Exceptions = True)
        if bool_log:
            logging_folder = "../logging"  # Adjust the folder path as necessary
    
            if os.path.exists(logging_folder):
                if not os.path.isdir(logging_folder):
                    raise FileExistsError(f"'{logging_folder}' exists but is not a directory. Please remove or rename the file.")
            else:
                os.makedirs(logging_folder)
    
            logging.basicConfig(filename=f"{logging_folder}/GLEIF_Update_level_2.log", level=logging.DEBUG, format='%(levelname)s: %(message)s', filemode="w")

        if not bool_downloaded:
            if not os.path.exists("../file_lib"):
                os.makedirs("../file_lib")
                
            self.obj_update_helpers.download_on_machine()
            self.str_json_file_path = self.obj_update_helpers.unpacking_GLEIF_zip_files()
    
        self.str_json_file_path = '../file_lib/Exceptions_update_unpacked\\20241130-0000-gleif-goldencopy-repex-intra-day.json'
        self.conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        
    def bulk_insert_using_copy(self , table_name , columns, data):
        """Perform a bulk insert using PostgreSQL COPY with an in-memory buffer

        Args:
            table_name (_type_): Name of the table to insert into
            columns (_type_): List of column names for the table
            data (_type_): List of tuples with the data to be inserted
        """
        
        buffer = io.StringIO()
        
        #write data to the buffer
        
        for row in data:
            buffer.write('\t'.join(map(str , row)) + "\n")
        buffer.seek(0) #reset buffer position to the beginning
        
        #Construct the copy query
        copy_query = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH DELIMITER '\t'"
        self.cursor.copy_expert(copy_query , buffer)
        self.conn.commit
        
    def process_data(self , dict_leis):
        list_tuples_exceptions = []
    
        for dict in dict_leis:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_input = dict)
            tuple_values = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat , subset_string = True , target_keys = ["LEI" , "ExceptionCategory" , "ExceptionReason"])
            list_tuples_exceptions.append(tuple(tuple_values))
            
        self.bulk_insert_using_copy(data = list_tuples_exceptions , table_name = "GLEIF_exception_data" , columns = ['lei' , 'ExceptionCategory' , 'ExceptionReason'])
        
    def storing_GLEIF_data_in_database(self):
        
        with open(self.str_json_file_path, 'r', encoding='utf-8') as file:
            dict_leis = json.load(file)
            
        self.process_data(dict_leis = dict_leis["exceptions"])
        
        
        self.conn.close()

if __name__ == "main":
    obj_gleif_reporting_exceptions = GLEIFUpdateExceptions()
    obj_gleif_reporting_exceptions.storing_GLEIF_data_in_database()
