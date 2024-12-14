import os
import logging
import json
import json
import psycopg2
import GLEIF_Backfill_Helpers
import io

class GLEIF_Reporting_Exceptions_Data:
    def __init__(self , bool_log = True , str_db_name = "GLEIF_test_db" , bool_downloaded = True):
        
        self.obj_backfill_helpers = GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers(bool_Level_2_Reporting_Exceptions = True)

        if bool_log:
            logging_folder = "../logging"  # Adjust the folder path as necessary
    
            if os.path.exists(logging_folder):
                if not os.path.isdir(logging_folder):
                    raise FileExistsError(f"'{logging_folder}' exists but is not a directory. Please remove or rename the file.")
            else:
                os.makedirs(logging_folder)
    
            logging.basicConfig(filename=f"{logging_folder}/GLEIF_Backfill_level_2_exceptions.log", level=logging.DEBUG, format='%(levelname)s: %(message)s', filemode="w")

        if not bool_downloaded:
            if not os.path.exists("../file_lib"):
                os.makedirs("../file_lib")
                
            str_level_2_exceptions_download_link = self.obj_backfill_helpers.get_level_download_links()
            self.str_json_file_path = self.obj_backfill_helpers.unpacking_GLEIF_zip_files(str_download_link = str_level_2_exceptions_download_link , str_unpacked_zip_file_path_name = "Level_2_unpacked_exceptions" , str_zip_file_path_name = "Level_2_exceptions.zip")
    
        str_unpacked_zip_file_name = os.listdir(rf"../file_lib/Level_2_unpacked_exceptions")[0]
        self.str_json_file_path = rf"../file_lib/Level_2_unpacked_exceptions" + "//" + str_unpacked_zip_file_name
        self.conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        
    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLEIF_exception_data (
            id SERIAL PRIMARY KEY,
            lei TEXT NOT NULL,
            ExceptionCategory TEXT,
            ExceptionReason Text
            );
        """)
    
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
        self.create_table()
        
        with open(self.str_json_file_path, 'r', encoding='utf-8') as file:
            dict_leis = json.load(file)
            
        self.process_data(dict_leis = dict_leis["exceptions"])
        
        
        self.conn.close()
    

if __name__ == "main":
    obj = GLEIF_Reporting_Exceptions_Data()
    obj.storing_GLEIF_data_in_database()
    