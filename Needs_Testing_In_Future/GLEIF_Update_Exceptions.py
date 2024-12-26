import os
import logging
import json
import json
import psycopg2
import sys
import io
from datetime import datetime, timezone
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
        else:
            current_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            current_interval = self.obj_update_helpers.get_current_interval()
            self.str_json_file_path = f'../file_lib/Exceptions_update_unpacked\\{current_date_str}-{current_interval}-gleif-goldencopy-repex-intra-day.json'
        self.conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
        self.cursor = self.conn.cursor()
        
    def bulk_insert_using_copy(self, table_name, columns, data):
        """
        Perform a bulk upsert using PostgreSQL COPY with a temporary table.
        Args:
            table_name (str): Name of the table to insert into.
            columns (list): List of column names for the table.
            data (list of tuples): Data to be inserted.
        """
        temp_table = f"{table_name}_temp"

        # Step 1: Create a temporary table
        self.cursor.execute(f"""
            CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING ALL) ON COMMIT DROP;
        """)

        # Step 2: Write data to an in-memory buffer
        buffer = io.StringIO()
        for row in data:
            buffer.write('\t'.join(map(str, row)) + "\n")
        buffer.seek(0)  # Reset buffer position to the beginning

        # Step 3: Copy data into the temporary table
        copy_query = f"COPY {temp_table} ({', '.join(columns)}) FROM STDIN WITH DELIMITER '\t'"
        self.cursor.copy_expert(copy_query, buffer)

        # Step 4: Perform the upsert from the temporary table into the main table
        upsert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM {temp_table}
            ON CONFLICT (lei, ExceptionCategory, ExceptionReason) DO NOTHING;
        """
        self.cursor.execute(upsert_query)

        # Commit the transaction
        self.conn.commit()
    
    def remove_duplicates_keep_order(self , input_list):
        seen = set()
        output_list = []
        for item in input_list:
            if item not in seen:
                output_list.append(item)
                seen.add(item)
        return output_list
    
    def process_data(self , dict_leis):
        list_tuples_exceptions = []
    
        for dict in dict_leis:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_input = dict)
            tuple_values = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat , subset_string = True , target_keys = ["LEI" , "ExceptionCategory" , "ExceptionReason"])
            list_tuples_exceptions.append(tuple(tuple_values))
            
        list_clean_tuples_exceptions = self.remove_duplicates_keep_order(list_tuples_exceptions)
        self.bulk_insert_using_copy(data=list_clean_tuples_exceptions , table_name = "GLEIF_exception_data" , columns = ['lei', 'ExceptionCategory', 'ExceptionReason'])
        
    def storing_GLEIF_data_in_database(self):
        
        with open(self.str_json_file_path, 'r', encoding='utf-8') as file:
            dict_leis = json.load(file)
            
        self.process_data(dict_leis = dict_leis["exceptions"])
        
        self.obj_backfill_helpers.file_tracker(file_path = self.str_json_file_path , str_db_name = "GLEIF_test_db" , str_data_title = "Level_2_exceptions")

        
        self.conn.commit()
        self.conn.close()
        
        

if __name__ == "main":
    obj_gleif_reporting_exceptions = GLEIFUpdateExceptions()
    obj_gleif_reporting_exceptions.storing_GLEIF_data_in_database()
