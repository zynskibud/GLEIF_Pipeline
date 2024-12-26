import psycopg2
import os
import sys
from datetime import datetime, timezone
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(target_directory)

from Production.Update import GLEIF_Update_Helpers
from Production.Update import GLIEF_Update_level_1
from Production.Backfill import GLEIF_Backfill_Helpers
from Production.Backfill import GLEIF_Backfill_Level_1

class CronJobLevel1:
    def __init__(self , str_db_name = "GLEIF_test_db"):
        self.conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
        self.cursor = self.conn.cursor()
        self.obj_gleif_update_helpers = GLEIF_Update_Helpers.GLEIF_Update_Helpers()
        self.obj_backfill_helpers = GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers
        #self.obj_backfill_level_1 = GLEIF_Backfill_Level_1.GLEIFLevel1Data()
        #self.obj_update_level_1 = GLIEF_Update_level_1.GLEIFUpdateLevel1()
        
    def get_file_recordings(self , str_table_name = "gleif_files_processed"):
        
        sql = f"""
            SELECT DISTINCT ON (data_title)
                time_interval, date, file_name, data_title
            FROM {str_table_name}
            WHERE data_title IN ('Level_1_meta_data')
            ORDER BY data_title, date DESC, time_interval DESC
        """
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()  # up to 2 rows here (one for each data_title)
        
        # Separate them into two lists
        level_1_list = []
        
        for row in rows:
            level_1_list.append(row)
        
        return level_1_list
    
    def obtain_necessary_date_elements(self , level_1_list):
        #level_1_list = self.get_file_recordings()
        tup_most_recent_entry = level_1_list[-1]
        str_time_interval = tup_most_recent_entry[0]
        str_parsed_time_interval = str_time_interval[:2] + str_time_interval[3:5]
        str_date_processed = (tup_most_recent_entry[1]).replace("-", "/")
        str_date_processed_naive_dt = datetime.strptime(str_date_processed, "%Y/%m/%d")
        str_date_processed_dt = str_date_processed_naive_dt.replace(tzinfo=timezone.utc)
        
        str_current_date = datetime.now(timezone.utc)
        #str_formatted_current_date = str_current_date.strftime("%m/%d/%Y")
        str_current_interval = self.obj_gleif_update_helpers.get_current_interval()
        
        return str_parsed_time_interval , str_date_processed_dt , str_current_date , str_current_interval
    
    def analyze_for_error(self , level_1_list):
        # This function will be used to analyze the level 1 data for errors in cron job scheduling
        
        str_parsed_time_interval , str_date_processed_dt , str_current_date , str_current_interval = self.obtain_necessary_date_elements(level_1_list = level_1_list)
                
        date_dif = str_current_date - str_date_processed_dt
        date_dif_days = date_dif.days
        
        if date_dif_days > 1:
            return -1
        
        if str_current_interval != 0000:
            str_predicted_last_interval = str_current_interval - 800
            if str_parsed_time_interval != str_predicted_last_interval:
                return -1
            else:
                return 0
        else:
            if str_parsed_time_interval != 1600:
                return -1
            else:
                return 0
        
    def cron_job_level_1(self):
        level_1_list = self.get_file_recordings()
        int_test_result = self.analyze_for_error(level_1_list = level_1_list)
        
        if int_test_result != -1:
            obj_update_level_1 = GLIEF_Update_level_1.GLEIFUpdateLevel1(bool_downloaded = False)
            obj_update_level_1.storing_GLEIF_data_in_database()
        else:
            obj_backfill_level_1 = GLEIF_Backfill_Level_1.GLEIFLevel1Data(bool_downloaded = False)
            obj_backfill_level_1.drop_table(lst_table_name = ["GLEIF_entity_data" , "GLEIF_other_legal_names" , "GLEIF_LegalAddress" , "GLEIF_HeadquartersAddress" , "GLEIF_LegalEntityEvents" , "GLEIF_registration_data" , "GLEIF_geocoding"])
            obj_backfill_level_1.storing_GLEIF_data_in_database()

if __name__ == "main":
    obj_cron_job = CronJobLevel1()
    obj_cron_job.cron_job_level_1()