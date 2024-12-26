import psycopg2
import os
import sys
import Cron_Job_level_1
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, ".."))
sys.path.append(target_directory)

from Production.Backfill import GLEIF_Backfill_Relationships
from Production.Update import GLEIF_Update_Relationships

class CronJobLevel2:
    def __innit__(self , str_db_name = "GLEIF_test_db"):
        self.conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
        self.cursor = self.conn.cursor()
        self.obj_cron_job_1 = Cron_Job_level_1.CronJobLevel1()
    
    def get_file_recordings(self , str_table_name = "gleif_files_processed"):
        
        sql = f"""
            SELECT DISTINCT ON (data_title)
                time_interval, date, file_name, data_title
            FROM {str_table_name}
            WHERE data_title IN ('Level_2_Relationships')
            ORDER BY data_title, date DESC, time_interval DESC
        """
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()  # up to 2 rows here (one for each data_title)
        
        # Separate them into two lists
        level_2_list = []
        
        for row in rows:
            level_2_list.append(row)
        
        return level_2_list
    
    def cron_job_level_2(self):
        level_2_list = self.get_file_recordings()
        int_test_result = self.obj_cron_job_1.analyze_for_error(level_1_list = level_2_list)
        
        if int_test_result != -1:
            obj_update_level_2 = GLEIF_Update_Relationships.GLEIFUpdateLevel2(bool_downloaded = False)
            obj_update_level_2.updating_GLEIF_data_in_database()
        else:
            obj_backfill_level_2 = GLEIF_Backfill_Relationships.GLEIFLevel2Data(bool_downloaded = False)
            obj_backfill_level_2.drop_table(lst_table_name = ["GLEIF_relationship_data" , "GLEIF_relationship_date_data" , "GLEIF_relationship_qualifiers" , "GLEIF_relationship_quantifiers"])
            obj_backfill_level_2.storing_GLEIF_data_in_database()
    
if __name__ == "main":
    obj_cron_job_2 = CronJobLevel2()
    obj_cron_job_2.cron_job_level_2()