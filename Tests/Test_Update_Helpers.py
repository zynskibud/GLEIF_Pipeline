import pytest
import sys
import os
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, "..", ".."))
sys.path.append(target_directory)
from datetime import datetime, timezone

from Production.Update import GLEIF_Update_Helpers

class TestingUpdateHelpers:
    
    @pytest.fixture
    def obj_update_helpers_level_1(self):
        return GLEIF_Update_Helpers.GLEIF_Update_Helpers(bool_Level_1 = True)
    
    @pytest.fixture
    def obj_update_helpers_relationships(self):
        return GLEIF_Update_Helpers.GLEIF_Update_Helpers(bool_Level_2_Trees = True)
    
    @pytest.fixture
    def obj_update_helpers_exceptions(self):
        return GLEIF_Update_Helpers.GLEIF_Update_Helpers(bool_Level_2_Reporting_Exceptions = True)
    
    def helper_test_download_and_search_level_1(self , obj_update_helpers_level_1):
        obj_update_helpers_level_1.download_on_machine()
        list_files = obj_update_helpers_level_1.find_default_download_folder_and_list_files()
        
        current_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        current_interval = obj_update_helpers_level_1.get_current_interval()
        str_filename = f"{current_date_str}-{current_interval}-gleif-goldencopy-lei2-intra-day.json.zip"
        
        str_zip_file_path = [file for file in list_files if str_filename in os.path.basename(file)][0]

        if not str_zip_file_path:
            assert False
        
    def helper_test_download_and_search_relationships(self , obj_update_helpers_relationships):
        obj_update_helpers_relationships.download_on_machine()
        list_files = obj_update_helpers_relationships.find_default_download_folder_and_list_files()
        
        current_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        current_interval = obj_update_helpers_relationships.get_current_interval()
        str_filename = f"{current_date_str}-{current_interval}-gleif-goldencopy-rr-intra-day.json.zip"

        str_zip_file_path = [file for file in list_files if str_filename in os.path.basename(file)][0]

        if not str_zip_file_path:
            assert False
        
    def helper_test_download_and_search_exceptions(self , obj_update_helpers_exceptions):
        obj_update_helpers_exceptions.download_on_machine()
        list_files = obj_update_helpers_exceptions.find_default_download_folder_and_list_files()
        
        current_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        current_interval = obj_update_helpers_exceptions.get_current_interval()
        str_filename = f"{current_date_str}-{current_interval}-gleif-goldencopy-repex-intra-day.json.zip"

        str_zip_file_path = [file for file in list_files if str_filename in os.path.basename(file)][0]

        if not str_zip_file_path:
            assert False
    
    def helper_test_unpacking_zip_file_level_1(self , obj_update_helpers_level_1):
        str_json_file = obj_update_helpers_level_1.unpacking_GLEIF_zip_files()
        current_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        current_interval = obj_update_helpers_level_1.get_current_interval()
        
        if str_json_file != f'../file_lib/Level_1_update_unpacked\\{current_date_str}-{current_interval}-gleif-goldencopy-lei2-intra-day.json':
            assert False
            
    def helper_test_unpacking_zip_file_relationships(self , obj_update_helpers_relationships):
        str_json_file = obj_update_helpers_relationships.unpacking_GLEIF_zip_files()
        current_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        current_interval = obj_update_helpers_relationships.get_current_interval()
        
        if str_json_file != f'../file_lib/Level_2_update_unpacked\\{current_date_str}-{current_interval}-gleif-goldencopy-rr-intra-day.json':
            assert False
    
    def helper_test_unpacking_zip_file_exceptions(self , obj_update_helpers_exceptions):
        str_json_file = obj_update_helpers_exceptions.unpacking_GLEIF_zip_files()
        current_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        current_interval = obj_update_helpers_exceptions.get_current_interval()
        
        if str_json_file != f'../file_lib/Exceptions_update_unpacked\\{current_date_str}-{current_interval}-gleif-goldencopy-repex-intra-day.json':
            assert False
            
    def test_update_helpers(self , obj_update_helpers_level_1 ,obj_update_helpers_relationships , obj_update_helpers_exceptions):
        self.helper_test_download_and_search_level_1(obj_update_helpers_level_1 = obj_update_helpers_level_1)
        self.helper_test_download_and_search_relationships(obj_update_helpers_relationships = obj_update_helpers_relationships)
        self.helper_test_download_and_search_exceptions(obj_update_helpers_exceptions = obj_update_helpers_exceptions)
        self.helper_test_unpacking_zip_file_level_1(obj_update_helpers_level_1 = obj_update_helpers_level_1)
        self.helper_test_unpacking_zip_file_relationships(obj_update_helpers_relationships = obj_update_helpers_relationships)
        self.helper_test_download_and_search_exceptions(obj_update_helpers_exceptions = obj_update_helpers_exceptions)