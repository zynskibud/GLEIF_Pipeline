import os
import psycopg2
import sys
import concurrent.futures
import time
from tqdm import tqdm
from datetime import datetime, timezone
import math
from collections import defaultdict
import requests_tor
import pickle
import requests
import pytest
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, "..", ".."))
sys.path.append(target_directory)

from Production.Backfill import GLEIF_Backfill_Helpers , GLEIF_Backfill_Relationships

class Testing_Level_2_Data:
    def __init__(self):
        """self.rt = requests_tor.RequestsTor(tor_ports=(9000, 9001, 9002, 9003, 9004), tor_cport=9151, password=None,
                 autochange_id=5, threads=8)"""
        self.rt = requests_tor.RequestsTor(
                    tor_ports=(9050, 9052, 9053, 9054, 9055),
                    tor_cport=9051,
                    autochange_id=1  # Change identity after each request
                )
        self.obj_backfill_helpers = GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers()
        self.obj_backfill_relationships = GLEIF_Backfill_Relationships.GLEIFLevel2Data(bool_downloaded = False) 
    
    def get_dict_map(self , list_input):
        dict_db_data = defaultdict(list)

        for item in list_input:
            dict_db_data[item[0]].append(item)
    # Convert defaultdict to a regular dictionary (optional)
        dict_db_data = dict(dict_db_data)

        return dict_db_data 
    
    def unify_date(self , date_str):
        """
        If there's a 'T', parse the date-time, normalize to UTC, and return 'YYYY-MM-DD'.
        Otherwise, assume it's already just 'YYYY-MM-DD' and return it as-is.
        """
        if date_str is None:
            return None

        # If there's no 'T', skip time-zone parsing entirely
        if 'T' not in date_str:
            return date_str  # e.g. "1969-04-17"

        try:
            # Replace 'Z' with '+00:00' so Python recognizes the time zone
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            dt_utc = dt.astimezone(timezone.utc)
            # Return just the date portion (YYYY-MM-DD)
            return dt_utc.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"Error normalizing date '{date_str}': {e}")
            return date_str  # Fallback: return original string
        
    def clean_date_string(self , list_input , date_indexes , bool_many = False):
        if bool_many == True:
            for sublist in list_input:
                #display(sublist)
                for idx in date_indexes:
                        sublist[idx] = (self.unify_date(sublist[idx]))
        else:
            for idx in date_indexes:
                list_input[idx] = (self.unify_date(list_input[idx]))
            
        return list_input
    
    def norm_none(self , list_db_rel_date_clean):
        #display(list_db_rel_date_clean)
        for sublist in list_db_rel_date_clean:
            #display(sublist)
            sublist[:] = [None if val == 'None' else val for val in sublist]
            
        return list_db_rel_date_clean
    
    def get_all_lei_data(self , str_table_name):
        conn = psycopg2.connect(
                dbname="GLEIF_test_db",
                user="Matthew_Pisinski",
                password="matt1",
                host="localhost",
                port="5432"
            )
        cursor = conn.cursor()

        # Define the SQL query to fetch all rows, ordered by 'lei'
        query = f"SELECT * FROM {str_table_name};"
        cursor.execute(query)

        # Retrieve all results
        results = cursor.fetchall()

        # Convert each row to a list and exclude the first column (e.g., an ID or primary key)
        all_data = [list(row)[1:] for row in results]
            
        cursor.close()
        conn.close()
        
        return all_data
    
    def get_ultimate_direct_keys(self , dict_level_2_data):
        list_direct_keys = []
        list_ultimate_keys = []

        for key, value in dict_level_2_data.items():
            if any(item[2] == "IS_DIRECTLY_CONSOLIDATED_BY" for item in value):
                list_direct_keys.append(key)
            if any(item[2] == "IS_ULTIMATELY_CONSOLIDATED_BY" for item in value):
                list_ultimate_keys.append(key)
                
        return list_direct_keys , list_ultimate_keys
    
    def fetch_relationship_ultimate(self , rt , key):
        """
        Fetch the ultimate-parent-relationship for a given LEI key.
        Returns a tuple of (key, status_code).
        """
        dict_return = {}
        
        url = f"https://api.gleif.org/api/v1/lei-records/{key}/ultimate-parent-relationship"
        
        try: 
            response = self.rt.get(url = url)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ProxyError):
            time.sleep(45)
            response = self.rt.get(url = url)
        
        if response.status_code == 200:
            json = response.json()
            dict_data = json["data"]
            dict_return[key] = dict_data
            return dict_return
        elif response.status_code == 429:
            time.sleep(60)
            dict_data = self.fetch_relationship_ultimate(rt , key)
            return dict_data
        elif response.status_code == 404:
            dict_return[key] = None
            return dict_return
    
    def fetch_relationship_direct(self , rt , key):
        """
        Fetch the ultimate-parent-relationship for a given LEI key.
        Returns a tuple of (key, status_code).
        """
        dict_return = {}
        
        url = f"https://api.gleif.org/api/v1/lei-records/{key}/direct-parent-relationship"
        
        try: 
            response = self.rt.get(url = url)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ProxyError):
            time.sleep(45)
            response = self.rt.get(url = url)        
        
        if response.status_code == 200:
            #display("ye")
            json = response.json()
            dict_data = json["data"]
            dict_return[key] = dict_data
            return dict_return
        elif response.status_code == 429:
            display("ye")
            time.sleep(60)
            dict_return = self.fetch_relationship_direct(rt , key)
            return dict_return
        elif response.status_code == 404:
            dict_return[key] = None
            return dict_return
        else:
            display(response.status_code)
            dict_return[key] = None
            return dict_return
        
    def task_ultimate(self , key):
            return self.fetch_relationship_ultimate(rt = self.rt, key = key)
        
    def task_direct(self , key):
            return self.fetch_relationship_direct(rt = self.rt, key = key)
    
    def process_keys(self , keys, max_workers=10 , direct = False , ultimate = False):
        """
        Process a list of keys using a ThreadPoolExecutor.
        Returns an iterator of results (key, status_code).
        """

        if ultimate:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Map returns an iterator of results as they complete
                results = executor.map(self.task_ultimate, keys)
            
                list_ultimate_relationship_api_dicts = {}
                for result in results:
                    # Each result is a dictionary; update the combined_results
                    list_ultimate_relationship_api_dicts.update(result)
                return list_ultimate_relationship_api_dicts
                
        elif direct:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Map returns an iterator of results as they complete
                results = executor.map(self.task_direct, keys)
                
                list_direct_relationship_api_dicts = {}
                for result in results:
                    # Each result is a dictionary; update the combined_results
                    list_direct_relationship_api_dicts.update(result)
                return list_direct_relationship_api_dicts
            
    
    def testing_relationship_data(self , dict_api_direct_relationships , dict_api_ultimate_relationships , ultimate = False , direct = False):
        list_db_relationship_data = self.get_all_lei_data(str_table_name = "gleif_relationship_data")
        dict_db_data = self.get_dict_map(list_input = list_db_relationship_data)
        
        if ultimate:
            dict_api_test = dict_api_ultimate_relationships
        if direct:
            dict_api_test = dict_api_direct_relationships

        
        for key in dict_api_test:
            try:
                list_db_rel_partial = dict_db_data[key]

                if ultimate:
                    has_ultimate_rel = any(row[2] == "IS_ULTIMATELY_CONSOLIDATED_BY" for row in list_db_rel_partial)
                    # If there is NO row with "IS_ULTIMATELY_CONSOLIDATED_BY", skip rest of loop
                    if not has_ultimate_rel:
                        continue

                if direct:
                    has_direct_rel = any(row[2] == "IS_DIRECTLY_CONSOLIDATED_BY" for row in list_db_rel_partial)
                    # If there is NO row with "IS_DIRECTLY_CONSOLIDATED_BY", skip rest of loop
                    if not has_direct_rel:
                        continue
                
                dict_api_data = dict_api_test[key]
                dict_flat_api_data = self.obj_backfill_helpers.flatten_dict(dict_input = dict_api_data)
                list_api_data = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat_api_data, target_keys = ["attributes_relationship_startNode_id" , "attributes_relationship_endNode_id" , "attributes_relationship_type" , "attributes_relationship_status" , "attributes_registration_status" , "attributes_registration_initialRegistrationDate" , "attributes_registration_lastUpdateDate" , "attributes_registration_nextRenewalDate" , "attributes_registration_managingLou" , "attributes_registration_corroborationLevel" , "attributes_registration_corroborationDocuments" , "attributes_registration_corroborationReference"])
                list_clean_api_data = self.obj_backfill_relationships.clean_url(list_input = list_api_data)
                
                lst_indexes = [5,6,7]
                
                
                list_db_rel_date_clean = self.clean_date_string(bool_many = True , date_indexes = lst_indexes , list_input = list_db_rel_partial)
                list_db_rel_date_clean = self.norm_none(list_db_rel_date_clean = list_db_rel_date_clean)
                
                list_date_clean_api_data = self.clean_date_string(bool_many = False , date_indexes = lst_indexes , list_input = list_clean_api_data)
                if list_date_clean_api_data not in list_db_rel_date_clean:
                    
                    print("The single list is NOT present in the list of lists.")
                    display(list_db_rel_date_clean)
                    display(list_date_clean_api_data)
            
            except KeyError:
                pass
            
    def testing_relationship_date_data(self , dict_api_direct_relationships , dict_api_ultimate_relationships , ultimate = False , direct = True):
        list_db_relationship_data = self.get_all_lei_data(str_table_name = "gleif_relationship_date_data")
        dict_db_data = self.get_dict_map(list_input = list_db_relationship_data)
            
        if ultimate:
            dict_api_test = dict_api_ultimate_relationships
        elif direct:
            dict_api_test = dict_api_direct_relationships
        #display(dict_api_test)
        
        for key in dict_api_test:
            try:
                list_db_rel_partial = dict_db_data[key]
                #display(f"1 .{key}")
                #display(f"2. {list_db_rel_partial}")
                
                
                if ultimate:
                    has_ultimate_rel = any(row[2] == "IS_ULTIMATELY_CONSOLIDATED_BY" for row in list_db_rel_partial)
                    # If there is NO row with "IS_ULTIMATELY_CONSOLIDATED_BY", skip rest of loop
                    if not has_ultimate_rel:
                        continue

                if direct:
                    has_direct_rel = any(row[2] == "IS_DIRECTLY_CONSOLIDATED_BY" for row in list_db_rel_partial)
                    # If there is NO row with "IS_DIRECTLY_CONSOLIDATED_BY", skip rest of loop
                    if not has_direct_rel:
                        continue
                
                dict_api_data = dict_api_test[key]
                
                #display(f"3. {dict_api_data}")
                dict_flat_api_data = self.obj_backfill_helpers.flatten_dict(dict_input = dict_api_data)    
                list_api_dates = self.obj_backfill_helpers.extract_event_data(bool_test = True , dict_data = dict_flat_api_data , base_keyword = "attributes_relationship_periods" , target_keys = ["startDate" , "endDate" , "type"])
                list_unique_keys = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat_api_data, target_keys = ["attributes_relationship_startNode_id" , "attributes_relationship_endNode_id" , "attributes_relationship_type"])
                list_api_dates_with_keys = [[*list_unique_keys, *list] for list in list_api_dates]
                #display(f"4. {list_api_dates_with_keys}")
                    
                list_api_dates_with_keys_clean = self.clean_date_string(bool_many = True , date_indexes = [3, 4] , list_input = list_api_dates_with_keys)
                list_db_dates_with_keys_clean = self.clean_date_string(bool_many = True , date_indexes = [3, 4] , list_input = list_db_rel_partial)
                #display(list_api_dates_with_keys_clean)
                
                list_db_dates_none_norm = self.norm_none(list_db_rel_date_clean = list_db_dates_with_keys_clean)
                
                for list_api_date in list_api_dates_with_keys_clean:
                    
                    if list_api_date not in list_db_dates_none_norm:
                        print("Fail")
                        display(f"5. {list_db_dates_with_keys_clean}")
                        display(f"6. {list_api_date}")
                        
            except KeyError:
                    pass
    
    def test_level_2_data(self):
        list_level_2 = self.get_all_lei_data(str_table_name = "gleif_relationship_data")
        dict_return = self.get_dict_map(list_input = list_level_2)
        list_direct_keys , list_ultimate_keys = self.get_ultimate_direct_keys(dict_return)
        dict_api_ultimate_relationships = self.process_keys(keys = list_ultimate_keys, max_workers = 100 , ultimate = True)
        dict_api_direct_relationships = self.process_keys(keys = list_direct_keys, max_workers = 75 , direct = True)
        self.obj_backfill_relationships.storing_GLEIF_data_in_database()
        self.testing_relationship_date_data(dict_api_direct_relationships = dict_api_direct_relationships, dict_api_ultimate_relationships = dict_api_ultimate_relationships , ultimate = True)