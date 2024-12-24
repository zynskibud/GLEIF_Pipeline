from bs4 import BeautifulSoup
import os
import requests
import re
import psycopg2
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import zipfile

class GLEIF_Backill_Helpers:
    def __init__(self, bool_Level_1 = False, bool_Level_2_Trees = False, bool_Level_2_Reporting_Exceptions = False):
        self.bool_Level_1 = bool_Level_1
        self.bool_Level_2_Trees = bool_Level_2_Trees
        self.bool_Level_2_Reporting_Exceptions = bool_Level_2_Reporting_Exceptions
        

    def get_level_download_links(self):
        """
        This function uses selenium to webscrape the download link for all Level 1 Data in the GLEIF database.
        
        @return: str_download_link - the link which is used to download the entire GLEIF level 1
        """

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)
        driver.get(url = "https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/")

        cookie_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, 'CybotCookiebotDialogBodyButton'))
        )

        cookie_button.click()

        download_buttons = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'gc-download-button'))
        )
        
        if self.bool_Level_1 == True:
            download_buttons[0].click()
        if self.bool_Level_2_Trees == True:
            download_buttons[1].click()
        if self.bool_Level_2_Reporting_Exceptions == True:
            download_buttons[2].click()
        
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')

        driver.close()

        str_download_link = ((soup.find_all("a" , class_ = "gc-icon gc-icon--json"))[0])["href"]
        
        return str_download_link        
    
    def unpacking_GLEIF_zip_files(self , str_download_link , str_zip_file_path_name , str_unpacked_zip_file_path_name):
        session = requests.Session()
        zip_file = session.get(url = str_download_link)

        with open(rf"../file_lib/{str_zip_file_path_name}", 'wb') as f:
            f.write(zip_file.content)

        with zipfile.ZipFile(rf"../file_lib/{str_zip_file_path_name}", 'r') as zip_ref:
            os.makedirs(rf"../file_lib/{str_unpacked_zip_file_path_name}", exist_ok=True)
            zip_ref.extractall(rf"../file_lib/{str_unpacked_zip_file_path_name}")
        
        str_unpacked_zip_file_name = os.listdir(rf"../file_lib/{str_unpacked_zip_file_path_name}")[-1]
        str_json_file_path = rf"../file_lib/{str_unpacked_zip_file_path_name}" + "//" + str_unpacked_zip_file_name
        
        return str_json_file_path
    
    def flatten_dict(self , dict_input):
        dict_flattened = {}

        def flatten(current_element, parent_key=''):
            if isinstance(current_element, dict):
                for key, value in current_element.items():
                    new_key = f"{parent_key}_{key}" if parent_key else key
                    flatten(value, new_key)
            elif isinstance(current_element, list):
                for index, item in enumerate(current_element, start=1):
                    indexed_key = f"{parent_key}_{index}"
                    flatten(item, indexed_key)
            else:
                dict_flattened[parent_key] = current_element

        flatten(dict_input)
        return dict_flattened
    
    def clean_keys(self , input_dict):
        cleaned_dict = {}
        
        for key, value in input_dict.items():
            if not '@xml:lang' in key:
                if key.endswith('_$'):
                    new_key = key[:-2]  # Remove the last 2 characters ('_$')
                else:
                    new_key = key  # Keep the key as is
                
                cleaned_dict[new_key] = value
        
        return cleaned_dict
    
    def organize_by_prefix(self , input_dict):
        dict_organized = {}
        
        for key, value in input_dict.items():
            prefix, _, sub_key = key.partition('_')
            
            if prefix not in dict_organized:
                dict_organized[prefix] = {}
            
            # Add the key-value pair to the corresponding sub-dictionary
            dict_organized[prefix][sub_key] = value
        
        return dict_organized

    def extract_both_entity_names(self , data_dict):
        """
        Extracts and organizes both `OtherEntityNames` and `TransliteratedOtherEntityNames` keys.
        Returns two lists of tuples:
        - First list for OtherEntityNames: (type, value)
        - Second list for TransliteratedOtherEntityNames: (type, value)
        """
        other_grouped_data = {}
        transliterated_grouped_data = {}

        for key, value in data_dict.items():
            if "OtherEntityNames" in key and "TransliteratedOtherEntityNames" not in key:
                # This is a regular OtherEntityNames key
                match = re.search(r"_(\d+)", key)
                if not match:
                    continue
                index = int(match.group(1))

                if index not in other_grouped_data:
                    other_grouped_data[index] = {"type": None, "value": None}

                if "@type" in key:
                    other_grouped_data[index]["type"] = value
                else:
                    other_grouped_data[index]["value"] = value

            elif "TransliteratedOtherEntityNames" in key:
                # This is a TransliteratedOtherEntityNames key
                match = re.search(r"_(\d+)", key)
                if not match:
                    continue
                index = int(match.group(1))

                if index not in transliterated_grouped_data:
                    transliterated_grouped_data[index] = {"type": None, "value": None}

                if "@type" in key:
                    transliterated_grouped_data[index]["type"] = value
                else:
                    transliterated_grouped_data[index]["value"] = value

        # Convert grouped data into lists of tuples
        other_result = [
            (other_grouped_data[idx]["type"], other_grouped_data[idx]["value"])
            for idx in sorted(other_grouped_data.keys())
        ]

        transliterated_result = [
            (transliterated_grouped_data[idx]["type"], transliterated_grouped_data[idx]["value"])
            for idx in sorted(transliterated_grouped_data.keys())
        ]

        return other_result, transliterated_result

    """def extract_other_entity_names(self , data_dict, base_keyword, exclude_keywords=None):
        grouped_data = {}

        # Default empty list for exclude_keywords
        if exclude_keywords is None:
            exclude_keywords = []

        # Group keys by numeric suffix
        for key, value in data_dict.items():
            # Skip keys with excluded keywords
            if any(exclude in key for exclude in exclude_keywords):
                continue

            if base_keyword in key:
                # Extract the numeric suffix using regex
                match = re.search(r"_(\d+)", key)
                if not match:
                    continue  # Skip keys without a numeric suffix
                index = int(match.group(1))

                if index not in grouped_data:
                    grouped_data[index] = {"type": None, "value": None}

                # Check if this key is `@type` or the main value
                if "@type" in key:
                    grouped_data[index]["type"] = value
                else:
                    grouped_data[index]["value"] = value

        # Convert grouped data into a list of tuples
        result = [(grouped_data[index]["type"], grouped_data[index]["value"]) for index in sorted(grouped_data.keys())]

        return result"""

    def extract_event_data(self , dict_data, base_keyword, target_keys , bool_test = False):
        """
        Extracts and organizes data for repeated keys in a dictionary based on a base keyword and target keys.

        :param dict_data: Dictionary containing the raw data.
        :param base_keyword: Common substring to identify relevant keys (e.g., "LegalEntityEvents").
        :param target_keys: List of substrings to match keys that should be included in the tuple.
        :return: A list of tuples, one for each numeric suffix group, containing values for the target keys.
        """
        grouped_data = {}


        # Group keys by numeric suffix
        for key, value in dict_data.items():
            if base_keyword in key:
                # Extract the numeric suffix using regex
                match = re.search(r"_(\d+)_", key)
                if not match:
                    continue  # Skip keys without a numeric suffix
                index = int(match.group(1))

                # Extract the part of the key after the numeric suffix
                key_suffix = key.split(f"_{index}_")[-1]

                if index not in grouped_data:
                    grouped_data[index] = {}
                
                # Check if this key matches any of the target keys as a substring
                for target in target_keys:
                    if target in key_suffix:
                        grouped_data[index][target] = value
                        break

        # Create tuples for each group of keys
        result = []
        for index in sorted(grouped_data.keys()):
            # Create a tuple of values for the target keys, using None if a key is missing
            tuple_values = tuple(grouped_data[index].get(target, None) for target in target_keys)
            result.append(tuple_values)

        if bool_test == True:
            result = [list(tup) for tup in result]
        
        return result

    def get_target_values(self , dict_data, target_keys, subset_string=False):
        """
        Retrieves values for a set of target keys from a dictionary.
        If a key is not present, it returns None for that key.
        Optionally allows searching for subsets of key strings.

        :param dict_data: Dictionary containing the raw data.
        :param target_keys: List of keys (or substrings) to retrieve values for.
        :param subset_string: Boolean indicating whether to search for subsets of key strings.
        :return: A tuple of values corresponding to the target keys (or None if a key is missing).
        """
        result = []
        for target in target_keys:
            if subset_string:
                # Search for a key containing the target substring
                found_key = next((key for key in dict_data if target in key), None)
                result.append(dict_data.get(found_key, None))
            else:
                # Direct key lookup
                result.append(dict_data.get(target, None))
        return list(result)

    def split_into_list_of_dictionaries(self , dict_data):
        """
        Splits a dictionary into a list of dictionaries, grouped by numeric suffixes.
        Keys without numeric suffixes are excluded.

        :param data_dict: Dictionary containing the keys and values.
        :return: A list of dictionaries, one for each numeric suffix.
        """
        grouped_dicts = {}

        # Iterate through the dictionary and group by numeric suffix
        for key, value in dict_data.items():
            match = re.search(r"_(\d+)_", key)
            if match:
                # Extract the numeric suffix
                group_number = int(match.group(1))
                if group_number not in grouped_dicts:
                    grouped_dicts[group_number] = {}
                grouped_dicts[group_number][key] = value

        # Convert the grouped dictionaries to a list
        return [grouped_dicts[group] for group in sorted(grouped_dicts.keys())]


    def further_flatten_geocoding(self , dict_data):
        """
        Flattens a dictionary by extracting bounding box values and adding them as separate keys.
        
        :param data_dict: Dictionary containing geocoding data with bounding box values.
        :return: A new dictionary with flattened bounding box values.
        """
        flattened_dict = {}

        for key, value in dict_data.items():
            # Check for bounding box keys and split their values
            if "bounding_box" in key:
                # Extract numeric suffix if present
                match = re.search(r"_(\d+)_", key)
                group_number = f"_{match.group(1)}_" if match else "_"

                # Parse the bounding box values
                bounding_box_values = value.split(", ")
                for box_value in bounding_box_values:
                    # Split key-value pair (e.g., "TopLeft.Latitude: 39.7496542")
                    box_key, box_val = box_value.split(": ")
                    # Create a new key with group number
                    new_key = f"{key.split('bounding_box')[0]}{box_key.strip()}{group_number}".strip("_")
                    flattened_dict[new_key] = box_val.strip()
            else:
                # If not bounding box, keep the original key-value pair
                flattened_dict[key] = value

        return flattened_dict
    
    def parse_file_path(self , file_path):
        str_filename = os.path.basename(file_path)
        date_part, time_part = str_filename.split('-')[:2]
        str_formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:]}"
        str_formatted_time = f"{time_part[:2]}:{time_part[2:]} UTC"
        return str_filename, str_formatted_date, str_formatted_time
    
    def create_file_tracker_table(self, conn, cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLEIF_files_processed (
                id SERIAL PRIMARY KEY,
                time_interval TEXT,
                date TEXT,
                file_name TEXT,
                Data_Title TEXT
            );
        """)
        conn.commit()
    
    def insert_file_into_file_tracker(self, conn, cursor, filename, formatted_date, formatted_time, str_data_title):
        insert_query = """
            INSERT INTO GLEIF_files_processed (file_name, date, time_interval, Data_Title)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (filename, formatted_date , formatted_time, str_data_title))
        conn.commit()
    
    def file_tracker(self , str_db_name , file_path , str_data_title):
        conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
        cursor = conn.cursor()

        self.create_file_tracker_table(conn = conn , cursor = cursor)
        
        
        
        str_filename, str_formatted_date, str_formatted_time = self.parse_file_path(file_path)
        
        self.insert_file_into_file_tracker(conn = conn , cursor = cursor , filename = str_filename , formatted_date = str_formatted_date , formatted_time = str_formatted_time , str_data_title = str_data_title)