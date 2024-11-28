from bs4 import BeautifulSoup
import os
import requests
import re
import logging
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import zipfile
import bigjson

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
        
        str_unpacked_zip_file_name = os.listdir(rf"../file_lib/{str_unpacked_zip_file_path_name}")[0]
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

    def extract_other_entity_names(self , data_dict, base_keyword, exclude_keywords=None):
        """
        Extracts and organizes `OtherEntityNames` keys into a list of tuples.
        Each tuple contains the `@type` and the main value for a given numeric suffix.

        :param data_dict: Dictionary containing the raw data.
        :param base_keyword: Common substring to identify relevant keys (e.g., "OtherEntityNames").
        :param exclude_keywords: List of keywords to exclude (e.g., ["TranslatedOtherEntityNames"]).
        :return: A list of tuples: (type, value) for each numeric suffix group.
        """
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

        return result

    def extract_event_data(self , dict_data, base_keyword, target_keys):
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
    
class GLEIFLevel1Data:
    def __init__(self , bool_log = True , str_db_name = "GLEIF_Data.db" , bool_downloaded = True):
        
        self.obj_backfill_helpers = GLEIF_Backill_Helpers(bool_Level_1 = True)

        if bool_log:
            logging_folder = "../logging"  # Adjust the folder path as necessary
    
            if os.path.exists(logging_folder):
                if not os.path.isdir(logging_folder):
                    raise FileExistsError(f"'{logging_folder}' exists but is not a directory. Please remove or rename the file.")
            else:
                os.makedirs(logging_folder)
    
            logging.basicConfig(filename=f"{logging_folder}/GLEIF_Backfill_level_1.log", level=logging.DEBUG, format='%(levelname)s: %(message)s', filemode="w")

        if not bool_downloaded:
            if not os.path.exists("../file_lib"):
                os.makedirs("../file_lib")
                
            obj_backfill_helpers = GLEIF_Backill_Helpers(bool_Level_1 = True)
            str_level_1_download_link = obj_backfill_helpers.get_level_download_links()
            self.str_json_file_path = obj_backfill_helpers.unpacking_GLEIF_zip_files(str_download_link = str_level_1_download_link , str_unpacked_zip_file_name = "Level_1_unpacked" , str_zip_file_path_name = "Level_1.zip")
    
        str_unpacked_zip_file_name = os.listdir(rf"../file_lib/Level_1_unpacked")[0]
        self.str_json_file_path = rf"../file_lib/Level_1_unpacked" + "//" + str_unpacked_zip_file_name
        self.conn = sqlite3.connect(f'{str_db_name}.db', timeout=10)
    
    def create_tables(self, conn):
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_entity_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lei TEXT NOT NULL,                
                LegalName TEXT,
                SuccessorEntityLEI TEXT,
                SuccessorEntityName TEXT,
                LegalJurisdiction TEXT,
                EntityCategory TEXT,
                EntitySubCategory TEXT,
                LegalForm_EntityLegalFormCode TEXT,
                LegalForm_OtherLegalForm TEXT,
                EntityStatus TEXT,
                EntityCreationDate TEXT,
                RegistrationAuthority_RegistrationAuthorityID TEXT,
                RegistrationAuthority_RegistrationAuthorityEntityID TEXT
                );
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_other_legal_names (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lei TEXT NOT NULL,                
                OtherEntityNames,
                Type,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei)
                );
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_LegalAddress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lei TEXT NOT NULL,
                LegalAddress_FirstAddressLine TEXT,
                LegalAddress_AdditionalAddressLine_1 TEXT,
                LegalAddress_AdditionalAddressLine_2 TEXT,
                LegalAddress_AdditionalAddressLine_3 TEXT,
                LegalAddress_City TEXT,
                LegalAddress_Region TEXT,
                LegalAddress_Country TEXT,
                LegalAddress_PostalCode TEXT,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei)
                );
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_HeadquartersAddress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lei TEXT NOT NULL,
                HeadquartersAddress_FirstAddressLine TEXT,
                HeadquartersAddress_AdditionalAddressLine_1 TEXT,
                HeadquartersAddress_AdditionalAddressLine_2 TEXT,
                HeadquartersAddress_AdditionalAddressLine_3 TEXT,
                HeadquartersAddress_City TEXT,
                HeadquartersAddress_Region TEXT,
                HeadquartersAddress_Country TEXT,
                HeadquartersAddress_PostalCode TEXT,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei)
                );
            """)
                        
            #entity
            conn.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_LegalEntityEvents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lei TEXT NOT NULL,
                group_type TEXT,
                event_status TEXT,
                LegalEntityEventType TEXT,
                LegalEntityEventEffectiveDate TEXT,
                LegalEntityEventRecordedDate TEXT,
                ValidationDocuments TEXT,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei)
                );
                """)
            
            #registration
            conn.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_registration_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lei TEXT NOT NULL,
                InitialRegistrationDate TEXT NOT NULL,
                LastUpdateDate TEXT,
                RegistrationStatus TEXT,
                NextRenewalDate TEXT,
                ManagingLOU TEXT,
                ValidationSources TEXT,
                ValidationAuthority TEXT,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei)
                );
            """)

            #geoencoding
            conn.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_geocoding (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lei TEXT NOT NULL,
                relevance REAL,
                match_type TEXT,
                lat REAL,
                lng REAL,
                geocoding_date TEXT,
                TopLeft_Latitude REAL,
                TopLeft_Longitude REAL,
                BottomRight_Latitude REAL,
                BottomRight_Longitude REAL,
                match_level TEXT,
                mapped_street TEXT,
                mapped_housenumber TEXT,
                mapped_postalcode TEXT,
                mapped_city TEXT,
                mapped_district TEXT,
                mapped_state TEXT,
                mapped_country TEXT,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei)
                );
            """)
            
            
        conn.commit()
    
    def insert_entity_data(self , str_lei , list_entity_data):
        
        list_entity_data = list_entity_data if list_entity_data else [None] * 12

        
        with self.conn:
            self.conn.execute("""
                INSERT INTO GLEIF_entity_data (
                    lei, LegalName, SuccessorEntityLEI, SuccessorEntityName, LegalJurisdiction,
                    EntityCategory, EntitySubCategory, LegalForm_EntityLegalFormCode,
                    LegalForm_OtherLegalForm, EntityStatus, EntityCreationDate,
                    RegistrationAuthority_RegistrationAuthorityID,
                    RegistrationAuthority_RegistrationAuthorityEntityID
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [str_lei] + list_entity_data)
            
    def insert_other_name_data(self, str_lei, list_other_name_data):
        """
        Inserts other entity names into the GLEIF_other_legal_names table.

        Args:
            lei (str): The LEI of the entity.
            other_name_data (list): A list of dictionaries containing 'OtherEntityNames' and 'Type'.
        """
        with self.conn:
            if list_other_name_data:
                for name_record in list_other_name_data:
                    self.conn.execute("""
                        INSERT INTO GLEIF_other_legal_names (
                            lei, OtherEntityNames, Type
                        ) VALUES (?, ?, ?);
                    """, (str_lei, name_record[0], name_record[1]))
            else:
                self.conn.execute("""
                        INSERT INTO GLEIF_other_legal_names (
                            lei, OtherEntityNames, Type
                        ) VALUES (?, ?, ?);
                    """, (str_lei, None, None))

                
    def insert_legal_address_data(self, str_lei, list_legal_address_data):
        """
        Inserts legal address data into the GLEIF_LegalAddress table.

        Args:
            lei (str): The LEI of the entity.
            legal_address_data (list): A list containing legal address data in the following order:
                [LegalAddress_FirstAddressLine, LegalAddress_AdditionalAddressLine_1,
                LegalAddress_AdditionalAddressLine_2, LegalAddress_AdditionalAddressLine_3,
                LegalAddress_City, LegalAddress_Region, LegalAddress_Country,
                LegalAddress_PostalCode]
        """
        list_legal_address_data = list_legal_address_data if list_legal_address_data else [None] * 8
        
        with self.conn:
            self.conn.execute("""
                INSERT INTO GLEIF_LegalAddress (
                    lei, LegalAddress_FirstAddressLine, LegalAddress_AdditionalAddressLine_1,
                    LegalAddress_AdditionalAddressLine_2, LegalAddress_AdditionalAddressLine_3,
                    LegalAddress_City, LegalAddress_Region, LegalAddress_Country,
                    LegalAddress_PostalCode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [str_lei] + list_legal_address_data)
            
    def insert_headquarter_address_data(self, str_lei, list_hq_address_data):
        """
        Inserts headquarters address data into the GLEIF_HeadquartersAddress table.

        Args:
            lei (str): The LEI of the entity.
            hq_address_data (list): A list containing headquarters address data in the following order:
                [HeadquartersAddress_FirstAddressLine, HeadquartersAddress_AdditionalAddressLine_1,
                HeadquartersAddress_AdditionalAddressLine_2, HeadquartersAddress_AdditionalAddressLine_3,
                HeadquartersAddress_City, HeadquartersAddress_Region, HeadquartersAddress_Country,
                HeadquartersAddress_PostalCode]
        """
        list_hq_address_data = list_hq_address_data if list_hq_address_data else [None] * 8        
        
        with self.conn:
            self.conn.execute("""
                INSERT INTO GLEIF_HeadquartersAddress (
                    lei, HeadquartersAddress_FirstAddressLine, HeadquartersAddress_AdditionalAddressLine_1,
                    HeadquartersAddress_AdditionalAddressLine_2, HeadquartersAddress_AdditionalAddressLine_3,
                    HeadquartersAddress_City, HeadquartersAddress_Region, HeadquartersAddress_Country,
                    HeadquartersAddress_PostalCode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, [str_lei] + list_hq_address_data)

    def insert_legal_entity_events_data(self, str_lei, list_legal_entity_events_data):
        """
        Inserts legal entity event data into the GLEIF_LegalEntityEvents table.

        Args:
            lei (str): The LEI of the entity.
            legal_entity_events_data (list): A list of dictionaries containing event data.
        """
        with self.conn:
            if list_legal_entity_events_data:
                if len(list_legal_entity_events_data) == 1:
                    self.conn.execute("""
                            INSERT INTO GLEIF_LegalEntityEvents (
                                lei, group_type, event_status, LegalEntityEventType,
                                LegalEntityEventEffectiveDate, LegalEntityEventRecordedDate,
                                ValidationDocuments
                            ) VALUES (?, ?, ?, ?, ?, ?, ?);
                        """, (str_lei, list_legal_entity_events_data[0][0], list_legal_entity_events_data[0][1], list_legal_entity_events_data[0][2], list_legal_entity_events_data[0][3], list_legal_entity_events_data[0][4], list_legal_entity_events_data[0][5]))
                else:
                    for event in list_legal_entity_events_data:
                        self.conn.execute("""
                            INSERT INTO GLEIF_LegalEntityEvents (
                                lei, group_type, event_status, LegalEntityEventType,
                                LegalEntityEventEffectiveDate, LegalEntityEventRecordedDate,
                                ValidationDocuments
                            ) VALUES (?, ?, ?, ?, ?, ?, ?);
                        """, (str_lei, event[0], event[1], event[2], event[3], event[4], event[5]))
            else:
                self.conn.execute("""
                        INSERT INTO GLEIF_LegalEntityEvents (
                            lei, group_type, event_status, LegalEntityEventType,
                            LegalEntityEventEffectiveDate, LegalEntityEventRecordedDate,
                            ValidationDocuments
                        ) VALUES (?, ?, ?, ?, ?, ?, ?);
                    """, (str_lei, None, None, None, None, None, None))

    def insert_registration_data(self, str_lei, list_registration_data):
        """
        Inserts registration data into the GLEIF_registration_data table.

        Args:
            lei (str): The LEI of the entity.
            registration_data (list): A list containing registration data in the following order:
                [InitialRegistrationDate, LastUpdateDate, RegistrationStatus, NextRenewalDate,
                ManagingLOU, ValidationSources, ValidationAuthority]
        """
        list_registration_data = list_registration_data if list_registration_data else [None] * 7        
        
        with self.conn:
            self.conn.execute("""
                INSERT INTO GLEIF_registration_data (
                    lei, InitialRegistrationDate, LastUpdateDate, RegistrationStatus,
                    NextRenewalDate, ManagingLOU, ValidationSources, ValidationAuthority
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """, [str_lei] + list_registration_data)

    def insert_geocoding_data(self, str_lei, list_geocoding_data):
        """
        Inserts geocoding data into the GLEIF_geocoding table.

        Args:
            lei (str): The LEI of the entity.
            geocoding_data_list (list): A list of dictionaries containing geocoding data.
        """
        with self.conn:
            if all(not isinstance(item , list) for item in list_geocoding_data):
                self.conn.execute("""
                        INSERT INTO GLEIF_geocoding (
                            lei, relevance, match_type, lat, lng, geocoding_date,
                            "TopLeft_Latitude", "TopLeft_Longitude", "BottomRight_Latitude", "BottomRight_Longitude",
                            match_level, mapped_street, mapped_housenumber, mapped_postalcode,
                            mapped_city, mapped_district, mapped_state, mapped_country
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        str_lei,
                        list_geocoding_data[0],
                        list_geocoding_data[1],
                        list_geocoding_data[2],
                        list_geocoding_data[3],
                        list_geocoding_data[4],
                        list_geocoding_data[5],
                        list_geocoding_data[6],
                        list_geocoding_data[7],
                        list_geocoding_data[8],
                        list_geocoding_data[9],
                        list_geocoding_data[10],
                        list_geocoding_data[11],
                        list_geocoding_data[12],
                        list_geocoding_data[13],
                        list_geocoding_data[14],
                        list_geocoding_data[15],
                        list_geocoding_data[16]
                    ))
            else:
                for geocoding_data in list_geocoding_data:                    
                    self.conn.execute("""
                        INSERT INTO GLEIF_geocoding (
                            lei, relevance, match_type, lat, lng, geocoding_date,
                            "TopLeft_Latitude", "TopLeft_Longitude", "BottomRight_Latitude", "BottomRight_Longitude",
                            match_level, mapped_street, mapped_housenumber, mapped_postalcode,
                            mapped_city, mapped_district, mapped_state, mapped_country
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        str_lei,
                        geocoding_data[0],
                        geocoding_data[1],
                        geocoding_data[2],
                        geocoding_data[3],
                        geocoding_data[4],
                        geocoding_data[5],
                        geocoding_data[6],
                        geocoding_data[7],
                        geocoding_data[8],
                        geocoding_data[9],
                        geocoding_data[10],
                        geocoding_data[11],
                        geocoding_data[12],
                        geocoding_data[13],
                        geocoding_data[14],
                        geocoding_data[15],
                        geocoding_data[16]
                    ))

    def process_entity_data(self , dict_entity_data , str_lei):
        list_gleif_entity_data = self.obj_backfill_helpers.get_target_values(dict_data = dict_entity_data , target_keys = ["LegalName" , "SuccessorEntityLEI" , "SuccessorEntityName" , "LegalJurisdiction" , "EntityCategory" , "EntitySubCategory" , "LegalForm_EntityLegalFormCode" , "LegalForm_OtherLegalForm" , "EntityStatus" , "EntityCreationDate" , "RegistrationAuthority_RegistrationAuthorityID" , "RegistrationAuthority_RegistrationAuthorityEntityID" ])      
        list_gleif_other_name_data = self.obj_backfill_helpers.extract_other_entity_names(data_dict = dict_entity_data, base_keyword="OtherEntityNames", exclude_keywords=["TranslatedOtherEntityNames"]) 
        list_gleif_legal_address_data = self.obj_backfill_helpers.get_target_values(dict_data = dict_entity_data , target_keys = ["LegalAddress_FirstAddressLine" , "LegalAddress_AdditionalAddressLine_1" , "LegalAddress_AdditionalAddressLine_2" , "LegalAddress_AdditionalAddressLine_3" , "LegalAddress_City" , "LegalAddress_Region" , "LegalAddress_Country" , "LegalAddress_PostalCode"])
        list_gleif_headquarter_address_data = self.obj_backfill_helpers.get_target_values(dict_data = dict_entity_data , target_keys = ["HeadquartersAddress_FirstAddressLine" , "HeadquartersAddress_AdditionalAddressLine_1" , "HeadquartersAddress_AdditionalAddressLine_2" , "HeadquartersAddress_AdditionalAddressLine_3" , "HeadquartersAddress_City" , "HeadquartersAddress_Region" , "HeadquartersAddress_Country" , "HeadquartersAddress_PostalCode"])
        list_gleif_legal_entity_event_data = self.obj_backfill_helpers.extract_event_data(dict_data = dict_entity_data , base_keyword="LegalEntityEvents" , target_keys=["group_type", "event_status", "LegalEntityEventType", "LegalEntityEventEffectiveDate", "LegalEntityEventRecordedDate", "ValidationDocuments"])

        self.insert_entity_data(str_lei = str_lei , list_entity_data = list_gleif_entity_data)
        self.insert_other_name_data(str_lei = str_lei , list_other_name_data = list_gleif_other_name_data)
        self.insert_legal_address_data(str_lei = str_lei , list_legal_address_data = list_gleif_legal_address_data)
        self.insert_headquarter_address_data(str_lei = str_lei , list_hq_address_data = list_gleif_headquarter_address_data)
        self.insert_legal_entity_events_data(str_lei = str_lei , list_legal_entity_events_data = list_gleif_legal_entity_event_data)
    
    def process_registration_data(self , dict_registration_data , str_lei):
        list_gleif_registration_dta = self.obj_backfill_helpers.get_target_values(dict_data = dict_registration_data , target_keys = ["InitialRegistrationDate" , "LastUpdateDate" , "RegistrationStatus" , "NextRenewalDate" , "ManagingLOU" , "ValidationSources" , "ValidationAuthority"])    
        self.insert_registration_data(str_lei = str_lei , list_registration_data = list_gleif_registration_dta)
    
    def process_extension_data(self , dict_extension_data , str_lei):
        dict_extension_data_flattened = self.obj_backfill_helpers.further_flatten_geocoding(dict_data = dict_extension_data)
        list_extension_data = []
        
        if any(re.search(r"_\d+_", key) for key in dict_extension_data_flattened.keys()):
            list_dicts = self.obj_backfill_helpers.split_into_list_of_dictionaries(dict_data = dict_extension_data_flattened)
            counter = 1
            for dict_extension in list_dicts:
                list_extension_data.append(self.obj_backfill_helpers.get_target_values(dict_data = dict_extension , subset_string = True, target_keys = ["relevance" , "match_type" , "lat" , "lng" , "geocoding_date" , "TopLeft.Latitude" , "TopLeft.Longitude" , "BottomRight.Latitude" , "BottomRight.Longitude" , "match_level" , "mapped_street" , "mapped_housenumber" , "mapped_postalcode" , "mapped_city" , "mapped_district" , "mapped_state" , "mapped_country"]))
            counter +=1
        else:
            list_extension_data = self.obj_backfill_helpers.get_target_values(dict_data = dict_extension_data_flattened , subset_string = True, target_keys = ["relevance" , "match_type" , "lat" , "lng" , "geocoding_date" , "TopLeft.Latitude" , "TopLeft.Longitude" , "BottomRight.Latitude" , "BottomRight.Longitude" , "match_level" , "mapped_street" , "mapped_housenumber" , "mapped_postalcode" , "mapped_city" , "mapped_district" , "mapped_state" , "mapped_country"])
        
        self.insert_geocoding_data(str_lei = str_lei , list_geocoding_data = list_extension_data) 
            
    def process_data(self , dict_lei_pre_processing):
        dict_lei = self.obj_backfill_helpers.flatten_dict(dict_lei_pre_processing)
        dict_lei_clean = self.obj_backfill_helpers.clean_keys(dict_lei)
        dict_organized = (self.obj_backfill_helpers.organize_by_prefix(dict_lei_clean))

        self.process_entity_data(dict_entity_data = dict_organized["Entity"] , str_lei = dict_organized["LEI"][""])
        self.process_registration_data(dict_registration_data = dict_organized["Registration"] , str_lei =  dict_organized["LEI"][""])
        self.process_extension_data(dict_extension_data = dict_organized["Extension"] , str_lei = dict_organized["LEI"][""])
        
    
    def storing_GLEIF_data_in_database(self):
        
        self.create_tables(conn = self.conn)
        
        
        with open(self.str_json_file_path, 'rb') as file:
            dict_leis = bigjson.load(file)
            
            counter = 0
            
            for dict_lei in dict_leis['records']:
                if counter != 10000:
                    dict_record = dict_lei.to_python()
                    self.process_data(dict_lei_pre_processing = dict_record) 
                else:
                    break
        self.conn.close()
        
if __name__ == "main":
    obj = GLEIFLevel1Data(bool_log = True)
    obj.storing_GLEIF_data_in_database()