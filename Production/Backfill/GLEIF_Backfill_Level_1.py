import os
import requests
import re
import logging
import sqlite3
import bigjson
import sys
import GLEIF_Backfill_Helpers


class GLEIFLevel1Data:
    def __init__(self , bool_log = True , str_db_name = "GLEIF_Data.db" , bool_downloaded = True):
        
        self.obj_backfill_helpers = GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers(bool_Level_1 = True)

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
                
            str_level_1_download_link = self.obj_backfill_helpers.get_level_download_links()
            self.str_json_file_path = self.obj_backfill_helpers.unpacking_GLEIF_zip_files(str_download_link = str_level_1_download_link , str_unpacked_zip_file_name = "Level_1_unpacked" , str_zip_file_path_name = "Level_1.zip")
    
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