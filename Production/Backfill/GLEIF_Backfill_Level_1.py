import os
import re
import logging
import psycopg2
import sys
import io
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, "..", ".."))
sys.path.append(target_directory)
import ijson

from Production.Backfill import GLEIF_Backfill_Helpers

class GLEIFLevel1Data_Rewritten:
    def __init__(self , bool_log = True , str_db_name = "GLEIF_test_db" , bool_downloaded = True):
        
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
        self.conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
    
    
    def drop_table(self , lst_table_names):
            """
            Drops a specific table from the database securely.
            
            Parameters:
                table_name (list of string): The names of the tables to drop.
            """

            for table_name in lst_table_names:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                
            self.conn.commit()
    
    def load_batches_as_list(self , file_path , batch_size=100000):
            """Yield records in batches as lists of dictionaries."""
            with open(file_path, 'rb') as file:
                # Create an iterator for the 'records' key
                records = ijson.items(file, "records.item")
                
                batch = []
                for index, record in enumerate(records, start=1):
                    batch.append(record)  # Add record to the batch
                    
                    if index % batch_size == 0:  # Yield the batch when size is reached
                        yield batch  # Yield the batch as a list
                        batch = []  # Reset for the next batch
                
                # Yield any remaining records
                if batch:
                    yield batch
    
    def create_tables(self):
        # GLEIF_entity_data
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLEIF_entity_data (
                id SERIAL PRIMARY KEY,
                lei TEXT UNIQUE,
                LegalName TEXT,
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

        # GLEIF_other_legal_names with unique constraint
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLEIF_other_legal_names (
                id SERIAL PRIMARY KEY,
                lei TEXT,
                Type TEXT,
                OtherEntityNames TEXT,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei),
                UNIQUE (lei, OtherEntityNames, Type)
            );
        """)

        # GLEIF_LegalAddress
        # If each LEI has only one legal address, keep the UNIQUE on lei
        # Otherwise, define a composite unique constraint as needed
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLEIF_LegalAddress (
                id SERIAL PRIMARY KEY,
                lei TEXT UNIQUE,
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

        # GLEIF_HeadquartersAddress
        # Similar to GLEIF_LegalAddress
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLEIF_HeadquartersAddress (
                id SERIAL PRIMARY KEY,
                lei TEXT UNIQUE,
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

        # GLEIF_LegalEntityEvents with unique constraint
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLEIF_LegalEntityEvents (
                id SERIAL PRIMARY KEY,
                lei TEXT,
                group_type TEXT,
                event_status TEXT,
                LegalEntityEventType TEXT,
                LegalEntityEventEffectiveDate TEXT,
                LegalEntityEventRecordedDate TEXT,
                ValidationDocuments TEXT,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei),
                UNIQUE (lei, group_type, event_status, LegalEntityEventType, LegalEntityEventEffectiveDate , LegalEntityEventRecordedDate, ValidationDocuments)
            );
        """)

        # GLEIF_registration_data
        # If each LEI has only one registration record, keep the UNIQUE on lei
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLEIF_registration_data (
                id SERIAL PRIMARY KEY,
                lei TEXT UNIQUE,
                InitialRegistrationDate TEXT,
                LastUpdateDate TEXT,
                RegistrationStatus TEXT,
                NextRenewalDate TEXT,
                ManagingLOU TEXT,
                ValidationSources TEXT,
                ValidationAuthorityID TEXT,
                ValidationAuthorityEntityID TEXT,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei)
            );
        """)

        # GLEIF_geocoding with unique constraint
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS GLEIF_geocoding (
                id SERIAL PRIMARY KEY,
                lei TEXT,
                relevance TEXT,
                match_type TEXT,
                lat TEXT,
                lng TEXT,
                geocoding_date TEXT,
                TopLeft_Latitude TEXT,
                TopLeft_Longitude TEXT,
                BottomRight_Latitude TEXT,
                BottomRight_longitude TEXT,
                match_level TEXT,
                mapped_street TEXT,
                mapped_housenumber TEXT,
                mapped_postalcode TEXT,
                mapped_city TEXT,
                mapped_district TEXT,
                mapped_state TEXT,
                mapped_country TEXT,
                FOREIGN KEY (lei) REFERENCES GLEIF_entity_data(lei),
                UNIQUE (lei, relevance, match_type, lat, geocoding_date, TopLeft_Latitude, TopLeft_Longitude, BottomRight_Latitude, BottomRight_longitude, match_level, mapped_street, mapped_housenumber, mapped_postalcode, mapped_city, mapped_district, mapped_state, mapped_country)
            );
        """)

        self.conn.commit()
    
    def bulk_insert_using_copy(self, table_name, columns, data):
        """Perform a bulk insert using PostgreSQL COPY with an in-memory buffer

        Args:
            table_name (str): Name of the table to insert into
            columns (list): List of column names for the table
            data (list): List of tuples with the data to be inserted
        """

        buffer = io.StringIO()

        for row in data:
            # Escape backslashes and replace None with \N for NULL
            row_converted = []
            for x in row:
                if x is None:
                    row_converted.append('\\N')  # NULL representation
                elif isinstance(x, str):
                    x = x.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
                    row_converted.append(x)
                else:
                    row_converted.append(str(x))
            buffer.write('\t'.join(row_converted) + '\n')
        
        buffer.seek(0)  # Reset buffer position

        # Construct the COPY query
        copy_query = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT text, DELIMITER E'\t', NULL '\\N')"
        self.cursor.copy_expert(copy_query, buffer)
        self.conn.commit()
        
    def process_entity_data(self , list_dict_records):
        list_entity_meta_data_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_clean , subset_string = "Entity" , target_keys = ["LegalName", "LegalJurisdiction", "EntityCategory", "EntitySubCategory", "LegalForm_EntityLegalFormCode", "LegalForm_OtherLegalForm", "EntityStatus", "EntityCreationDate", "RegistrationAuthority_RegistrationAuthorityID", "RegistrationAuthority_RegistrationAuthorityEntityID"])
            list_output.insert(0 , dict_clean["LEI"])
            list_entity_meta_data_tuples.append(tuple(list_output))
        
        self.bulk_insert_using_copy(data = list_entity_meta_data_tuples , table_name = "GLEIF_entity_data" , 
                            columns = 
                            ["lei",
                                "LegalName",
                                "LegalJurisdiction",
                                "EntityCategory",
                                "EntitySubCategory",
                                "LegalForm_EntityLegalFormCode",
                                "LegalForm_OtherLegalForm",
                                "EntityStatus",
                                "EntityCreationDate",
                                "RegistrationAuthority_RegistrationAuthorityID",
                                "RegistrationAuthority_RegistrationAuthorityEntityID"])
    
    def process_other_legal_names(self , list_dict_records):
        list_other_names_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            dict_entity = (self.obj_backfill_helpers.organize_by_prefix(dict_clean))["Entity"]
            list_output = self.obj_backfill_helpers.extract_other_entity_names(data_dict = dict_entity, base_keyword="OtherEntityNames", exclude_keywords=["TranslatedOtherEntityNames"]) 
            for index, tup in enumerate(list_output):
                list_output[index] = (dict_clean["LEI"],) + tup         
            list_other_names_tuples.extend(list_output)
        
        list_clean_other_names_tuples = list(set(list_other_names_tuples))
        
        self.bulk_insert_using_copy(data = list_clean_other_names_tuples , table_name = "GLEIF_other_legal_names" , columns = ["lei", "Type" , "OtherEntityNames"])        
    
    def process_legal_address(self , list_dict_records):
        list_legal_address_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_clean , target_keys = ["Entity_LegalAddress_FirstAddressLine" , "Entity_LegalAddress_AdditionalAddressLine_1" , "Entity_LegalAddress_AdditionalAddressLine_2" , "Entity_LegalAddress_AdditionalAddressLine_3" , "Entity_LegalAddress_City" , "Entity_LegalAddress_Region" , "Entity_LegalAddress_Country" , "Entity_LegalAddress_PostalCode"])
            list_output.insert(0 , dict_clean["LEI"])
            list_legal_address_tuples.append(tuple(list_output))

        
        self.bulk_insert_using_copy(data = list_legal_address_tuples , table_name = "GLEIF_LegalAddress" , 
                                columns = ["lei",
                                            "LegalAddress_FirstAddressLine",
                                            "LegalAddress_AdditionalAddressLine_1",
                                            "LegalAddress_AdditionalAddressLine_2",
                                            "LegalAddress_AdditionalAddressLine_3",
                                            "LegalAddress_City",
                                            "LegalAddress_Region",
                                            "LegalAddress_Country",
                                            "LegalAddress_PostalCode"])  
    
    def process_headquarters_address(self , list_dict_records):
        list_headquarters_address_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            dict_entity = (self.obj_backfill_helpers.organize_by_prefix(dict_clean))["Entity"]
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_entity , target_keys = ["HeadquartersAddress_FirstAddressLine" , "HeadquartersAddress_AdditionalAddressLine_1" , "HeadquartersAddress_AdditionalAddressLine_2" , "HeadquartersAddress_AdditionalAddressLine_3" , "HeadquartersAddress_City" , "HeadquartersAddress_Region" , "HeadquartersAddress_Country" , "HeadquartersAddress_PostalCode"])
            list_output.insert(0 , dict_clean["LEI"])
            list_headquarters_address_tuples.append(tuple(list_output))
        
        self.bulk_insert_using_copy(data = list_headquarters_address_tuples , table_name = "GLEIF_HeadquartersAddress" , 
                                columns = ["lei",
                                            "HeadquartersAddress_FirstAddressLine",
                                            "HeadquartersAddress_AdditionalAddressLine_1",
                                            "HeadquartersAddress_AdditionalAddressLine_2",
                                            "HeadquartersAddress_AdditionalAddressLine_3",
                                            "HeadquartersAddress_City",
                                            "HeadquartersAddress_Region",
                                            "HeadquartersAddress_Country",
                                            "HeadquartersAddress_PostalCode"]) 
    
    def process_legal_entity_events(self , list_dict_records):
        list_legal_entity_events_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            dict_entity = (self.obj_backfill_helpers.organize_by_prefix(dict_clean))["Entity"]
            list_output = self.obj_backfill_helpers.extract_event_data(dict_data = dict_entity , base_keyword="LegalEntityEvents" , target_keys=["group_type", "event_status", "LegalEntityEventType", "LegalEntityEventEffectiveDate", "LegalEntityEventRecordedDate", "ValidationDocuments"])
            for index, tup in enumerate(list_output):
                list_output[index] = (dict_clean["LEI"],) + tup 
            list_legal_entity_events_tuples.extend(list_output)

        list_clean_legal_entity_events_tuples = list(set(list_legal_entity_events_tuples))

        self.bulk_insert_using_copy(data = list_clean_legal_entity_events_tuples , table_name = "GLEIF_LegalEntityEvents" , 
                                columns = ["lei",
                                        "group_type",
                                        "event_status",
                                        "LegalEntityEventType",
                                        "LegalEntityEventEffectiveDate",
                                        "LegalEntityEventRecordedDate",
                                        "ValidationDocuments"])
        
    def process_registration_data(self , list_dict_records):
        list_registration_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            dict_registration = (self.obj_backfill_helpers.organize_by_prefix(dict_clean))["Registration"]
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_registration , target_keys = ["InitialRegistrationDate" , "LastUpdateDate" , "RegistrationStatus" , "NextRenewalDate" , "ManagingLOU" , "ValidationSources" , "ValidationAuthority_ValidationAuthorityID" , "ValidationAuthority_ValidationAuthorityEntityID"])
            list_output.insert(0 , dict_clean["LEI"])
            list_registration_tuples.append(tuple(list_output))

        self.bulk_insert_using_copy(data = list_registration_tuples , table_name = "GLEIF_registration_data" , 
                                columns = [
                                            "lei",
                                            "InitialRegistrationDate",
                                            "LastUpdateDate",
                                            "RegistrationStatus",
                                            "NextRenewalDate",
                                            "ManagingLOU",
                                            "ValidationSources",
                                            "ValidationAuthorityID",
                                            "ValidationAuthorityEntityID"]) 
    
    def process_geoencoding_data(self , list_dict_records):
        list_extension_data_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            dict_extension = (self.obj_backfill_helpers.organize_by_prefix(dict_clean))["Extension"]
            dict_mega_flat = self.obj_backfill_helpers.further_flatten_geocoding(dict_data = dict_extension)
            if any(re.search(r"_\d+_", key) for key in dict_mega_flat.keys()):
                list_dicts = self.obj_backfill_helpers.split_into_list_of_dictionaries(dict_data = dict_mega_flat)
                for dict_extension in list_dicts:
                    list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_extension , subset_string = True, target_keys = ["relevance" , "match_type" , "lat" , "lng" , "geocoding_date" , "TopLeft.Latitude" , "TopLeft.Longitude" , "BottomRight.Latitude" , "BottomRight.Longitude" , "match_level" , "mapped_street" , "mapped_housenumber" , "mapped_postalcode" , "mapped_city" , "mapped_district" , "mapped_state" , "mapped_country"])
                    list_output.insert(0 , dict_clean["LEI"])
                    list_extension_data_tuples.append(tuple(list_output))
            else:
                list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_mega_flat , subset_string = True, target_keys = ["relevance" , "match_type" , "lat" , "lng" , "geocoding_date" , "TopLeft.Latitude" , "TopLeft.Longitude" , "BottomRight.Latitude" , "BottomRight.Longitude" , "match_level" , "mapped_street" , "mapped_housenumber" , "mapped_postalcode" , "mapped_city" , "mapped_district" , "mapped_state" , "mapped_country"])
                list_output.insert(0 , dict_clean["LEI"])
                list_extension_data_tuples.append(tuple(list_output))

        list_clean_extension_data_tuples = list(set(list_extension_data_tuples))
        
        with open("output.txt", "w", encoding="utf-8") as file:
            for item in list_clean_extension_data_tuples:
                file.write(f"{item}\n")    
                    
        self.bulk_insert_using_copy(data = list_clean_extension_data_tuples , table_name = "GLEIF_geocoding" , 
                                columns = ["lei",
                                            "relevance",
                                            "match_type",
                                            "lat",
                                            "lng",
                                            "geocoding_date",
                                            "TopLeft_Latitude",
                                            "TopLeft_Longitude",
                                            "BottomRight_Latitude",
                                            "BottomRight_longitude",
                                            "match_level",
                                            "mapped_street",
                                            "mapped_housenumber",
                                            "mapped_postalcode",
                                            "mapped_city",
                                            "mapped_district",
                                            "mapped_state",
                                            "mapped_country"]) 
    
    def process_all_data(self , list_dict_records):
        self.process_entity_data(list_dict_records = list_dict_records)
        self.process_other_legal_names(list_dict_records = list_dict_records)
        self.process_legal_address(list_dict_records = list_dict_records)
        self.process_headquarters_address(list_dict_records = list_dict_records)
        self.process_legal_entity_events(list_dict_records = list_dict_records)
        self.process_registration_data(list_dict_records = list_dict_records)
        self.process_geoencoding_data(list_dict_records = list_dict_records)
    
    def storing_GLEIF_data_in_database(self):
        
        self.create_tables()
        
        generator_batched_json = self.load_batches_as_list(file_path = self.str_json_file_path , batch_size = 100000)
        
        for index , list_dict_records in enumerate(generator_batched_json):
            self.process_all_data(list_dict_records = list_dict_records)
        
        self.conn.close()
        
if __name__ == "main":
    obj = GLEIFLevel1Data_Rewritten(bool_log = True)
    obj.storing_GLEIF_data_in_database()