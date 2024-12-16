import os
import logging
import json
import json
import psycopg2
import sys
import io
import re
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, "..", ".."))
sys.path.append(target_directory)

from Production.Update import GLEIF_Update_Helpers
from Production.Backfill import GLEIF_Backfill_Helpers

class GLEIFUpdateLevel1:
    def __init__(self , bool_log = True , str_db_name = "GLEIF_test_db" , bool_downloaded = True):
        self.obj_update_helpers = GLEIF_Update_Helpers.GLEIF_Update_Helpers(bool_Level_1 = True)
        self.obj_backfill_helpers = GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers(bool_Level_1 = True)
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
    
        self.str_json_file_path = '../file_lib/Level_1_update_unpacked\\20241130-0000-gleif-goldencopy-lei2-intra-day.json'
        self.conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
        #self.conn.autocommit = True
        self.cursor = self.conn.cursor()
    
    def bulk_upsert_using_copy(self, table_name, columns, data, conflict_columns, set_clause=None, do_nothing=False, compare_columns=None):
        """
        Perform a bulk upsert using PostgreSQL COPY with a temporary table.
        Supports both:
        - Upsert with conditional update (skip exact duplicates).
        - Insert with conflict ignoring.
        
        Args:
            table_name (str): Name of the target table.
            columns (list): List of column names.
            data (list of tuples): Data to be inserted.
            conflict_columns (list): Columns that define uniqueness.
            set_clause (str, optional): SET clause for DO UPDATE.
            do_nothing (bool, optional): If True, perform DO NOTHING on conflict.
            compare_columns (list, optional): Columns to compare for skipping updates.
        """
        temp_table = f"{table_name}_temp"

        # Step 1: Create a temporary table
        create_temp_table_query = f"""
            CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING ALL)
            ON COMMIT DROP;
        """
        self.cursor.execute(create_temp_table_query)

        # Step 2: Copy data into the temporary table
        buffer = io.StringIO()
        for row in data:
            # Escape backslashes, tabs, and newlines
            escaped_row = []
            for item in row:
                if item is None:
                    escaped_row.append('\\N')  # NULL representation
                elif isinstance(item, str):
                    item = item.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
                    escaped_row.append(item)
                else:
                    escaped_row.append(str(item))
            buffer.write('\t'.join(escaped_row) + "\n")
        buffer.seek(0)

        copy_query = f"""
            COPY {temp_table} ({', '.join(columns)})
            FROM STDIN WITH (FORMAT text, DELIMITER '\t', NULL '\\N')
        """
        self.cursor.copy_expert(copy_query, buffer)

        # Step 3: Construct the upsert query based on `do_nothing`
        conflict_clause = ', '.join(conflict_columns)

        if do_nothing:
            # Perform INSERT ... ON CONFLICT DO NOTHING
            upsert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                SELECT {', '.join(columns)} FROM {temp_table}
                ON CONFLICT ({conflict_clause}) DO NOTHING;
            """
        else:
            # Perform INSERT ... ON CONFLICT DO UPDATE SET ... WHERE ...
            if not set_clause or not compare_columns:
                raise ValueError("For upsert operations, `set_clause` and `compare_columns` must be provided when `do_nothing=False`.")

            # Build the WHERE clause to skip exact duplicates
            where_conditions = " OR ".join(
                f"{table_name}.{col} IS DISTINCT FROM EXCLUDED.{col}"
                for col in compare_columns
            )
            where_clause = f"WHERE ({where_conditions})"

            upsert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                SELECT {', '.join(columns)} FROM {temp_table}
                ON CONFLICT ({conflict_clause}) DO UPDATE SET
                    {set_clause}
                {where_clause};
            """

        # Execute the upsert query
        self.cursor.execute(upsert_query)

    def process_entity_data(self , list_dict_records):
        list_entity_meta_data_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_clean , subset_string = "Entity" , target_keys = ["LegalName", "LegalJurisdiction", "EntityCategory", "EntitySubCategory", "LegalForm_EntityLegalFormCode", "LegalForm_OtherLegalForm", "EntityStatus", "EntityCreationDate", "RegistrationAuthority_RegistrationAuthorityID", "RegistrationAuthority_RegistrationAuthorityEntityID"])
            list_output.insert(0 , dict_clean["LEI"])
            list_entity_meta_data_tuples.append(tuple(list_output))
        
        self.bulk_upsert_using_copy(
            data=list_entity_meta_data_tuples,
            table_name="GLEIF_entity_data",
            columns=[
                "lei",
                "LegalName",
                "LegalJurisdiction",
                "EntityCategory",
                "EntitySubCategory",
                "LegalForm_EntityLegalFormCode",
                "LegalForm_OtherLegalForm",
                "EntityStatus",
                "EntityCreationDate",
                "RegistrationAuthority_RegistrationAuthorityID",
                "RegistrationAuthority_RegistrationAuthorityEntityID"
            ],
            conflict_columns=["lei"],  # Unique constraint on 'lei'
            compare_columns=[
                "LegalName",
                "LegalJurisdiction",
                "EntityCategory",
                "EntitySubCategory",
                "LegalForm_EntityLegalFormCode",
                "LegalForm_OtherLegalForm",
                "EntityStatus",
                "EntityCreationDate",
                "RegistrationAuthority_RegistrationAuthorityID",
                "RegistrationAuthority_RegistrationAuthorityEntityID"
            ],
            set_clause="""
                LegalName = EXCLUDED.LegalName,
                LegalJurisdiction = EXCLUDED.LegalJurisdiction,
                EntityCategory = EXCLUDED.EntityCategory,
                EntitySubCategory = EXCLUDED.EntitySubCategory,
                LegalForm_EntityLegalFormCode = EXCLUDED.LegalForm_EntityLegalFormCode,
                LegalForm_OtherLegalForm = EXCLUDED.LegalForm_OtherLegalForm,
                EntityStatus = EXCLUDED.EntityStatus,
                EntityCreationDate = EXCLUDED.EntityCreationDate,
                RegistrationAuthority_RegistrationAuthorityID = EXCLUDED.RegistrationAuthority_RegistrationAuthorityID,
                RegistrationAuthority_RegistrationAuthorityEntityID = EXCLUDED.RegistrationAuthority_RegistrationAuthorityEntityID
            """,
            do_nothing=False  # Perform upsert with possible updates
        )
                            
    
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

        self.bulk_upsert_using_copy(
            data=list_clean_other_names_tuples,
            table_name="GLEIF_other_legal_names",
            columns=["lei", "OtherEntityNames", "Type"],
            conflict_columns=["lei", "OtherEntityNames", "Type"],  # Composite unique constraint
            do_nothing=True  # Perform INSERT ... ON CONFLICT DO NOTHING
        )
        
    def process_legal_address(self , list_dict_records):
        list_legal_address_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_clean , target_keys = ["Entity_LegalAddress_FirstAddressLine" , "Entity_LegalAddress_AdditionalAddressLine_1" , "Entity_LegalAddress_AdditionalAddressLine_2" , "Entity_LegalAddress_AdditionalAddressLine_3" , "Entity_LegalAddress_City" , "Entity_LegalAddress_Region" , "Entity_LegalAddress_Country" , "Entity_LegalAddress_PostalCode"])
            list_output.insert(0 , dict_clean["LEI"])
            list_legal_address_tuples.append(tuple(list_output))

        self.bulk_upsert_using_copy(
            data=list_legal_address_tuples,
            table_name="GLEIF_LegalAddress",
            columns=[
                "lei",
                "LegalAddress_FirstAddressLine",
                "LegalAddress_AdditionalAddressLine_1",
                "LegalAddress_AdditionalAddressLine_2",
                "LegalAddress_AdditionalAddressLine_3",
                "LegalAddress_City",
                "LegalAddress_Region",
                "LegalAddress_Country",
                "LegalAddress_PostalCode"
            ],
            conflict_columns=["lei"],  # Unique constraint on 'lei'
            compare_columns=[
                "LegalAddress_FirstAddressLine",
                "LegalAddress_AdditionalAddressLine_1",
                "LegalAddress_AdditionalAddressLine_2",
                "LegalAddress_AdditionalAddressLine_3",
                "LegalAddress_City",
                "LegalAddress_Region",
                "LegalAddress_Country",
                "LegalAddress_PostalCode"
            ],
            set_clause="""
                LegalAddress_FirstAddressLine = EXCLUDED.LegalAddress_FirstAddressLine,
                LegalAddress_AdditionalAddressLine_1 = EXCLUDED.LegalAddress_AdditionalAddressLine_1,
                LegalAddress_AdditionalAddressLine_2 = EXCLUDED.LegalAddress_AdditionalAddressLine_2,
                LegalAddress_AdditionalAddressLine_3 = EXCLUDED.LegalAddress_AdditionalAddressLine_3,
                LegalAddress_City = EXCLUDED.LegalAddress_City,
                LegalAddress_Region = EXCLUDED.LegalAddress_Region,
                LegalAddress_Country = EXCLUDED.LegalAddress_Country,
                LegalAddress_PostalCode = EXCLUDED.LegalAddress_PostalCode
            """,
            do_nothing=False  # Perform upsert with possible updates
        )
    
    def process_headquarters_address(self , list_dict_records):
        list_headquarters_address_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            dict_entity = (self.obj_backfill_helpers.organize_by_prefix(dict_clean))["Entity"]
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_entity , target_keys = ["HeadquartersAddress_FirstAddressLine" , "HeadquartersAddress_AdditionalAddressLine_1" , "HeadquartersAddress_AdditionalAddressLine_2" , "HeadquartersAddress_AdditionalAddressLine_3" , "HeadquartersAddress_City" , "HeadquartersAddress_Region" , "HeadquartersAddress_Country" , "HeadquartersAddress_PostalCode"])
            list_output.insert(0 , dict_clean["LEI"])
            list_headquarters_address_tuples.append(tuple(list_output))

        self.bulk_upsert_using_copy(
            data=list_headquarters_address_tuples,
            table_name="GLEIF_HeadquartersAddress",
            columns=[
                "lei",
                "HeadquartersAddress_FirstAddressLine",
                "HeadquartersAddress_AdditionalAddressLine_1",
                "HeadquartersAddress_AdditionalAddressLine_2",
                "HeadquartersAddress_AdditionalAddressLine_3",
                "HeadquartersAddress_City",
                "HeadquartersAddress_Region",
                "HeadquartersAddress_Country",
                "HeadquartersAddress_PostalCode"
            ],
            conflict_columns=["lei"],  # Unique constraint on 'lei'
            compare_columns=[
                "HeadquartersAddress_FirstAddressLine",
                "HeadquartersAddress_AdditionalAddressLine_1",
                "HeadquartersAddress_AdditionalAddressLine_2",
                "HeadquartersAddress_AdditionalAddressLine_3",
                "HeadquartersAddress_City",
                "HeadquartersAddress_Region",
                "HeadquartersAddress_Country",
                "HeadquartersAddress_PostalCode"
            ],
            set_clause="""
                HeadquartersAddress_FirstAddressLine = EXCLUDED.HeadquartersAddress_FirstAddressLine,
                HeadquartersAddress_AdditionalAddressLine_1 = EXCLUDED.HeadquartersAddress_AdditionalAddressLine_1,
                HeadquartersAddress_AdditionalAddressLine_2 = EXCLUDED.HeadquartersAddress_AdditionalAddressLine_2,
                HeadquartersAddress_AdditionalAddressLine_3 = EXCLUDED.HeadquartersAddress_AdditionalAddressLine_3,
                HeadquartersAddress_City = EXCLUDED.HeadquartersAddress_City,
                HeadquartersAddress_Region = EXCLUDED.HeadquartersAddress_Region,
                HeadquartersAddress_Country = EXCLUDED.HeadquartersAddress_Country,
                HeadquartersAddress_PostalCode = EXCLUDED.HeadquartersAddress_PostalCode
            """,
            do_nothing=False  # Perform upsert with possible updates
        )
                                
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

        self.bulk_upsert_using_copy(
            data=list_clean_legal_entity_events_tuples,
            table_name="GLEIF_LegalEntityEvents",
            columns=[
                "lei",
                "group_type",
                "event_status",
                "LegalEntityEventType",
                "LegalEntityEventEffectiveDate",
                "LegalEntityEventRecordedDate",
                "ValidationDocuments"
            ],
            conflict_columns=[
                "lei",
                "group_type",
                "event_status",
                "LegalEntityEventType", 
                "LegalEntityEventEffectiveDate", 
                "LegalEntityEventRecordedDate",
                "ValidationDocuments"
            ],  # Composite unique constraint
            do_nothing=True  # Perform INSERT ... ON CONFLICT DO NOTHING
        )
        
    def process_registration_data(self , list_dict_records):
        list_registration_tuples = []
        
        for dict_record in list_dict_records:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_record)
            dict_clean = self.obj_backfill_helpers.clean_keys(input_dict = dict_flat)
            dict_registration = (self.obj_backfill_helpers.organize_by_prefix(dict_clean))["Registration"]
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_registration , target_keys = ["InitialRegistrationDate" , "LastUpdateDate" , "RegistrationStatus" , "NextRenewalDate" , "ManagingLOU" , "ValidationSources" , "ValidationAuthorityID" , "ValidationAuthorityEntityID"])    
            list_output.insert(0 , dict_clean["LEI"])
            list_registration_tuples.append(tuple(list_output))

        self.bulk_upsert_using_copy(
            data=list_registration_tuples,
            table_name="GLEIF_registration_data",
            columns=[
                "lei",
                "InitialRegistrationDate",
                "LastUpdateDate",
                "RegistrationStatus",
                "NextRenewalDate",
                "ManagingLOU",
                "ValidationSources",
                "ValidationAuthorityID",
                "ValidationAuthorityEntityID"
            ],
            conflict_columns=["lei"],  # Unique constraint on 'lei'
            compare_columns=[
                "InitialRegistrationDate",
                "LastUpdateDate",
                "RegistrationStatus",
                "NextRenewalDate",
                "ManagingLOU",
                "ValidationSources",
                "ValidationAuthorityID",
                "ValidationAuthorityEntityID"
            ],
            set_clause="""
                InitialRegistrationDate = EXCLUDED.InitialRegistrationDate,
                LastUpdateDate = EXCLUDED.LastUpdateDate,
                RegistrationStatus = EXCLUDED.RegistrationStatus,
                NextRenewalDate = EXCLUDED.NextRenewalDate,
                ManagingLOU = EXCLUDED.ManagingLOU,
                ValidationSources = EXCLUDED.ValidationSources,
                ValidationAuthorityID = EXCLUDED.ValidationAuthorityID,
                ValidationAuthorityEntityID = EXCLUDED.ValidationAuthorityEntityID
            """,
            do_nothing=False  # Perform upsert with possible updates
        )
    
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

        self.bulk_upsert_using_copy(
            data=list_clean_extension_data_tuples,
            table_name="GLEIF_geocoding",
            columns=[
                "lei",
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
                "mapped_country"
            ],
            conflict_columns=["lei", "relevance", "match_type", "lat", "geocoding_date", "TopLeft_Latitude", "TopLeft_Longitude", "BottomRight_Latitude", "BottomRight_longitude", "match_level", "mapped_street", "mapped_housenumber", "mapped_postalcode", "mapped_city", "mapped_district", "mapped_state", "mapped_country"],  # Composite unique constraint
            do_nothing=True  # Perform INSERT ... ON CONFLICT DO NOTHING
        )
    
    def process_all_data(self , list_dict_records):
        self.process_entity_data(list_dict_records = list_dict_records)
        self.process_other_legal_names(list_dict_records = list_dict_records)
        self.process_legal_address(list_dict_records = list_dict_records)
        self.process_headquarters_address(list_dict_records = list_dict_records)
        self.process_legal_entity_events(list_dict_records = list_dict_records)
        self.process_registration_data(list_dict_records = list_dict_records)
        self.process_geoencoding_data(list_dict_records = list_dict_records)
    
    def storing_GLEIF_data_in_database(self):
        
        with open(self.str_json_file_path, 'r', encoding='utf-8') as file:
            dict_relationships = json.load(file)            
            self.process_all_data(list_dict_records = dict_relationships["records"])               
        self.conn.commit()
        
        self.conn.close()
    
if __name__ == "main":
    obj = GLEIFUpdateLevel1(bool_log = True)
    obj.storing_GLEIF_data_in_database()