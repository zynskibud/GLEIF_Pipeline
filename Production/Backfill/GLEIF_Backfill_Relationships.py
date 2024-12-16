import os

import logging
import json
import psycopg2
import io
import sys
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, "..", ".."))
sys.path.append(target_directory)

from Production.Backfill import GLEIF_Backfill_Helpers

class GLEIFLevel2Data:
    def __init__(self , bool_log = True , str_db_name = "GLEIF_test_db" , bool_downloaded = True):
        self.obj_backfill_helpers = GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers(bool_Level_2_Trees = True)
        if bool_log:
            logging_folder = "../logging"  # Adjust the folder path as necessary
    
            if os.path.exists(logging_folder):
                if not os.path.isdir(logging_folder):
                    raise FileExistsError(f"'{logging_folder}' exists but is not a directory. Please remove or rename the file.")
            else:
                os.makedirs(logging_folder)
    
            logging.basicConfig(filename=f"{logging_folder}/GLEIF_Backfill_level_2.log", level=logging.DEBUG, format='%(levelname)s: %(message)s', filemode="w")

        if not bool_downloaded:
            if not os.path.exists("../file_lib"):
                os.makedirs("../file_lib")
                
            str_level_2_download_link = self.obj_backfill_helpers.get_level_download_links()
            self.str_json_file_path = self.obj_backfill_helpers.unpacking_GLEIF_zip_files(str_download_link = str_level_2_download_link , str_unpacked_zip_file_path_name = "Level_2_unpacked" , str_zip_file_path_name = "Level_2.zip")
    
        str_unpacked_zip_file_name = os.listdir(rf"../file_lib/Level_2_unpacked")[0]
        self.str_json_file_path = rf"../file_lib/Level_2_unpacked" + "//" + str_unpacked_zip_file_name
        self.conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
    
    def create_table(self):
        self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_relationship_data (
                id SERIAL PRIMARY KEY,
                StartNode TEXT NOT NULL,
                EndNode TEXT NOT NULL,
                RelationshipType TEXT NOT NULL,
                RelationshipStatus TEXT,
                RegistrationStatus TEXT,
                InitialRegistrationDate TEXT,
                LastUpdateDate TEXT,
                NextRenewalDate TEXT,
                ManagingLOU TEXT,
                ValidationSources TEXT,
                ValidationDocuments TEXT,
                ValidationReference TEXT,
                UNIQUE (StartNode, EndNode, RelationshipType)
                );
            """)
            
        self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_relationship_date_data (
                id SERIAL PRIMARY KEY,
                StartNode TEXT NOT NULL,
                EndNode TEXT NOT NULL,
                RelationshipType TEXT NOT NULL,
                StartDate TEXT,
                EndDate TEXT,
                PeriodType TEXT,
                FOREIGN KEY (StartNode, EndNode, RelationshipType) 
                    REFERENCES GLEIF_relationship_data(StartNode, EndNode, RelationshipType),
                UNIQUE (StartNode, EndNode, RelationshipType, StartDate, PeriodType)
                );
            """)
        
        self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_relationship_qualifiers (
                id SERIAL PRIMARY KEY,
                StartNode TEXT NOT NULL,
                EndNode TEXT NOT NULL,
                RelationshipType TEXT NOT NULL,                
                QualifierDimension TEXT,                
                QualifierCategory TEXT,
                FOREIGN KEY (StartNode, EndNode, RelationshipType) 
                    REFERENCES GLEIF_relationship_data(StartNode, EndNode, RelationshipType),
                UNIQUE (StartNode, EndNode, RelationshipType , QualifierDimension , QualifierCategory)                
                );
            """)
        
        self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS GLEIF_relationship_quantifiers (
                id SERIAL PRIMARY KEY,
                StartNode TEXT NOT NULL,
                EndNode TEXT NOT NULL,
                RelationshipType TEXT NOT NULL,               
                MeasurementMethod TEXT,                
                QuantifierAmount TEXT,
                QuantifierUnits TEXT,
                FOREIGN KEY (StartNode, EndNode, RelationshipType) 
                    REFERENCES GLEIF_relationship_data(StartNode, EndNode, RelationshipType),
                UNIQUE (StartNode, EndNode, RelationshipType, MeasurementMethod, QuantifierAmount, QuantifierUnits)                
                );
            """)
        
        self.conn.commit()
    
    def drop_table(self , lst_table_names):
            """
            Drops a specific table from the database securely.
            
            Parameters:
                table_name (list of string): The names of the tables to drop.
            """

            for table_name in lst_table_names:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                
            self.conn.commit()
    
    def bulk_insert_using_copy(self , table_name , columns, data):
        """Perform a bulk insert using PostgreSQL COPY with an in-memory buffer

        Args:
            table_name (_type_): Name of the table to insert into
            columns (_type_): List of column names for the table
            data (_type_): List of tuples with the data to be inserted
        """
        
        buffer = io.StringIO()
        
        #write data to the buffer
        
        for row in data:
            buffer.write('\t'.join(map(str , row)) + "\n")
        buffer.seek(0) #reset buffer position to the beginning
        
        #Construct the copy query
        copy_query = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH DELIMITER '\t'"
        self.cursor.copy_expert(copy_query , buffer)
        self.conn.commit
    
    
    def clean_string(self , value):
        if value:
            # Replace specific problematic sequences
            value = value.replace('\\00', '').replace('\\09', '')
            # Replace backslashes with forward slashes or keep as needed
            value = value.replace('\\', '/')
            return value.strip()
        return None
        
    def clean_url(self, list_input):
        """Clean the ValidationReference field (index 11)."""
        if list_input[11]:
            original_value = list_input[11]
            list_input[11] = self.clean_string(list_input[11])
            logging.debug(f"Original: {original_value}, Cleaned: {list_input[11]}")
        else:
            list_input[11] = None
        return list_input
        
    def process_meta_data(self , dict_relationships):
        list_tuples_relationships = []

        for dict_relationship in dict_relationships:
            dict_relationship_flattened = self.obj_backfill_helpers.flatten_dict(dict_input = dict_relationship)
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_relationship_flattened, subset_string = True, target_keys = ["StartNode" , "EndNode" , "RelationshipType" , "RelationshipStatus" , "RegistrationStatus" , "InitialRegistrationDate" , "LastUpdateDate" , "NextRenewalDate" , "ManagingLOU" , "ValidationSources" , "ValidationDocuments" , "ValidationReference"])
            list_clean_output = self.clean_url(list_input = list_output)
            list_tuples_relationships.append(tuple(list_clean_output))
        
        list_clean_tuples_relationships = list(set(list_tuples_relationships))
        
        self.bulk_insert_using_copy(table_name = "GLEIF_relationship_data" , 
                                            data = list_clean_tuples_relationships , 
                                            columns = [
                                                        'StartNode',
                                                        'EndNode',
                                                        'RelationshipType',
                                                        'RelationshipStatus',
                                                        'RegistrationStatus',
                                                        'InitialRegistrationDate',
                                                        'LastUpdateDate',
                                                        'NextRenewalDate',
                                                        'ManagingLOU',
                                                        'ValidationSources',
                                                        'ValidationDocuments',
                                                        'ValidationReference'
                                                    ])      
        
    def process_relationships_date_data(self , dict_relationships):
        list_relationship_date_data = []
        
        for dict_relationship in dict_relationships:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_input = dict_relationship)
            list_tuples_dates = self.obj_backfill_helpers.extract_event_data(dict_data = dict_flat , base_keyword = "RelationshipPeriods_RelationshipPeriod" , target_keys = ["StartDate" , "EndDate" , "PeriodType"])
            
            list_unique_keys = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat, subset_string = True, target_keys = ["StartNode" , "EndNode" , "RelationshipType"])
            
            list_tuples_with_keys = [(*list_unique_keys, *tup) for tup in list_tuples_dates]
            
            # Append the result to the main list
            list_relationship_date_data.extend(list_tuples_with_keys)
        
        list_clean_relationship_date_data = list(set(list_relationship_date_data))

        
        self.bulk_insert_using_copy(table_name = "GLEIF_relationship_date_data" , 
                                    data = list_clean_relationship_date_data , 
                                    columns = [
                                        'StartNode',
                                        'EndNode',
                                        'RelationshipType',                                        
                                        "StartDate",
                                        "EndDate",
                                        "PeriodType"])
                
    def process_relationships_qualifiers(self , dict_relationships):
        list_qualifier_data = []
            
        for dict_relationship in dict_relationships:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_input = dict_relationship)
            list_tuples_qualifiers = self.obj_backfill_helpers.extract_event_data(dict_data = dict_flat , base_keyword = "RelationshipQualifiers_RelationshipQualifier" , target_keys = ["QualifierDimension" , "QualifierCategory"])
            
            list_unique_keys = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat, subset_string = True, target_keys = ["StartNode" , "EndNode" , "RelationshipType"])
            
            list_tuples_with_keys = [(*list_unique_keys, *tup) for tup in list_tuples_qualifiers]
            
            # Append the result to the main list
            list_qualifier_data.extend(list_tuples_with_keys)
        
        list_clean_qualifier_data = list(set(list_qualifier_data))
        
        self.bulk_insert_using_copy(table_name = "GLEIF_relationship_qualifiers" , data = list_clean_qualifier_data , 
                                    columns = [
                                        'StartNode',
                                        'EndNode',
                                        'RelationshipType',
                                        "QualifierDimension",                
                                        "QualifierCategory"
                                    ])
    
    def process_relationships_quantifiers(self , dict_relationships):
        list_quantifier_data = []
            
        for dict_relationship in dict_relationships:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_input = dict_relationship)
            list_tuples_quantifiers = self.obj_backfill_helpers.extract_event_data(dict_data = dict_flat , base_keyword = "Relationship_RelationshipQuantifiers" , target_keys = ["MeasurementMethod" , "QuantifierAmount" , "QuantifierUnits"])
            
            list_unique_keys = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat, subset_string = True, target_keys = ["StartNode" , "EndNode" , "RelationshipType"])
            
            list_tuples_with_keys = [(*list_unique_keys, *tup) for tup in list_tuples_quantifiers]
            
            # Append the result to the main list
            list_quantifier_data.extend(list_tuples_with_keys)
            
        list_clean_quantifier_data = list(set(list_quantifier_data))
        
        self.bulk_insert_using_copy(table_name = "GLEIF_relationship_quantifiers" , data = list_clean_quantifier_data , 
                                    columns = [
                                        'StartNode',
                                        'EndNode',
                                        'RelationshipType',
                                        "MeasurementMethod",                
                                        "QuantifierAmount",
                                        "QuantifierUnits",
                                    ])    
    
    def process_relationships(self , dict_relationships):

        self.process_meta_data(dict_relationships = dict_relationships)
        
        self.process_relationships_date_data(dict_relationships = dict_relationships)
        
        self.process_relationships_qualifiers(dict_relationships = dict_relationships)
        
        self.process_relationships_quantifiers(dict_relationships = dict_relationships)
        
            
    def storing_GLEIF_data_in_database(self):
        
        self.create_table()
        
        with open(self.str_json_file_path, 'r', encoding='utf-8') as file:
            dict_relationships = json.load(file)            
            self.process_relationships(dict_relationships = dict_relationships["relations"])               
        
        self.conn.close()
        
if __name__ == "main":
    obj = GLEIFLevel2Data()
    obj.storing_GLEIF_data_in_database()