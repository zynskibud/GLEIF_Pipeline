import os
import logging
import json
import json
import psycopg2
import sys
import io
from datetime import datetime, timezone
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, "..", ".."))
sys.path.append(target_directory)

from Production.Update import GLEIF_Update_Helpers
from Production.Backfill import GLEIF_Backfill_Helpers

class GLEIFUpdateLevel2:
    def __init__(self , bool_log = True , str_db_name = "GLEIF_test_db" , bool_downloaded = True):
        self.obj_update_helpers = GLEIF_Update_Helpers.GLEIF_Update_Helpers(bool_Level_2_Trees = True)
        self.obj_backfill_helpers = GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers(bool_Level_2_Trees = True)
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
        else:   
            current_date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            current_interval = self.obj_update_helpers.get_current_interval()
            self.str_json_file_path = f'../file_lib/Level_2_update_unpacked\\{current_date_str}-{current_interval}-gleif-goldencopy-rr-intra-day.json'
        self.conn = psycopg2.connect(dbname = str_db_name, user="Matthew_Pisinski", password="matt1", host="localhost", port="5432")    
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

        # Commit the transaction
        self.conn.commit()
    
    
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
        
    def remove_duplicates_keep_order(self , input_list):
        seen = set()
        output_list = []
        for item in input_list:
            if item not in seen:
                output_list.append(item)
                seen.add(item)
        return output_list
    
    def process_meta_data(self , dict_relationships):
        list_tuples_relationships = []

        for dict_relationship in dict_relationships:
            dict_relationship_flattened = self.obj_backfill_helpers.flatten_dict(dict_input = dict_relationship)
            list_output = self.obj_backfill_helpers.get_target_values(dict_data = dict_relationship_flattened, subset_string = True, target_keys = ["StartNode" , "EndNode" , "RelationshipType" , "RelationshipStatus" , "RegistrationStatus" , "InitialRegistrationDate" , "LastUpdateDate" , "NextRenewalDate" , "ManagingLOU" , "ValidationSources" , "ValidationDocuments" , "ValidationReference"])
            list_clean_output = self.clean_url(list_input = list_output)
            list_tuples_relationships.append(tuple(list_clean_output))
        
        list_clean_tuples_relationships = list(set(list_tuples_relationships))
        
        self.bulk_upsert_using_copy(table_name = "GLEIF_relationship_data" , 
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
                                                    ],
                                            conflict_columns = ["StartNode", "EndNode", "RelationshipType"],
                                            set_clause = """
                                                    RelationshipStatus = EXCLUDED.RelationshipStatus,
                                                    RegistrationStatus = EXCLUDED.RegistrationStatus,
                                                    InitialRegistrationDate = EXCLUDED.InitialRegistrationDate,
                                                    LastUpdateDate = EXCLUDED.LastUpdateDate,
                                                    NextRenewalDate = EXCLUDED.NextRenewalDate,
                                                    ManagingLOU = EXCLUDED.ManagingLOU,
                                                    ValidationSources = EXCLUDED.ValidationSources,
                                                    ValidationDocuments = EXCLUDED.ValidationDocuments,
                                                    ValidationReference = EXCLUDED.ValidationReference
                                                """,
                                            compare_columns = [
                                                    "RelationshipStatus",
                                                    "RegistrationStatus",
                                                    "InitialRegistrationDate",
                                                    "LastUpdateDate",
                                                    "NextRenewalDate",
                                                    "ManagingLOU",
                                                    "ValidationSources",
                                                    "ValidationDocuments",
                                                    "ValidationReference"
                                                ],
                                            do_nothing = False
                                            )      
        
    def process_relationships_date_data(self , dict_relationships):
        list_relationship_date_data = []
        
        for dict_relationship in dict_relationships:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_input = dict_relationship)
            list_tuples_dates = self.obj_backfill_helpers.extract_event_data(dict_data = dict_flat , base_keyword = "RelationshipPeriods_RelationshipPeriod" , target_keys = ["StartDate" , "EndDate" , "PeriodType"])
            
            list_unique_keys = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat, subset_string = True, target_keys = ["StartNode" , "EndNode" , "RelationshipType"])
            
            list_tuples_with_keys = [(*list_unique_keys, *tup) for tup in list_tuples_dates]
            
            # Append the result to the main list
            list_relationship_date_data.extend(list_tuples_with_keys)
        
        list_clean_relationship_date_data = self.remove_duplicates_keep_order(list_relationship_date_data)

        
        self.bulk_upsert_using_copy(table_name = "GLEIF_relationship_date_data" , 
                                    data = list_clean_relationship_date_data , 
                                    columns = [
                                        'StartNode',
                                        'EndNode',
                                        'RelationshipType',                                        
                                        "StartDate",
                                        "EndDate",
                                        "PeriodType"],
                                        conflict_columns = ["StartNode", "EndNode", "RelationshipType", "StartDate", "PeriodType"],
                                        set_clause = """
                                            EndDate = EXCLUDED.EndDate
                                        """,
                                        compare_columns = [
                                                "EndDate",
                                            ],
                                        do_nothing = False
                                        )
                
    def process_relationships_qualifiers(self , dict_relationships):
        list_qualifier_data = []
            
        for dict_relationship in dict_relationships:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_input = dict_relationship)
            list_tuples_qualifiers = self.obj_backfill_helpers.extract_event_data(dict_data = dict_flat , base_keyword = "RelationshipQualifiers_RelationshipQualifier" , target_keys = ["QualifierDimension" , "QualifierCategory"])
            
            list_unique_keys = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat, subset_string = True, target_keys = ["StartNode" , "EndNode" , "RelationshipType"])
            
            list_tuples_with_keys = [(*list_unique_keys, *tup) for tup in list_tuples_qualifiers]
            
            # Append the result to the main list
            list_qualifier_data.extend(list_tuples_with_keys)
        
        list_clean_qualifier_data = self.remove_duplicates_keep_order(list_qualifier_data)
        
        self.bulk_upsert_using_copy(table_name = "GLEIF_relationship_qualifiers" , data = list_clean_qualifier_data , 
                                    columns = [
                                        'StartNode',
                                        'EndNode',
                                        'RelationshipType',
                                        "QualifierDimension",                
                                        "QualifierCategory"
                                    ],
                                    conflict_columns = ["StartNode", "EndNode", "RelationshipType", "QualifierDimension", "QualifierCategory"],
                                    do_nothing = True
                                    )
    
    def process_relationships_quantifiers(self , dict_relationships):
        list_quantifier_data = []
            
        for dict_relationship in dict_relationships:
            dict_flat = self.obj_backfill_helpers.flatten_dict(dict_input = dict_relationship)
            list_tuples_quantifiers = self.obj_backfill_helpers.extract_event_data(dict_data = dict_flat , base_keyword = "Relationship_RelationshipQuantifiers" , target_keys = ["MeasurementMethod" , "QuantifierAmount" , "QuantifierUnits"])
            
            list_unique_keys = self.obj_backfill_helpers.get_target_values(dict_data = dict_flat, subset_string = True, target_keys = ["StartNode" , "EndNode" , "RelationshipType"])
            
            list_tuples_with_keys = [(*list_unique_keys, *tup) for tup in list_tuples_quantifiers]
            
            # Append the result to the main list
            list_quantifier_data.extend(list_tuples_with_keys)
            
        list_clean_quantifier_data = self.remove_duplicates_keep_order(list_quantifier_data)
        
        self.bulk_upsert_using_copy(table_name = "GLEIF_relationship_quantifiers" , data = list_clean_quantifier_data , 
                                    columns = [
                                        'StartNode',
                                        'EndNode',
                                        'RelationshipType',
                                        "MeasurementMethod",                
                                        "QuantifierAmount",
                                        "QuantifierUnits",
                                    ],
                                    conflict_columns = ["StartNode", "EndNode", "RelationshipType", "MeasurementMethod", "QuantifierAmount", "QuantifierUnits"],
                                    do_nothing = True
                                    )    
    
    def process_relationships(self , dict_relationships):

        self.process_meta_data(dict_relationships = dict_relationships)
        
        self.process_relationships_date_data(dict_relationships = dict_relationships)
        
        self.process_relationships_qualifiers(dict_relationships = dict_relationships)
        
        self.process_relationships_quantifiers(dict_relationships = dict_relationships)
        

            
    def updating_GLEIF_data_in_database(self):
        
        with open(self.str_json_file_path, 'r', encoding='utf-8') as file:
            dict_relationships = json.load(file)            
            self.process_relationships(dict_relationships = dict_relationships["relations"])               
        
        self.conn.commit()
        
        self.obj_backfill_helpers.file_tracker(file_path = self.str_json_file_path , str_db_name = "GLEIF_test_db" , str_data_title = "Level_2_relationships")
        
        self.conn.close()
        
if __name__ == "main":
    obj = GLEIFUpdateLevel2()
    obj.updating_GLEIF_data_in_database()