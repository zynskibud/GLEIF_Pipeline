import sys
import os
import random
import string
import pytest
current_directory = os.getcwd()
target_directory = os.path.abspath(os.path.join(current_directory, "..", ".."))
sys.path.append(target_directory)

from Production.Backfill import GLEIF_Backfill_Helpers


class TestingBackfillHelpers:
    
    @pytest.fixture
    def obj_backfill_helpers_level_1(self):
        return GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers(bool_Level_1 = True)
    
    @pytest.fixture
    def obj_backfill_helpers_relationships(self):
        return GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers(bool_Level_2_Trees = True)
    
    @pytest.fixture   
    def obj_backfill_helpers_exceptions(self):
        return GLEIF_Backfill_Helpers.GLEIF_Backill_Helpers(bool_Level_2_Reporting_Exceptions = True)
        
    def random_key(self , length=5):
        return ''.join(random.choices(string.ascii_lowercase, k=length))

    def random_value(self):
        # Random scalar value
        if random.random() < 0.5:
            return random.randint(0, 1000)
        return ''.join(random.choices(string.ascii_letters, k=8))

    def generate_nested_dict(self , depth=3, width=3):
        """
        Recursively generate a nested dictionary.
        """
        if depth == 0:
            # Return a scalar
            return self.random_value()

        d = {}
        for _ in range(random.randint(1, width)):
            # Sometimes nest, sometimes scalar
            if random.random() < 0.5:
                d[self.random_key()] = self.generate_nested_dict(depth - 1, width)
            else:
                # Make a list or scalar
                if random.random() < 0.5:
                    d[self.random_key()] = [self.generate_nested_dict(depth - 1, width) for __ in range(random.randint(1,3))]
                else:
                    d[self.random_key()] = self.random_value()

        return d

    def count_leaf_nodes(self, d):
        """
        Count the total number of leaf nodes in a nested structure of dicts/lists.
        """
        if isinstance(d, dict):
            return sum(self.count_leaf_nodes(v) for v in d.values())
        elif isinstance(d, list):
            return sum(self.count_leaf_nodes(i) for i in d)
        else:
            # Scalar
            return 1
    
    def generate_dirty_keys_dict(self , num_keys=10):
        d = {}
        for _ in range(num_keys):
            base_key = self.random_key()
            # Randomly decide to make it dirty
            if random.random() < 0.5:
                # Add "_$"
                key = base_key + "_$"
            else:
                # Insert "@xml:lang" somewhere
                key = base_key.replace('_','',1) + "@xml:lang"
            d[key] = self.random_value()

        # Add some clean keys as well
        for _ in range(3):
            d[self.random_key()] = self.random_value()

        return d

    def generate_other_entity_names_dict(self , num=3):
        # Create OtherEntityNames keys
        d = {}
        for i in range(1, num+1):
            d[f"OtherEntityNames_{i}_@type"] = f"Type{i}"
            d[f"OtherEntityNames_{i}"] = f"Name{i}"

        # Create TranslatedOtherEntityNames keys
        for i in range(1, num+1):
            d[f"TranslatedOtherEntityNames_{i}_@type"] = f"TransType{i}"
            d[f"TranslatedOtherEntityNames_{i}"] = f"TransName{i}"

        return d
    
    def generate_prefix_dict(self , num_prefixes=3, keys_per_prefix=4):
        prefixes = [self.random_key() for _ in range(num_prefixes)]
        d = {}
        for p in prefixes:
            for _ in range(keys_per_prefix):
                d[p + "_" + self.random_key()] = self.random_value()
        return d, prefixes
    
    def generate_event_data_dict(self , common_base="Mike", names=["Jerry","Allen","Jared"], index=1):
        d = {}
        for n in names:
            # Example key: Mike_1_{n}
            key = f"{common_base}_{index}_{n}"
            d[key] = f"Value_for_{n}"
        # Add some unrelated keys
        d["RandomKey"] = "RandomValue"
        return d, common_base, names
    
    def generate_exact_keys_dict(self):
        d = {
            "Nico": "NicoValue",
            "Matt": "MattValue",
            "Gradient": "GradientValue",
            # Add extras
            "Other": "OtherValue"
        }
        return d

    def generate_substring_keys_dict(self):
        d = {
            "SomeLongPath_Nico_end": "NicoValue",
            "X_Matt_Y": "MattValue",
            "Gradient_Something": "GradientValue",
            "OtherKey": "OtherValue"
        }
        return d
    
    def generate_split_dict(self , num_entities=3):
        d = {}
        for i in range(1, num_entities+1):
            d[f"Entity_{i}_Name"] = f"Name{i}"
            d[f"Entity_{i}_Type"] = f"Type{i}"
        # Add a non-matching key
        d["NoNumber_Key"] = "NoNumberValue"
        return d, num_entities
    
    def generate_geocoding_dict(self):
        # Possible corner descriptors and coordinate types
        corners = ["TopLeft", "TopRight", "BottomLeft", "BottomRight", "Center"]
        coord_types = ["Latitude", "Longitude"]

        # Create all possible (corner, coordinate) pairs
        all_pairs = [(c, ct) for c in corners for ct in coord_types]

        # Decide how many key-value pairs in bounding box (at least 2, at most number of all_pairs)
        num_pairs = random.randint(2, len(all_pairs))

        # Select unique pairs without replacement
        selected_pairs = random.sample(all_pairs, num_pairs)

        bbox_pairs = []
        for corner, ctype in selected_pairs:
            # Random latitude/longitude values in a plausible range
            val = round(random.uniform(-180, 180), 4)
            bbox_pairs.append(f"{corner}.{ctype}: {val}")

        bounding_box_value = ", ".join(bbox_pairs)

        # Create a dictionary with bounding_box and some other random keys
        d = {
            "bounding_box": bounding_box_value,
            "SomeOtherKey": "SomeValue"
        }

        # Return the dictionary and the count of pairs to verify after flattening
        return d, num_pairs
        
    #Testing the retrieval of data from data source for all three cases
    def helper_test_get_level_download_links_level_1(self , obj_backfill_helpers_level_1):
        str_download_link = obj_backfill_helpers_level_1.get_level_download_links()
        
        if not str_download_link:
            assert False
    
    def helper_test_get_level_download_links_relationships(self , obj_backfill_helpers_relationships):
        str_download_link = obj_backfill_helpers_relationships.get_level_download_links()
        
        if not str_download_link:
            assert False

    def helper_test_get_level_download_links_exceptions(self , obj_backfill_helpers_exceptions):
        str_download_link = obj_backfill_helpers_exceptions.get_level_download_links()
        
        if not str_download_link:
            assert False 
    
    def helper_test_unpacking_GLEIF_zip_files_level_1(self , obj_backfill_helpers_level_1):
        str_json_file_path = obj_backfill_helpers_level_1.unpacking_GLEIF_zip_files()
        
        if not str_json_file_path:
            assert False
    
    def helper_test_unpacking_GLEIF_zip_files_relationships(self , obj_backfill_helpers_relationships):
        str_json_file_path = obj_backfill_helpers_relationships.unpacking_GLEIF_zip_files()
        
        if not str_json_file_path:
            assert False
    
    def helper_test_unpacking_GLEIF_zip_files_exceptions(self , obj_backfill_helpers_exceptions):
        str_json_file_path = obj_backfill_helpers_exceptions.unpacking_GLEIF_zip_files()
        
        if not str_json_file_path:
            assert False
    
    # Testing the helper functions used to process all of the data in the rest of the backfill part of the pipeline
    def helper_test_flatten_dict(self , obj):
        input_dict = self.generate_nested_dict(depth=3, width=3)
        expected_leaf_count = self.count_leaf_nodes(input_dict)
        flattened = obj.flatten_dict(input_dict)
        if len(flattened) != expected_leaf_count:
            assert False
            
    def helper_test_clean_keys(self, obj):
        input_dict = self.generate_dirty_keys_dict()
        cleaned = obj.clean_keys(input_dict)
        
        # Check if any dirty patterns remain
        for k in cleaned.keys():
            if "_$" in k or "@xml:lang" in k:
                assert False
                
    def helper_test_organize_by_prefix(self , obj):
        input_dict, prefixes = self.generate_prefix_dict()
        organized = obj.organize_by_prefix(input_dict)
        if len(organized) != len(prefixes):
            assert False
            
    def helper_test_extract_other_entity_names(self , obj):
        base_keyword = "OtherEntityNames"
        exclude_keywords = ["TranslatedOtherEntityNames"]
        input_dict = self.generate_other_entity_names_dict()
        result = obj.extract_other_entity_names(input_dict, base_keyword, exclude_keywords)

        # Check that none of the TranslatedOtherEntityNames values are included
        # TranslatedOtherEntityNames were: TransType1, TransName1, etc.
        for t_i in range(1,4):
            if any(f"TransName{t_i}" in r for r in result) or any(f"TransType{t_i}" in r for r in result):
                assert False
                
    def helper_test_extract_event_data(self , obj):
        # target keys we look for
        target_keys = ["Jerry", "Allen", "Jared"]
        input_dict, base_keyword, names = self.generate_event_data_dict()
        # Run extraction
        result = obj.extract_event_data(input_dict, base_keyword, target_keys)

        # result should be a list of tuples, each tuple has values for (Jerry, Allen, Jared)
        # Check if we got the correct values
        if not result:  # Should not be empty
            assert False
        # Just check first tuple
        first_tuple = result[0]
        # Compare each name
        expected = tuple(f"Value_for_{n}" for n in names)
        if first_tuple != expected:
            assert False  
            
    def helper_test_get_target_values_exact(self , obj):
        input_dict = self.generate_exact_keys_dict()
        target_keys = ["Nico", "Matt", "Gradient"]
        result = obj.get_target_values(input_dict, target_keys, subset_string=False)
        expected = ["NicoValue", "MattValue", "GradientValue"]
        if result != expected:
            assert False

    def helper_test_get_target_values_subset(self , obj):
        input_dict = self.generate_substring_keys_dict()
        target_keys = ["Nico", "Matt", "Gradient"]
        result = obj.get_target_values(input_dict, target_keys, subset_string=True)
        expected = ["NicoValue", "MattValue", "GradientValue"]
        if result != expected:
            assert False
            
    def helper_test_split_into_list_of_dictionaries(self , obj):
        input_dict, num_entities = self.generate_split_dict()
        result = obj.split_into_list_of_dictionaries(input_dict)

        # The length of result should match num_entities
        if len(result) != num_entities:
            assert False
            
    def helper_test_further_flatten_geocoding(self , obj):
        input_dict, num_pairs = self.generate_geocoding_dict()
        result = obj.further_flatten_geocoding(input_dict)
        # Expected keys:
        # - bounding_box is replaced by one key per pair in bbox
        # - plus the original non-bounding_box keys (in this case, 1: "SomeOtherKey")
        expected_total_keys = num_pairs + 1

        if len(result) != expected_total_keys:
            assert False
    
    @pytest.mark.skip          
    def testing_GLIEF_Backfill_helpers_data_retrieval(self , obj_backfill_helpers_level_1 , obj_backfill_helpers_relationships , obj_backfill_helpers_exceptions):
        self.helper_test_get_level_download_links_level_1(obj_backfill_helpers_level_1 = obj_backfill_helpers_level_1)
        self.helper_test_get_level_download_links_relationships(obj_backfill_helpers_relationships = obj_backfill_helpers_relationships)
        self.helper_test_get_level_download_links_exceptions(obj_backfill_helpers_exceptions = obj_backfill_helpers_exceptions)
        self.helper_test_unpacking_GLEIF_zip_files_level_1(obj_backfill_helpers_level_1 = obj_backfill_helpers_level_1)
        self.helper_test_unpacking_GLEIF_zip_files_relationships(obj_backfill_helpers_relationships = obj_backfill_helpers_relationships)
        self.helper_test_unpacking_GLEIF_zip_files_exceptions(obj_backfill_helpers_exceptions = obj_backfill_helpers_exceptions)
    
    def testing_GLIEF_Backfill_helpers_processing_helpers(self , obj_backfill_helpers_level_1):
        self.helper_test_flatten_dict(obj = obj_backfill_helpers_level_1)
        self.helper_test_clean_keys(obj = obj_backfill_helpers_level_1)
        self.helper_test_organize_by_prefix(obj = obj_backfill_helpers_level_1)
        self.helper_test_organize_by_prefix(obj = obj_backfill_helpers_level_1)
        self.helper_test_extract_other_entity_names(obj = obj_backfill_helpers_level_1)
        self.helper_test_extract_event_data(obj = obj_backfill_helpers_level_1)
        self.helper_test_get_target_values_exact(obj = obj_backfill_helpers_level_1)
        self.helper_test_get_target_values_subset(obj = obj_backfill_helpers_level_1)
        self.helper_test_split_into_list_of_dictionaries(obj = obj_backfill_helpers_level_1)
        self.helper_test_further_flatten_geocoding(obj = obj_backfill_helpers_level_1)
        
        assert True