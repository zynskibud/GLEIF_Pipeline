import os
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import zipfile
import os
from pathlib import Path
import time

class GLEIF_Update_Helpers:
    def __init__(self, bool_Level_1 = False, bool_Level_2_Trees = False, bool_Level_2_Reporting_Exceptions = False):
        self.bool_Level_1 = bool_Level_1
        self.bool_Level_2_Trees = bool_Level_2_Trees
        self.bool_Level_2_Reporting_Exceptions = bool_Level_2_Reporting_Exceptions
        

    def download_on_machine(self):
        """
        This function uses selenium to webscrape the download link for all Level 1 Data in the GLEIF database.
        
        @return: str_download_link - the link which is used to download the entire GLEIF level 1
        """

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)
        driver.get(url = "https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/")

            
        driver.get(url = "https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/")

        cookie_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CLASS_NAME, 'CybotCookiebotDialogBodyButton'))
        )

        cookie_button.click()

        download_buttons = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, 'dropdown-toggle'))
        )

        if self.bool_Level_1 == True:
            download_buttons[0].click()
        if self.bool_Level_2_Trees == True:
            download_buttons[1].click()
        if self.bool_Level_2_Reporting_Exceptions == True:
            download_buttons[2].click()

        dropdown_menu = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'dropdown-menu'))
        )

        # Locate the specific "Eight hours earlier" option using XPath
        eight_hours_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//ul[@class="dropdown-menu"]/li[contains(., "Eight hours earlier.")]')
            )
        )

        eight_hours_option.click()

        download_buttons_2 = WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, 'dropdown-toggle'))
        )

        if self.bool_Level_1 == True:
            download_buttons_2[1].click()
        if self.bool_Level_2_Trees == True:
            download_buttons_2[2].click()
        if self.bool_Level_2_Reporting_Exceptions == True:
            download_buttons_2[3].click()


        # Wait for the format dropdown parent container

        dropdown_menu_2 = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'dropdown-menu'))
        )

        format_options = dropdown_menu_2.find_elements(By.TAG_NAME, 'li')

        for option in format_options:
            if "JSON" in option.text:
                option.click()
                break
        
        time.sleep(15)
        
        
    def find_default_download_folder_and_list_files(self):
        """
        Finds the default download folder for the current operating system and lists the files
        in that folder, sorted by most recent modification time.
        
        @return: List of files sorted by modification time (most recent first).
        """
        # Detect the default download folder
        if os.name == "nt":  # Windows
            download_folder = os.path.join(os.environ['USERPROFILE'], "Downloads")
        elif os.name == "posix":  # macOS and Linux
            download_folder = os.path.join(Path.home(), "Downloads")
        
        """# Verify the folder exists
        if not os.path.exists(download_folder):
            raise FileNotFoundError(f"Download folder not found at {download_folder}")"""
        
        # Get a list of all files in the download folder
        list_files = [os.path.join(download_folder, file) for file in os.listdir(download_folder)]
        list_files = [file for file in list_files if os.path.isfile(file)]  # Filter out directories

        # Sort files by modification time (most recent first)
        list_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

        return list_files
    
    def unpacking_GLEIF_zip_files(self):
        
        str_zip_file_path = (self.find_default_download_folder_and_list_files())[0]
        
        if self.bool_Level_1 == True:
            str_extract_to_directory = rf"../file_lib/Level_1_update_unpacked" # Replace with your desired extraction directory
        if self.bool_Level_2_Trees == True:
            str_extract_to_directory = rf"../file_lib/Level_2_update_unpacked" # Replace with your desired extraction directory
        if self.bool_Level_2_Reporting_Exceptions == True:
            str_extract_to_directory = rf"../file_lib/Exceptions_update_unpacked" # Replace with your desired extraction directory

        # Extract the zip file
        with zipfile.ZipFile(str_zip_file_path, 'r') as zip_ref:
            file_names = zip_ref.namelist()  # Get the list of files in the archive
            if file_names:  # Ensure the archive is not empty
                first_file_name = file_names[0]  # Grab the first file name
                
            # Extract all files to the specified directory
            zip_ref.extractall(str_extract_to_directory)
            
            # Get the full path of the first file
            str_json_file_path = os.path.join(str_extract_to_directory, first_file_name)
            
        return str_json_file_path