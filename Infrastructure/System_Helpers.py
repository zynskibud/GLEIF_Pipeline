import os

class SystemHelpers:
    
    def delete_file_directory(self , str_file_path , bool_file = False , bool_directory = False):
        """
        Deletes a file at the given path.
        
        Parameters:
            file_path (str): The full path of the file to delete.
            
        Returns:
            str: A message indicating the result.
        """
        if bool_file == True:
            os.remove(str_file_path)
        
        if bool_directory == True:
            str_directory_path = str_file_path
            for str_file_name in os.listdir(str_directory_path):
                str_file_path = os.path.join(str_directory_path, str_file_name)
                os.remove(str_file_path)