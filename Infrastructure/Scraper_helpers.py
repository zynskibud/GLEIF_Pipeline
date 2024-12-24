import time 
import requests 
import speedtest
import numpy as np 
import os , logging

class Scraper_Helpers:
    def __init__(self , headers = None , gzip = False , example_url = None , local_logging = False , log = True , check_tor = False):
        """ 
        This constru
        @param: headers - passing headers since a lot of these functions require them, but it is optional.
        """
        self.headers = headers
        self.session_obj = requests.Session()
        self.log = log
        self.dict_sizes = {"bytes" : 1 , "kilobytes" : 1_000 , "megabytes" : 1_000_000 , "gigabytes" : 1_000_000_000} #conversion rates from bytes
       
        if check_tor:
            self.tor = self.check_tor(example_url)

        if local_logging:
            if not os.path.exists("logging"): #if the logging folder doesnt exist in the directory, make it
                os.makedirs("logging")
                
            logging.basicConfig(filename = "logging/Scraper_Helpers_Logs.log" , level = logging.DEBUG, format = '%(levelname)s: %(message)s' , filemode = "w")

        if not example_url:
            #want to implement it so that it automatically checks the download size of files to see if gzip is necessary.
            pass
            
        if gzip:
            headers["Accept-Encoding"] = 'gzip, deflate' #this allows gzip compression from the host if they support it which speeds up data transfer

    def accepted_status_codes(self):
        """ 
        These are all the status codes which results in an acceptance
        """
        return [200 , 201 , 202 , 203 , 204 , 205 , 206 , 207 , 208 , 226]

    def check_tor(self , url):
        """ 
        This function checks if a url will allow tor connections and if it does will return true, otherwise it will return false. 
        @param: url - this is the url to test

        @return: false if tor doesnt work/is blocked, true if it isnt. User must have tor downloaded in order for this to work
        """

        proxies = {'http': 'socks5h://127.0.0.1:9050', 'https': 'socks5h://127.0.0.1:9050'} #by default tor runs a socks 5 proxy on local host
        response = self.spotty_network(url , proxies = proxies)

        if response != -1: #if the request worked return true 
            return True
        else: #otherwise return false 
            return False
    
    def spotty_network(self , url , wait_time = 1 , number_retries = 2 , proxies = None , 
                       acceptable_status_codes = None , download_size = False , stream = False):
        """
        Specifically this needs to be used for cable internet. 

        This function is used when you are using a spotty network (one that goes out sometimes) this function handles the request you are trying to get
        For now this function will just be working off a timer that the user can enter and a retry amount also specified by the user, but for the future might want to add more 
        to this. 
        
        @param: url - the url we are trying to get request
        @param: wait_time - the amount of time to wait before making a new request 
        @param: number_retries - the amount of times to try to make a request with the wait time seperating each iteration
        @param: acceptable_status_codes - this is a list of acceptable status codes for the response object, this allows the user to only accept responses which fit the status code they are looking for 

        @return - either the json response or -1. If -1 is returned then no response was able to be recieved
        """
        attempt_num = 0
        while attempt_num != number_retries: 
            try:
                if proxies: #if the request needs to be done with headers and proxies
                    response = self.session_obj.get(url , headers = self.headers , proxies = proxies , stream = stream) 
                else: #if the request is with neither
                    response = self.session_obj.get(url , headers = self.headers , stream = stream)

                if acceptable_status_codes and response.status_code in acceptable_status_codes: #if the user did pass a status code list, make sure this is one of the ones they are looking for
                    if download_size:
                        self.get_obj_size(response)
                    else:
                        return response
                
                elif not acceptable_status_codes: #if the user didnt pass any status codes they want to recieve, just return whatever the response object is
                    if download_size:
                        self.get_obj_size(response)     
                    return response   
                
                elif self.log:
                    logging.error(f"status_code_Failure : {response.status_code} , attempt is {attempt_num + 1} , url: {url}")     
                    attempt_num += 1
                else:
                    attempt_num += 1
                    
            except requests.exceptions.RequestException as e:
                if self.log:
                    logging.error(f"Exception_Failure : exception was {e} , attempt is {attempt_num + 1}  , url: {url}")
                
                attempt_num += 1
                time.sleep(wait_time)
                continue
        
        return -1
    

    def get_obj_size(self , response_obj , size = "megabytes"):
        """ 
        This function gets the size in megabytes or gigabytes of a object
        @param: response_obj - the object to get the size of in bytes
        @param: size - valid strings are bytes, kilobytes , megabytes, gigabytes. This just controls the scale factor we apply to the total number of bytes
        
        @return: the total size in the desired unit of bytes the user asked for
        """
        total_size_bytes = 0
        for chunk in response_obj.iter_content(chunk_size = 8192):
            total_size_bytes += len(chunk)

        conversion_factor = self.dict_sizes[size]
        total_size_scaled = total_size_bytes / conversion_factor 

        if self.log:
            logging.info(f"Download Size : {total_size_scaled} {size}")
            
        return total_size_scaled
    
    def response_time(self , url , proxies = None , session = False):
        """
        This function will return the response time for a given server
        
        @param: url - this is the server we want to determine the response time for.
        @param: proxies - this is the proxies to be used, pass an empty dictionary if none
        @param: session - this is a flag to tell the program if it should use a session obj or just a request obj

        @return: the response time and the response. 
        """
        if not proxies:
            proxies = {}
        # Make the request to the URL and download the content, in microseconds. Note passing an empty
        #dictioanry for proxies will ensure that no global built in proxies are used. 
        if session: #using a session obj
            start_time = time.perf_counter()
            response = self.session_obj.get(url , headers = self.headers , proxies = proxies)
            end_time = time.perf_counter()

        else: #using a request obj
            start_time = time.perf_counter()
            response = requests.get(url , headers = self.headers , proxies = proxies)
            end_time = time.perf_counter()

        return end_time - start_time , response 

    def Network_check(self , lst_urls , average = False):
        """
        This function is meant to tell the user how many requests per second to a specific url
        their wifi can support so they can easily determine if their wifi will be a bottleneck. 

        Urls should all be from the same server. 
        """

        self.gzip = self.using_gzip(url = lst_urls[0]) #checking if we can use gzip. 
        network_speed_mb , file_size_mb , response_time = self.network_urls_metrics(lst_urls = lst_urls , average = average)
        time_to_download = file_size_mb / network_speed_mb #need to determine the time it takes to do a single download
        latency = response_time - time_to_download #calculating latency as the response time minus the time it takes to download

        #the network capacity for requests in a single second should be the the networks speed
        network_capacity_requests = 1 / (response_time) #for each second we need to divide by the time it takes to download plus the response time
        #server requests limit 

        #here we will calcualte the amount of requests the network can handle a second. Also we can calcualte the optimal number of threads here to by seeing how long 
        #each server request will actually take with our network and the latency. Will just have to factor in the os time to switch between threads. 
        return network_capacity_requests , latency , time_to_download , network_speed_mb , self.gzip

    def network_urls_metrics(self , lst_urls , average = False):
        """
        This function gets the average or median for each of the metrics we are looking for.
        @param: lst_urls - this is the list of urls the user wants to average or get the median of all from the same server
        @param: average - this is flag to tell the program to return the averages or medians depending on what the user wants 

        @return: avg or median download speed in mb , file size in mb, and response time in mb
        """  

        total_download_speed_mb , total_size_megabits , total_response_time = 0 , 0 , 0
        #going to want to get an average download speed and ping and then get an average file size
        if not average:
            np_median = np.empty(shape = (len(lst_urls) , 3))

        for index , url in enumerate(lst_urls):
            download_speed_mb , size_megabits , respnse_time = self.get_network_speed(url = url)
            total_download_speed_mb += download_speed_mb
            total_size_megabits += size_megabits
            total_response_time += respnse_time

            if not average:
                np_median[index , 0] = download_speed_mb
                np_median[index , 1] = size_megabits
                np_median[index , 2] = respnse_time

        if average:
            return total_download_speed_mb / len(lst_urls) , total_size_megabits / len(lst_urls) , total_response_time / len(lst_urls)
        else:
            #being more reserved taking the lower index for download speed, the higher index for file size and the higher index for response time.
            np_median_sorted = np.sort(np_median , axis = 0) #sorting all of the rows aka each set of data
            return np_median_sorted[int(np.floor(len(lst_urls)/2))][0] ,  np_median_sorted[int(np.ceil(len(lst_urls)/2))][1]  ,  np_median_sorted[int(np.ceil(len(lst_urls)/2))][2] 


    def using_gzip(self , url):
        """
        This function checks if the server is able to use gzip compression to respond to requests
        which would result in a speed boost if they do. 
        """

        # Make a request to the server
        response = requests.get(url , headers = self.headers)

        # Check if the server responded with gzip compression
        if 'gzip' in response.headers.get('Content-Encoding', ''):
            return True
        else:
            return False


    def get_network_speed(self , url = None):
        """
        This function gets the network speed for any device

        @return : the network download speed, upload speed (no url is provided), and ping in mbps.
        """

        # Get the best server based on ping to test the network
        if url:
            return self.network_test_url(url = url)
        else:
            return self.network_test_general()

    def network_test_general(self):
        """
        Allows network testing with the speed test library and one of their servers
        @return: the network download speed, upload speed , and ping in mbps.
        """
        obj_st = speedtest.Speedtest()

        obj_st.get_best_server() 

        # Measure download speed
        download_speed = obj_st.download() / 1_000_000  # Convert from bits/s to Mbps

        # Measure upload speed
        upload_speed = obj_st.upload() / 1_000_000  # Convert from bits/s to Mbps

        # Get ping latency
        ping = obj_st.results.ping

        return download_speed , upload_speed , ping

    def network_test_url(self , url):
        """
        Test the network speed on a specific url. 
        
        @param: url - this is the url we want to test the download speed and response time (latency) for

        @return: this function returns the download speed in megabites and the network response time
        """

        self.network_response_time , response = self.response_time(url = url)

        # Iterate over the response content in chunks, this works for all kinds of requests which is why we are using it
        total_size_megabytes = self.get_obj_size(response_obj = response)

        download_speed_mb = total_size_megabytes / self.network_response_time

        return download_speed_mb , total_size_megabytes  , self.network_response_time

    def request_limit(self , request_limit):
        """ 
        This function can be used to make sure we are abiding by a set request limit given by the website
        so that we dont get blocked or anything like that

        @param: request_limit - this is the requests per second we are limited to. 
        """
        pass


    def find_request_limit(self , url):
        """
        This function is meant to, given a url find out what number of requests causes a slow done. 

        @param: url - the url we are trying to find the request limit for
        """
        pass


    def multi_thread_network(self):
        """ 
        The goal of this function would be to multithread any network bottleneck in a program. Basically using 
        a bunch of proxies to greatly speed up the process. 
        """
        pass


    def optimal_num_threads(self , url):
        """
        This function will calculate the optimal number of threads based on the server response time,
        the speed of each threads proxy ips internet aka how many requests each ip makes a second, and the 
        server rate limit if there is one. 

        @param: url 
        """
        
        pass

    def Scraper_Report(self , url , network_speed , request_limit = None):
        """
        This function automatically analyzes the source of interest and tells the user the optimal way 
        of scraping this website telling them all the constraints and how to optimize. 
        """

        pass