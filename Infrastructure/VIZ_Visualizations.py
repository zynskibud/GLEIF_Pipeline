import numpy as np 
import seaborn as sns 
import matplotlib.pyplot as plt
from matplotlib.colors import BoundaryNorm, ListedColormap
import re
from pyvis.network import Network
import networkx as nx
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import urllib.parse
import re

class Visualizations:
    """
    This class houses all of our visualizations, they could be time series plots, heatmaps, anything 
    Things not implemented, the recrusive case for find factors i dont think its necessary for right now
    """
    def datatframe_heatmap(self, df_plot , filename, xaxis_size = 13 , yaxis_size = 7 , show = False):
        """
        This function turns a dataframe into a heatmap
        @param: df_plot - this is the dataframe that is going to be plotted
        @param: filename - this is the name of the file it will be saved to in images
        @param: xaxis_size - this is the size of the plots x-axis
        @param: yaxis_size - this is the size of the plots y-axis
        @param: show - if true the plot will be plotted to the screen (if calling in a python environment)
    
        """        
        plt.figure(figsize = (xaxis_size, yaxis_size))
        sns.heatmap(df_plot, annot = True, cmap = 'RdYlGn')

        self.results_path = rf"C:\Git\Systematic_Trading\Library\Images\Heatmaps\HM_{filename}.png"
        plt.savefig(self.results_path)
        if show:
            plt.figure()


    def HeatMap(self , df_data , namingColumn , numericalColumn , title , filename , xaxis_size = 13 , yaxis_size = 7 , percent = False , plot = False, bounds_replacement = None, Color_Map_replacement = None , threshold = 8):
        """
        This function produces a heatmap for the user given specific columns to plot 
        @param: df_data - this is the dataframe that houses all of the data which this function will use to plot 
        @param: naming column - this is the column name that houses the names of whatever you are plotting for example tickers is a good example
        @param: numerical column - this is the column name that houses the numerical data that will be plotted along with the naming column 
        @param: title - this is the title of the plot that the user must enter 
        @param: filename - this is the name of the file which will house this image in the images folder
        @param: xaxis_size - this is the size of the plots x-axis
        @param: yaxis_size - this is the size of the plots y-axis
        @param: percent - if true the plot will be plotted to the screen (if calling in a python environment)
        @param: plot - if this is true the heatmap is plotted to the screen.
        @param: bounds_replacement - if this is populated then the color map bounds are set to be the entered in bounds
        @param: Color_Map_replacement - if this is populated then the color map coloring scheme is set to whatever the entered in color map

        @calls: findFactors - to get the optimal dimensions for the heatmap

        """
        
        #getting the string labels (or could be numerical) as a numpy array 
        nump_labels = df_data[namingColumn].to_numpy()

        if percent:
            df_data[numericalColumn] = df_data.apply(lambda x: x[numericalColumn] * 100 , axis = 1)
        #this is the numerical column which is being plotted
        nump_numerical = df_data[numericalColumn].to_numpy()

        #getting the dimensions of the plot and the modified numerical numpy array
        Dimension1 , Dimension2 , nump_numerical, nump_labels = self.findFactors(length_numerical = len(nump_numerical) , nump_numerical = nump_numerical , nump_labels = nump_labels , threshold = threshold)

        #now reshape both arrays to fit the dimensions above
        nump_numerical = nump_numerical.reshape(Dimension1 , Dimension2)
        nump_labels = nump_labels.reshape(Dimension1 , Dimension2)

        #determine the color range you want to use it can either be the preset one or can be entered in by the user (hence the if statements),
        if not Color_Map_replacement:
            my_colors = ['red', 'orange', 'white', 'lightgreen', 'green' ,'darkgreen']

        if Color_Map_replacement:
            my_colors = Color_Map_replacement

        my_cmap = ListedColormap(my_colors)

        #determine the values that will classify which color in the color map is recieved by a box in the heatmap, this is also variate meaning it can be entered in by the user or preset 
        if not bounds_replacement:
            bounds = [-1, -0.5, 0, 0.5, 1, 5]
        
        if bounds_replacement:
            bounds = bounds_replacement

        my_norm = BoundaryNorm(bounds, ncolors = len(my_colors))

        #if the user wants to use percentages the percentages string is added
        if percent:
            map_labels = (np.asarray(["{0} \n {1:.4f}%".format(ticker,derivative) for ticker,derivative in zip(nump_labels.flatten(),nump_numerical.flatten())])).reshape(Dimension1,Dimension2)
        else:
            map_labels = (np.asarray(["{0} \n {1:.4f}".format(ticker,derivative) for ticker,derivative in zip(nump_labels.flatten(),nump_numerical.flatten())])).reshape(Dimension1,Dimension2)
        
        #creating the plot
        fig,ax = plt.subplots(figsize = (xaxis_size,yaxis_size))

        #setting the title
        plt.title(title,fontsize=18)
        ttl = ax.title
        ttl.set_position([0.5,1.05])
        
        #hide ticks for x and y axis 
        ax.set_xticks([])
        ax.set_yticks([])
    
        ax.axis('off')

        #produce heatmap
        sns.heatmap(nump_numerical , annot = map_labels , fmt = "", cmap = my_cmap , norm = my_norm , linewidths = 0.30 , ax = ax)

        #path to where the figure is saved using the filename variable
        results_path = rf"C:\Git\Systematic_Trading\Library\Images\Heatmaps\HM_{filename}.png"
        plt.savefig(results_path)
        
        #plotting it to screen if the user wants
        if plot:
            plt.show()

    def findFactors(self , length_numerical , nump_numerical , nump_labels , threshold , bestDifference = 10000):  
        """
        This function determines the best dimensions to use for a plot so it is the most square
        @param: length_numerical - this is the number of numerical values to plot
        @param: nump_numerical - this is the numpy array of those numerical values
        @param: nump_labels - this is the numpy array of the labels  
        @param: threshold - this is the max distance the two dimensions should be from eachother, meaning the difference between the number of rows and columns
        
        @calls: findFactors - it calls itself if it isnt able to find the optimal differencing 
        @return - the best combination of dimensions for a given numerical list and the two modified lists if they were modified
        """

        dimension1 , dimension2 = 0 , 0 #initializing dimensions to 0
        for i in range(1 , length_numerical + 1): #ranging from 1 to the number of Num_Numerical + 1 since it is 0 indexed (and we want to run up the number itself in an edge case) and we are trying to break the number down into factors 
            if length_numerical % i == 0: #if the number evenely divides into the number of Num_Numerical
                dimensiontemp1 = i #this is a temporary dimension we are testing
                dimensiontemp2 = int(length_numerical / i) #getting the other factor 

                if np.abs(dimensiontemp1 - dimensiontemp2) < bestDifference: #seeing how far off these factors are from one another and seeing if that is better than what we have found so far
                    bestDifference = np.abs(dimensiontemp1 - dimensiontemp2) #reassigning this as the best difference if it worked
                    dimension1 = dimensiontemp1 #reassinging the dimensions to the best ones weve found so far
                    dimension2 = dimensiontemp2

        if bestDifference < threshold:
            return dimension1 , dimension2, nump_numerical, nump_labels
        else:
            #this is the recursive case, if it doesnt work for some reason maybe the number has terrible factors basically we just add two and try again
            nump_numerical = np.append(nump_numerical , 0)
            nump_labels = np.append(nump_labels , 0)
            return self.findFactors(len(nump_numerical) , nump_numerical, nump_labels)
        
    def visualize_graph_interactive_html(self , G: nx.DiGraph, title: str = 'Interactive_Graph') -> str:
        """
        Visualizes the graph using PyVis and returns the HTML content as a string.
        
        :param G: The NetworkX directed graph to visualize.
        :param title: The title of the graph.
        :return: HTML string of the interactive graph.
        """
        # Initialize PyVis Network with remote CDN resources for better compatibility
        net = Network(notebook=False, directed=True, height='750px', width='100%', cdn_resources='remote')
        
        # Add nodes with titles
        for node, data in G.nodes(data=True):
            label = node
            title_text = (
                f"Node: {node}<br>"
                f"Parents: {len(data.get('parent_relationships', {}))}<br>"
                f"Children: {len(data.get('child_relationships', {}))}"
            )
            net.add_node(node, label=label, title=title_text)
        
        # Add edges
        for source, target in G.edges():
            net.add_edge(source, target)
        
        # Customize physics for better layout
        net.show_buttons(filter_=['physics'])
        net.toggle_physics(True)
        
        # Generate HTML as a string
        try:
            html_content = net.generate_html()
            print(f"Interactive graph '{title}' generated successfully.")
            return html_content
        except Exception as e:
            print(f"Error generating the interactive graph: {e}")
            return ""

    def display_html_in_browser(self , html_content: str):
        """
        Displays the given HTML content in a web browser using Selenium and keeps the browser open.
        
        :param html_content: The HTML content to display.
        """
        if not html_content:
            print("No HTML content to display.")
            return
        
        # Encode the HTML content for use in a data URL
        encoded_html = urllib.parse.quote(html_content)
        data_url = f"data:text/html;charset=utf-8,{encoded_html}"
        
        # Setup Selenium WebDriver (Chrome in this example)
        try:
            # Initialize Chrome WebDriver using webdriver_manager for automatic driver management
            service = ChromeService(ChromeDriverManager().install())
            options = webdriver.ChromeOptions()
            options.add_argument("--start-maximized")  # Optional: start maximized
            options.add_experimental_option("detach", True)  # Keep the browser open after script ends
            driver = webdriver.Chrome(service=service, options=options)
            
            # Open the data URL
            driver.get(data_url)
            
            #print("Interactive graph opened in the browser.")
            
            # Optional: Keep the script running to maintain the WebDriver session
            # Uncomment the following lines if you prefer the browser to stay open until you manually close it
            # import time
            # while True:
            #     time.sleep(10)
            
        except Exception as e:
            print(f"Error opening the interactive graph in the browser: {e}")

