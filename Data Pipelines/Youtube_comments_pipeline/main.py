import os
import json
import pandas as pd
from utility import api_connect, process_comments

API_KEY = 'AIzaSyBPBYiwuOb23cFoP_FTGF_YOturSWsANEc'

if __name__ == '__main__':
    
    #Connecting to the api 
    response = api_connect(API_KEY)

    #Processing the data obtained from the api request
    data = process_comments(response)

    #Displaying the dataframe 
    print(data)
    
    #Saving the data in a csv file
    data.to_csv('youtube_api_comments.csv', index=False)










