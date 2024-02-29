from googleapiclient.discovery import build
import pandas as pd

def api_connect(API_KEY:str):
    youtube = build('youtube','v3',developerKey = API_KEY)

    request = youtube.commentThreads().list(
            part="snippet, replies",
            videoId="j58-XvdR62E",
            maxResults=50,
            order='time'
        )

    response = request.execute()
    #print(response)
    return response


def process_comments(response):
  """
  Function to process the response from the Twitter API.
  """
  data_info = [
    {
        'Comments': item['snippet']['topLevelComment']['snippet']['textOriginal'],
        'Author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
        'Timestamp': item['snippet']['topLevelComment']['snippet']['publishedAt']
    }
    for item in response.get('items', [])
]
  
  print(f'Finished processing {len(data_info)} comments.')

  #Converting the list of comments to a dataframe
  data_info = pd.DataFrame(data_info)

  return data_info