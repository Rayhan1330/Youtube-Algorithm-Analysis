from googleapiclient.discovery import build
from hdfs import InsecureClient
import pandas as pd

# Hadoop connection settings
hadoop_host = 'http://localhost:9870'
hadoop_user = 'rayhanabdurrahman'
hadoop_input_path = '/user/Rayhan/youtube_data_input'

# Replace 'YOUR_API_KEY' with the API key you obtained
api_key = 'AIzaSyCwLlcEsA1dHZ8ahrvK6qxVmBJSH9bV560'

# Build the YouTube Data API service
youtube = build('youtube', 'v3', developerKey=api_key)

# Specify the category you are interested in (e.g., 'Gaming')
category = 'Gaming'

# Initialize an empty list to store video IDs
video_ids = []

# Request videos from the specified category
search_query = input("Enter your search: ")
response = youtube.search().list(
    q=search_query,
    part='id',
    type='video',
    maxResults=50
).execute()

# Extract video IDs from the response
for item in response['items']:
    video_ids.append(item['id']['videoId'])

# Initialize Hadoop client
hdfs_client = InsecureClient(hadoop_host, user=hadoop_user)

# Loop through each video ID and write data to HDFS
for video_id in video_ids:
    response = youtube.videos().list(
        part='snippet,contentDetails,statistics',
        id=video_id
    ).execute()

    # Extract relevant information from the response
    video_info = response['items'][0]
    snippet = video_info['snippet']
    content_details = video_info['contentDetails']
    statistics = video_info['statistics']

    # Extract video URL from snippet
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    # Store the data in a DataFrame
    video_data = {
        'Video ID': video_id,
        'Title': snippet['title'],
        'Published At': snippet['publishedAt'],
        'Duration': content_details['duration'],
        'Views': int(statistics.get('viewCount', 0)),
        'Likes': int(statistics.get('likeCount', 0)),
        'Dislikes': int(statistics.get('dislikeCount', 0)),
        'Comments': int(statistics.get('commentCount', 0)),
        'URL': video_url
    }

    df = pd.DataFrame([video_data])

    # Write DataFrame to HDFS
    with hdfs_client.write(f'{hadoop_input_path}/youtube_data_{video_id}.csv', encoding='utf-8') as writer:
        df.to_csv(writer, index=False, header=True)

print('Data written to HDFS successfully.')
