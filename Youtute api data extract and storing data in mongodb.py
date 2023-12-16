import os
import googleapiclient.discovery
import pymongo

# Set up the Google API key and API service
api_key = 'AIzaSyAcSYd14SzeVTNZM1X5IxNiSAsa6Y-ogrs'  # Insert YouTube Data API key
youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=api_key)

# Define the YouTube channel name(Vivo,Sony,CNN,BBC,Meta,Apple,PlayStation,Nokia,Walmart)
channel_name = 'Microsoft'  #insert only one channel name ones

# Retrieve channel information using the channel name
channel_response = youtube.channels().list(
    part='snippet,statistics',
    forUsername=channel_name  # Use channel name instead of the ID
).execute()

# Check if the response contains items
if 'items' in channel_response:
    channel_data = channel_response['items'][0]
    channel_id = channel_data['id']
    channel_name = channel_data['snippet']['title']
    channel_type = channel_data['snippet']['description']  # Fix the description retrieval
    channel_views = channel_data['statistics']['viewCount']
    channel_description = channel_data['snippet']['description']
    subscriber_count = channel_data['statistics']['subscriberCount']
    total_video_count = channel_data['statistics']['videoCount']

    print(f"Channel Name: {channel_name}")
    print(f"Channel ID: {channel_id}")
    print(f"Channel Type: {channel_type}")
    print(f"Channel Views: {channel_views}")
    print(f"Channel_Description: {channel_description}")
    print(f"Subscribers: {subscriber_count}")
    print(f"Total Videos: {total_video_count}")

    # Retrieve playlists for the channel
    playlists_response = youtube.playlists().list(
        part='snippet',
        channelId=channel_id,
        maxResults=20  # You can adjust this, if you want more playlists
    ).execute()

    # Check if the response contains items
    if 'items' in playlists_response:
        # Extract playlist IDs
        playlist_ids = [item['id'] for item in playlists_response['items']]

        # Retrieve videos from each playlist
        for playlist_id in playlist_ids:
            playlist_response = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=20  # You can adjust this value to get more videos if needed
            ).execute()

            # Check if the response contains items
            if 'items' in playlist_response:
                video_ids = [item['contentDetails']['videoId'] for item in playlist_response['items']]
                print('Playlist_name')

                # Retrieve video information for each video
                for video_id in video_ids:
                    video_response = youtube.videos().list(
                        part='snippet,statistics',
                        id=video_id
                    ).execute()

                    # Check if the response contains items and is not empty
                    if 'items' in video_response and len(video_response['items']) > 0:
                        video_data = video_response['items'][0]
                        video_title = video_data['snippet']['title']
                        video_description = video_data['snippet']['description']
                        published_date = video_data['snippet']['publishedAt']
                        view_count = video_data['statistics'].get('viewCount', 'N/A')
                        like_count = video_data['statistics'].get('likeCount', 'N/A')
                        dislike_count = video_data['statistics'].get('dislikeCount', 'N/A')
                        favorite_count = video_data['statistics'].get('favoriteCount', 'N/A')
                        comment_count = video_data['statistics'].get('commentCount', 'N/A')
                        duration= video_data['statistics'].get('duration','N/A')
                        thumbnail_url = video_data['snippet']['thumbnails']['default']['url']

                        print(f"Video Title: {video_title}")
                        print(f"Video ID: {video_id}")
                        print(f"video_description: {video_description}")
                        print(f"view_count:{view_count}")
                        print(f"published_date:{published_date}")
                        print(f"Likes: {like_count}")
                        print(f"Dislikes: {dislike_count}")
                        print(f"favorite_count: {favorite_count}")
                        print(f"Comments: {comment_count}")
                        print(f"duration: {duration}")
                        print(f"thumbnail_url: {thumbnail_url}")
                        print()
                    else:
                        print(f"Video with ID {video_id} not found or has no statistics data.")

# Connect to MongoDB
client = pymongo.MongoClient('mongodb+srv://SSP:Surya123@cluster0.fkaowh5.mongodb.net/?retryWrites=true&w=majority')
db = client['youtube_data']  # Replace with your database name

# Define functions to insert data into MongoDB
def insert_channel_data(channel_data):
    channels_collection = db['channels']
    channels_collection.insert_one(channel_data)

def insert_playlist_data(playlist_data):
    playlists_collection = db['playlists']
    playlists_collection.insert_many(playlist_data)

def insert_video_data(video_data):
    videos_collection = db['videos']
    videos_collection.insert_many(video_data)


# Insert channel data
channel_data_to_insert = {
    'channel_id': channel_id,
    'channel_name': channel_name,
    'channel_type': channel_type,
    'channel_views': channel_views,
    'channel_description': channel_description,
    'subscriber_count': subscriber_count,
    'total_video_count': total_video_count
}
insert_channel_data(channel_data_to_insert)

# Insert playlist data
playlist_data_to_insert = []
for playlist_id in playlist_ids:
    # Fetch playlist details from the YouTube API
    playlist_response = youtube.playlists().list(
        part='snippet',
        id=playlist_id
    ).execute()

    if 'items' in playlist_response and playlist_response['items']:
        playlist_name = playlist_response['items'][0]['snippet']['title']
        playlist_data_to_insert.append({
            'playlist_id': playlist_id,
            'channel_id': channel_id,
            'playlist_name': playlist_name
        })
    else:
        print(f"Playlist with ID {playlist_id} not found.")

insert_playlist_data(playlist_data_to_insert)

# Insert video data
video_data_to_insert = []
for playlist_id in playlist_ids:
    playlist_response = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=20
    ).execute()

    if 'items' in playlist_response:
        video_ids = [item['contentDetails']['videoId'] for item in playlist_response['items']]

        for video_id in video_ids:
            video_response = youtube.videos().list(
                part='snippet,statistics',
                id=video_id
            ).execute()

            if 'items' in video_response and video_response['items']:
                video_data = video_response['items'][0]
                video_name = video_data['snippet']['title']
                video_description = video_data['snippet']['description']
                published_date = video_data['snippet']['publishedAt']
                view_count = video_data['statistics'].get('viewCount', 'N/A')
                like_count = video_data['statistics'].get('likeCount', 'N/A')
                dislike_count = video_data['statistics'].get('dislikeCount', 'N/A')
                favorite_count = video_data['statistics'].get('favoriteCount', 'N/A')
                comment_count = video_data['statistics'].get('commentCount', 'N/A')
                duration = video_data['statistics'].get('duration', 'N/A')
                thumbnail_url = video_data['snippet']['thumbnails']['default']['url']

                video_data_to_insert.append({
                    'video_id': video_id,
                    'playlist_id': playlist_id,
                    'video_name': video_name,
                    'video_description': video_description,
                    'published_date': published_date,
                    'view_count': view_count,
                    'like_count': like_count,
                    'favorite_count': favorite_count,
                    'comment_count': comment_count,
                    'duration':duration,
                    'thumbnail_url':thumbnail_url
                    # Add other fields here as needed
                })
            else:
                print(f"Video with ID {video_id} not found or has no statistics data.")

insert_video_data(video_data_to_insert)

# Close MongoDB connection
client.close()
