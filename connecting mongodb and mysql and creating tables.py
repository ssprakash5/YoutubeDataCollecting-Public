import pymongo
import mysql.connector
import datetime

# converting ISO datetime to MySQL datetime format
def convert_iso8601_to_mysql(iso_datetime_str):
    try:
        iso_datetime = datetime.datetime.strptime(iso_datetime_str, "%Y-%m-%dT%H:%M:%SZ")
        return iso_datetime.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        print(f"Error converting datetime: {e}")
        return None

# Function to handle non-integer values(to avoid entering integers int sql)
def handle_non_integer(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return None  # Handle non-integer values as NULL

# Connecting to MongoDB
mongo_client = pymongo.MongoClient("mongodb+srv://SSP:Surya123@cluster0.fkaowh5.mongodb.net/?retryWrites=true&w=majority")
mongo_db = mongo_client['youtube_data']
mongo_channels_collection = mongo_db['channels']
mongo_playlists_collection = mongo_db['playlists']
mongo_videos_collection = mongo_db['videos']

# Connecting to MySQL
mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    port='3306',
    password='Surya123',
    database='youtube'  # Replace with your actual database name
)

# Create MySQL cursor
mysql_cursor = mysql_connection.cursor()

# Create MySQL tables channels,playlists and videos
mysql_cursor.execute('''
    CREATE TABLE IF NOT EXISTS channels (
        channel_id VARCHAR(500),
        channel_name VARCHAR(500),
        channel_type TEXT,
        channel_views BIGINT,
        channel_description TEXT,
        subscriber_count INT,
        total_video_count INT
        -- Add more columns as needed
    )
''')

mysql_cursor.execute('''
    CREATE TABLE IF NOT EXISTS playlists (
        playlist_id VARCHAR(500),
        channel_id VARCHAR(500),
        playlist_name VARCHAR(500)
        -- Add more columns as needed
    )
''')

mysql_cursor.execute('''
    CREATE TABLE IF NOT EXISTS videos (
        video_id VARCHAR(500),
        playlist_id VARCHAR(500),
        video_name VARCHAR(500),
        video_description TEXT,
        published_date DATETIME,
        view_count BIGINT,
        like_count INT,
        dislike_count INT,
        favourite_count INT,
        comment_count INT,
        duration INT,
        thumbnail_url TEXT
        -- Add more columns as needed
    )
''')

mysql_connection.commit()

try:
    # Insert channel data from MongoDB into MySQL
    channel_data = mongo_channels_collection.find()
    for channel in channel_data:
        channel_id = channel.get('channel_id', None)
        channel_name = channel.get('channel_name', None)
        channel_type = channel.get('channel_type', None)
        channel_views = handle_non_integer(channel.get('channel_views', None))
        channel_description = channel.get('channel_description', None)
        subscriber_count = channel.get('subscriber_count', None)
        total_video_count = channel.get('total_video_count', None)

        if channel_id is not None:
            insert_query = '''
                INSERT INTO channels (channel_id, channel_name, channel_type, channel_views, channel_description, subscriber_count, total_video_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''
            mysql_cursor.execute(insert_query, (channel_id, channel_name, channel_type, channel_views, channel_description, subscriber_count, total_video_count))

    # Insert playlist data from MongoDB into MySQL
    playlist_data = mongo_playlists_collection.find()
    for playlist in playlist_data:
        playlist_id = playlist.get('playlist_id', None)
        channel_id = playlist.get('channel_id', None)
        playlist_name = playlist.get('playlist_name', None)
        if playlist_id is not None:
            insert_query = '''
                INSERT INTO playlists (playlist_id, channel_id, playlist_name)
                VALUES (%s, %s, %s)
            '''
            mysql_cursor.execute(insert_query, (playlist_id, channel_id, playlist_name))

    # Insert video data from MongoDB into MySQL
    video_data = mongo_videos_collection.find()
    for video in video_data:
        video_id = video.get('video_id', None)
        playlist_id = video.get('playlist_id', None)
        video_name = video.get('video_name', None)
        video_description = video.get('video_description', None)
        published_date_iso8601 = video.get('published_date', None)
        view_count = handle_non_integer(video.get('view_count', None))
        like_count = handle_non_integer(video.get('like_count', None))
        dislike_count = handle_non_integer(video.get('dislike_count', None))
        favourite_count = handle_non_integer(video.get('favourite_count', None))
        comment_count = handle_non_integer(video.get('comment_count', None))
        duration = handle_non_integer(video.get('duration', None))
        thumbnail_url = video.get('thumbnail_url', None)

        if video_id is not None:
            # Converting ISO 8601 datetime to MySQL datetime format
            published_date_mysql = convert_iso8601_to_mysql(published_date_iso8601)

            insert_query = '''
                INSERT INTO videos (video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favourite_count, comment_count, duration, thumbnail_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            mysql_cursor.execute(insert_query, (video_id, playlist_id, video_name, video_description, published_date_mysql, view_count, like_count, dislike_count, favourite_count, comment_count, duration, thumbnail_url))

    # Commit changes and close MySQL cursor and connection
    mysql_connection.commit()

except Exception as e:
    # Handle exceptions here (e.g., log the error, roll back the transaction)
    print(f"Error: {str(e)}")
    mysql_connection.rollback()

finally:
    mysql_cursor.close()
    mysql_connection.close()
