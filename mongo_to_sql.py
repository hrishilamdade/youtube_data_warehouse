import pymongo
import mysql.connector

# MongoDB Connection



# Define a function to map MongoDB documents to MySQL inserts

def map_mongo_to_mysql_playlist(document):
    # Extract values from the MongoDB document
    playlist_name = document.get("Playlist_Name")
    playlist_id = document.get("Playlist_Id")
    channel_id = document.get("Channel_Id")

    return (playlist_id,channel_id, playlist_name )

def map_mongo_to_mysql(document):
    # Extract values from the MongoDB document
    channel_name = document.get("channel_name")
    channel_id = document.get("Channel_Id")
    subscription_count = document.get("Subscription_Count")
    channel_views = int(document.get("Channel_Views"))
    channel_description = document.get("Channel_Description")
    playlist_id = document.get("Playlist_Id")
    # playlist_name = document.get("Playlist_Name")
    return (channel_name, channel_id, subscription_count, channel_views, channel_description, playlist_id)

def videos_mongo_to_mysql(document):
    # Extract values from the MongoDB document
    video_data = document
    video_id = video_data.get("Video_Id")
    channel_id = video_data.get("Channel_Id")
    video_name = video_data.get("Video_Name")
    video_description = video_data.get("Video_Description")
    tags = ",".join(video_data.get("Tags", []))
    published_at = video_data.get("PublishedAt")
    view_count = video_data.get("View_Count")
    like_count = video_data.get("Like_Count")
    dislike_count = video_data.get("Dislike_Count")
    favorite_count = video_data.get("Favorite_Count")
    comment_count = video_data.get("Comment_Count")
    duration = video_data.get("Duration")
    thumbnail = video_data.get("Thumbnail")
    caption_status = video_data.get("Caption_Status")

    return (
        video_id,channel_id, video_name, video_description, tags, published_at,
        view_count, like_count, dislike_count, favorite_count, comment_count,
        duration, thumbnail, caption_status
    )

# Define a function to map MongoDB documents to MySQL inserts for comments
def map_mongo_to_mysql_comment(document):
    video_data = document
    video_id = video_data.get("Video_Id")
    comments_data = video_data.get("Comments", {})

    comments = []
    for comment_info in comments_data:
        comment_id = comment_info.get("Comment_Id")
        comment_text = comment_info.get("Comment_Text")
        comment_author = comment_info.get("Comment_Author")
        comment_published_at = comment_info.get("Comment_PublishedAt")
        comments.append((comment_id, video_id, comment_text, comment_author, comment_published_at))

    return comments



# Fetch data from MongoDB
def main(channel_name):
    mongo_client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.ivmsvhv.mongodb.net/?retryWrites=true&w=majority/")
    mongo_db = mongo_client["yt_data_final"]
    mongo_collection = mongo_db["channels"]

    name_to_id_mapper = {}
    for document in mongo_collection.find():
        name_to_id_mapper[document.get("channel_name")] = document.get("Channel_Id")
    # MySQL Connection
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="admin",
        password="123",
        database="yt_data"
    )
    mysql_cursor = mysql_connection.cursor()

    channel_id = name_to_id_mapper.get(channel_name)
    channel_mongo_documents = mongo_collection.find({"Channel_Id":channel_id}).limit(1)
    video_mongo_documents = mongo_db["videos"].find({"Channel_Id":channel_id})
    video_mongo_documents = list(video_mongo_documents)

    print(channel_mongo_documents[0])
    print(video_mongo_documents)
    data_tuple = map_mongo_to_mysql(channel_mongo_documents[0])
    insert_query = "INSERT INTO channels (channel_name, Channel_Id, Subscription_Count, Channel_Views, Channel_Description, Playlist_Id) VALUES (%s, %s, %s, %s, %s, %s)"  # Adjust the query to match your table schema
    mysql_cursor.execute(insert_query, data_tuple)

    data_tuple = map_mongo_to_mysql_playlist(channel_mongo_documents[0])
    print(data_tuple)
    insert_query = "INSERT INTO playlists (Playlist_Id, Channel_Id ,Playlist_Name) VALUES (%s, %s, %s)"  # Adjust the query to match your table schema
    mysql_cursor.execute(insert_query, data_tuple)

    for document in video_mongo_documents:
        # print(document)
        data_tuple = videos_mongo_to_mysql(document)
        print(data_tuple)
        insert_query = """
        INSERT INTO videos (
            video_id, channel_id,video_name, video_description, tags, published_at,
            view_count, like_count, dislike_count, favorite_count, comment_count,
            duration, thumbnail, caption_status
        ) VALUES (%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """  
        mysql_cursor.execute(insert_query, data_tuple)
    # print(video_mongo_documents[0])
    for document in video_mongo_documents:
        # print(document)
        data_tuple = map_mongo_to_mysql_comment(document)
        # print(data_tuple)
        insert_query = """
        INSERT INTO comments (comment_id, video_id, comment_text, comment_author, comment_published_at) VALUES (%s, %s, %s, %s, %s)
        """
        mysql_cursor.executemany(insert_query, data_tuple)
   
    mysql_connection.commit()

    # Close connections

    print("Data migration completed.")
    mongo_client.close()
    # mysql_cursor.close()
    mysql_connection.close()