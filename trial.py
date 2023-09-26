import streamlit as st
import youtube_data as yt
import mysql.connector
from pymongo import MongoClient

# MySQL Connection
mysql_connection = mysql.connector.connect(
    host="localhost",
    user="admin",
    password="123",
    database="yt_data"
)
mysql_cursor = mysql_connection.cursor()    

def get_all_videos_and_channels():
    mysql_cursor.execute("""
        SELECT c.Channel_Name, v.Video_Name
        FROM channels c
        JOIN videos v ON c.Channel_Id = v.Channel_Id;
    """)
    videos = mysql_cursor.fetchall()
    return videos

def get_likes_dislikes():
    mysql_cursor.execute("""
        SELECT v.Video_Name, v.Like_Count, v.Dislike_Count
        FROM videos v;
    """)
    videos = mysql_cursor.fetchall()
    return videos

def get_highest_liked_videos():
    mysql_cursor.execute("""
        SELECT v.Video_Name, v.Like_Count, c.Channel_Name
        FROM videos v
        JOIN channels c ON v.Channel_Id = c.Channel_Id
        ORDER BY v.Like_Count DESC
        LIMIT 10;
    """)
    videos = mysql_cursor.fetchall()
    return videos

def get_total_views():
    mysql_cursor.execute("""
        SELECT c.Channel_Name, c.Channel_Views
        FROM channels c
        ORDER BY c.Channel_Views DESC
        LIMIT 10;
    """)
    videos = mysql_cursor.fetchall()
    return videos

st.title("YouTube Data Collection and Migration")

# Input field for YouTube channel ID
channel_id = st.text_input("Enter YouTube Channel ID:")

# Button to initiate data collection
if st.button("Collect Data"):
    yt.main(channel_id)

# dropdown to select a channel from the database
st.subheader("Migrate Data to SQL")
channel_name = st.selectbox("Select Channel Name", yt.get_all_channels_from_mongo())

# Button to initiate data migration
if st.button("Migrate Data to SQL"):
    yt.migrate_data(channel_name)

st.divider()
st.subheader("Get Data from SQL")
# Button to get all channels and their from the database
if st.button("Get All Channels"):
    tmp = get_all_videos_and_channels()
    st.table(tmp)


# Button to get all videos and their from the database
if st.button("Get All Videos"):
    yt.get_all_videos_from_mongo()

st.text("Get highest liked videos channels")
if st.button("Get highest liked videos channels"):
    tmp = get_highest_liked_videos()
    st.table(tmp)

st.text("Get likes and dislikes for videos")
if st.button("Get likes and dislikes for videos"):
    tmp = get_likes_dislikes()
    st.table(tmp)

st.text("Get total views on channel")
if st.button("Get total views on channel"):
    tmp = get_total_views()
    st.table(tmp)