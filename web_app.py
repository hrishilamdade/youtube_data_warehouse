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
        SELECT v.Video_Name, c.Channel_Name
        FROM channels c
        JOIN videos v ON c.Channel_Id = v.Channel_Id;
    """)
    videos = mysql_cursor.fetchall()
    return videos

def get_video_count_by_channel():
    mysql_cursor.execute("""
        SELECT c.Channel_Name, COUNT(v.Video_Name) AS Video_Count
        FROM channels c
        JOIN videos v ON c.Channel_Id = v.Channel_Id
        GROUP BY c.Channel_Name ORDER BY Video_Count DESC;
    """)
    videos = mysql_cursor.fetchall()
    return videos

def get_views_video():
    mysql_cursor.execute("""
        SELECT v.Video_Name, v.View_Count
        FROM videos v ORDER BY v.View_Count DESC;
    """)
    videos = mysql_cursor.fetchall()
    return videos

def get_comments_count():
    mysql_cursor.execute("""
        SELECT v.Video_Name, v.Comment_Count
        FROM videos v ORDER BY v.Comment_Count DESC;
    """)
    videos = mysql_cursor.fetchall()
    return videos    

def get_likes_dislikes():
    mysql_cursor.execute("""
        SELECT v.Video_Name, v.Like_Count, v.Dislike_Count
        FROM videos v ORDER BY v.Like_Count DESC;
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

def get_channels_posted_in_2022():
    mysql_cursor.execute("""
        SELECT c.Channel_Name, v.Video_Name, v.published_at
        FROM channels c
        JOIN videos v ON c.Channel_Id = v.Channel_Id
        WHERE v.published_at LIKE '%2022%';
    """)
    videos = mysql_cursor.fetchall()
    
    return videos

def get_average_duration_of_videos():

    mysql_cursor.execute("""
        SELECT c.Channel_Name, AVG(v.Duration) AS Average_Duration
        FROM channels c
        JOIN videos v ON c.Channel_Id = v.Channel_Id
        GROUP BY c.Channel_Name ORDER BY v.Duration DESC;
    """)
    videos = mysql_cursor.fetchall()

    # convert duration in ISO 8601 to seconds , e.g. P1W2DT6H21M32S= ,PT1H2M3S = 3723 seconds

    return videos

def get_highest_comments():
    mysql_cursor.execute("""
        SELECT v.Video_Name, v.Comment_Count, c.Channel_Name
        FROM videos v
        JOIN channels c ON v.Channel_Id = c.Channel_Id
        ORDER BY v.Comment_Count DESC
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
# if st.button("Get All Channels"):
    # tmp = get_all_videos_and_channels()
    # st.table(tmp)


# Button to get all videos and their from the database
st.text("1. Get All Videos")
if st.button("Get All Videos"):
    tmp = get_all_videos_and_channels()
    st.table(tmp)

st.text("2. Video count by channel")
if st.button("Video count by channel"):
    tmp = get_video_count_by_channel()
    st.table(tmp)

st.text("3. View count on Video")
if st.button("View count on Video"):
    tmp = get_views_video()
    st.table(tmp)

st.text("4. Get comments Count on video")
if st.button("Get comments Count"):
    tmp = get_comments_count()
    st.table(tmp)

st.text("5. Get highest liked videos channels")
if st.button("Get highest liked videos channels"):
    tmp = get_highest_liked_videos()
    st.table(tmp)

st.text("6 .Get likes and dislikes for videos")
if st.button("Get likes and dislikes for videos"):
    tmp = get_likes_dislikes()
    st.table(tmp)

st.text("7. Get total views on channel")
if st.button("Get total views on channel"):
    tmp = get_total_views()
    st.table(tmp)

st.text("8. Get Channles who posted video in 2022")
if st.button("Get Channles"):
    tmp = get_channels_posted_in_2022()
    st.table(tmp)

st.text("9. Get average duration of videos")
if st.button("Get average duration of videos"):
    tmp = get_average_duration_of_videos()
    st.table(tmp)

st.text("10. Get highest comments")
if st.button("Get highest comments"):
    tmp = get_highest_comments()
    st.table(tmp)