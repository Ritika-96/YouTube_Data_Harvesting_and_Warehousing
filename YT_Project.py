from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
import pymongo
from pymongo import MongoClient
import json
import mysql.connector
import sqlite3
from isodate import parse_duration

# Creating a connection and database to connect MONGODB
client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client["youtube_db"]

api_key = 'AIzaSyDM08xVRyWxAsfUG2PPzCHDx45Mlfy-IaA'
youtube = build("youtube", "v3", developerKey=api_key)


# Getting channel information using cahnnel ids
def get_channel_info(youtube, channel_id):
    all_data = []
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    )

    response = request.execute()
    for i in response['items']:
        data = {'Channel_name': i['snippet']['title'],
                'Subscribers': i['statistics']['subscriberCount'],
                'views': i['statistics']['viewCount'],
                'Total_videos': i['statistics']['videoCount'],
                'Description': i['snippet']['description'],
                'PublishedAt': i['snippet']['publishedAt'],
                'playlist_id': i['contentDetails']['relatedPlaylists']['uploads']
                }

        all_data.append(data)

        return all_data


channel_id = 'UCiEmtpFVJjpvdhsQ2QAhxVA'
channel_info = get_channel_info(youtube, channel_id)
print(channel_info)

channel_data = pd.DataFrame(channel_info)
print(channel_data)


# Getting playlist id and name for a particular channel using channel id
def get_playlist_ids(youtube, channel_id):
    all_data = []
    request = youtube.playlists().list(
        part='snippet,contentDetails',
        channelId=channel_id,
        maxResults=50
    )
    response = request.execute()

    for i in range(len(response['items'])):
        data = dict(Playlist_id=response['items'][i]['id'],
                    Playlist_name=response['items'][i]['snippet']['title'])
        all_data.append(data)

    return all_data


playlist_info = get_playlist_ids(youtube, channel_id)
print(playlist_info)

playlist_data = pd.DataFrame(playlist_info)
print(playlist_data)


# getting video ids
def get_video_ids(youtube, playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part='snippet, contentDetails',
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()
    for i in response['items']:
        video_ids.append(i['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part='snippet, contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for i in response['items']:
            video_ids.append(i['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
    return video_ids


playlist_id = 'PLLKD38iDspS5JYvswNeB2KeQ7YzFRHU0h'
video_ids = get_video_ids(youtube, playlist_id)
print(video_ids)
print(len(video_ids))


# getting video details
def get_video_details(youtube, video_ids):
    all_video_info = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join(video_ids[i:i + 50])
        )
        response = request.execute()
        for video in response['items']:
            stats = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                     'statistics': ['viewCount', 'likeCount', 'dislikeCount', 'favouriteCount', 'commentCount'],
                     'contentDetails': ['duration', 'definition', 'caption']}
        video_info = {}
        video_info['video_id'] = video['id']

        for k in stats.keys():
            for v in stats[k]:
                try:
                    video_info[v] = video[k][v]
                except:
                    video_info[v] = None
        all_video_info.append(video_info)
    return all_video_info


video_details = get_video_details(youtube, video_ids)
print(video_details)

video_table = pd.DataFrame(video_details)
print(video_table)


# getting comment details
def get_comments_details(youtube, video_ids):
    all_comments = []
    for i in video_ids:
        request = youtube.commentThreads().list(
            part='snippet, replies',
            videoId=i,
            maxResults=50
        )
        response = request.execute()
        if response['items']:
            get_comments_in_video = [comment['snippet']['topLevelComment']['snippet']['textOriginal'] for comment in
                                     response['items']]

            comments_in_video = {'video_id': i, 'comments': get_comments_in_video}
            all_comments.append(comments_in_video)
    return all_comments


comment_details = get_comments_details(youtube, video_ids)
print(comment_details)

comment_table = pd.DataFrame(comment_details)
print(comment_table)


def migrate_data_to_sqlite():
    # Set up the MongoDB client
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    # Select the database and collection to retrieve the data
    db = client["youtube_db"]
    collection = db["channel_data"]

    # Set up the SQLite connection and cursor
    conn = sqlite3.connect("youtube_db")
    cur = conn.cursor()
    # Create the tables in the SQLite database
    cur.execute('''CREATE TABLE IF NOT EXISTS channels
                   (channel_id text PRIMARY KEY, channel_name text, subscribers integer, video_count integer)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS videos
                   (video_id text PRIMARY KEY, channel_id text, title text, description text, publish_time text,
                    views integer, likes integer, dislikes integer, comments integer)''')

    # Retrieve the data from the MongoDB data lake
    data = list(collection.find())

    # Insert the data into the SQLite tables
    for item in data:
        # Insert data into the channels table
        channel_id = item['channel_id']
        channel_name = item['channel_name']
        subscribers = item['subscribers']
        video_count = item['video_count']

        cur.execute("INSERT INTO channels VALUES (?, ?, ?, ?)", (channel_id, channel_name, subscribers, video_count))

        # Insert data into the videos table
        for video in item['videos']:
            video_id = video['video_id']
            title = video['title']
            description = video['description']
            publish_time = video['publish_time']
            views = video['views']
            likes = video['likes']
            dislikes = video['dislikes']
            comments = video['comments']

            cur.execute("INSERT INTO videos VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (video_id, channel_id, title, description, publish_time, views, likes, dislikes, comments))

        data = list(collection.find())

        # Create the table in SQLite
        cur.execute("""CREATE TABLE IF NOT EXISTS channel_data 
                    (channel_name TEXT, channel_id TEXT, subscribers INTEGER, 
                    video_count INTEGER, playlist_id TEXT, video_id TEXT, 
                    likes INTEGER, dislikes INTEGER, comments INTEGER)""")

        # Insert the data into the SQLite table
        for item in data:
            cur.execute("""INSERT INTO channel_data 
                        (channel_name, channel_id, subscribers, video_count, 
                        playlist_id, video_id, likes, dislikes, comments) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (item['channel_name'], item['channel_id'], item['subscribers'],
                         item['video_count'], item['playlist_id'], item['video_id'],
                         item['likes'], item['dislikes'], item['comments']))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    print("Data migration to SQLite is complete.")

    def query_sqlite_data(selected_channel):
        conn = sqlite3.connect("youtube.db")
        cur = conn.cursor()

        # Join the channel_data and channel_details tables to get the channel details
        cur.execute("""SELECT channel_data.channel_name, channel_data.subscribers, 
                    channel_data.video_count, channel_data.playlist_id, 
                    channel_data.video_id, channel_data.likes, channel_data.dislikes, 
                    channel_data.comments, channel_details.description, 
                    channel_details.view_count, channel_details.comment_count, 
                    channel_details.published_date 
                    FROM channel_data 
                    JOIN channel_details ON channel_data.channel_id = channel_details.channel_id 
                    WHERE channel_data.channel_name = ?""", (selected_channel,))

        # Fetch the data and store it in a DataFrame
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=["Channel Name", "Subscribers", "Total Videos",
                                         "Playlist ID", "Video ID", "Likes", "Dislikes",
                                         "Comments", "Description", "View Count",
                                         "Comment Count", "Published Date"])

        # Close the connection
        conn.close()

        return df


# setting up streamlit app
def app():
    st.title("youtube data")
    channel_id = st.text_input('Enter the channel id here!')
    button = st.button('Search')
    if button:
        plyid = get_channel_info(youtube, channel_id)
        if plyid:
            unique_video_ids = get_video_ids(youtube, plyid[0]['playlist_id'])
            st.write(unique_video_ids)
            selected_choice = st.selectbox('select a video id', unique_video_ids)
            st.write('You selected:', selected_choice)
            button = st.button('Get video details')
            if button:
                video_details = get_video_details(youtube, selected_choice)
                st.write(video_details)
        else:
            st.error('No channel found')

    st.subheader("Select a channel id")
    channel_id = st.selectbox("Channel_Ids : ", ("UCnz-ZXXER4jOvuED5trXfEA",  # techTFQ
                                                 "UCiT9RITQ9PW6BhXK0y2jaeg",  # Keŋ Jee
                                                 "UC2UXDak6o7rBm23k3Vv5dww",  # Tina Huang
                                                 "UCz22l7kbce-uFJAoaZqxD1A",  # Gaur Gopal Das
                                                 "UCnjX8fylNvSKVSMuyTqshsQ",  # Cosmo Coding
                                                 "UCLhLpPmymIUy0JfF3Nkcf_w",  # Dharshan and rithika
                                                 "UCLLw7jmFsvfIVaUFsLs8mlQ",  # Luke Barousse
                                                 "UC7cs8q-gJRLGwj4A80mCmXg"))  # Alex the analyst
    button = st.button('Search here')
    if button:
        st. write('You selected:', channel_id)
        plyid = get_channel_info(youtube, channel_id)
        if plyid:
            unique_video_ids = get_video_ids(youtube, plyid[0]['playlist_id'])
            st.write(unique_video_ids)
            selected_choice = st.selectbox('select a video id', unique_video_ids)
            st.write('You selected:', selected_choice)

    st.subheader("Select a  question!!")
    ques1 = '1.	What are the names of all the videos and their corresponding channels?'
    ques2 = '2.	Which channels have the most number of videos, and how many videos do they have?'
    ques3 = '3.	What are the top 10 most viewed videos and their respective channels?'
    ques4 = '4.	How many comments were made on each video, and what are their corresponding video names?'
    ques5 = '5.	Which videos have the highest number of likes, and what are their corresponding channel names?'
    ques6 = '6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?'
    ques7 = '7.	What is the total number of views for each channel, and what are their corresponding channel names?'
    ques8 = '8.	What are the names of all the channels that have published videos in the year 2022?'
    ques9 = '9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?'
    ques10 = '10.	Which videos have the highest number of comments, and what are their corresponding channel names?'
    question = st.selectbox('Queries!!', (ques1, ques2, ques3, ques4, ques5, ques6, ques7, ques8, ques9, ques10))
    clicked4 = st.button("Go..")
    if clicked4:
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Ritika1996",
            database="youtube_db"
        )
        cursor = mydb.cursor()

        if question == ques1:
            query = "select c.channel_name,v.title FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id"
        elif question == ques2:
            query = "SELECT channel_name,video_count FROM channel_details ORDER BY video_count DESC"
        elif question == ques3:
            query = "SELECT c.channel_name,v.title,v.viewCount FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id ORDER BY view_count DESC LIMIT 10"
        elif question == ques4:
            query = "SELECT title,commentcount from video_details ORDER BY commentCount DESC"
        elif question == ques5:
            query = "SELECT c.channel_name, v.title, v.likeCount FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id ORDER BY like_count DESC"
        elif question == ques6:
            query = "SELECT title,likeCount, dislikeCount from video_details ORDER BY likeCount DESC"
        elif question == ques7:
            query = "SELECT c.channel_name,sum(v.viewCount) as total_views FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id GROUP BY channel_name ORDER BY sum(view_count) DESC"
        elif question == ques8:
            query = "SELECT c.channel_name, COUNT(v.video_id) as videos_published_in_2022 FROM video_details as v JOIN channel_details as c JOIN playlist_details as p ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id WHERE v.published_at LIKE '2022%' GROUP BY c.channel_name"
        elif question == ques9:
            query = "SELECT C.channel_name, AVG(v.duration) FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id GROUP BY c.channel_id"
        elif question == ques10:
            query = "SELECT c.channel_name, v.title, v.commentCount FROM channel_details as c JOIN playlist_details as p JOIN video_details as v ON v.playlist_id = p.playlist_id and p.channel_id = c.channel_id ORDER BY comment_count DESC"
        cursor.execute(query)
        results = cursor.fetchall()


if __name__ == '__main__':
    app()