# Importing the package and making the connection that is required for this project

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
api_key = "AIzaSyCzCso-tR1Ga1aVW2TyIqUFLvyhcWe5bIo"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey = api_key)

import pymongo
client = pymongo.MongoClient("mongodb+srv://giri:giri1005@cluster0.oufax3z.mongodb.net/?retryWrites=true&w=majority")
db = client['YouTube_Project']
collection = db["Extracted_Data"]

import psycopg2
mydb = psycopg2.connect(
                        user = 'postgres',
                        password = '1005',
                        host = 'localhost',
                        database = 'youtube',
                        port = 5432)
mycursor = mydb.cursor()


from sqlalchemy import create_engine, String, Integer, DateTime, Interval, Text

engine = create_engine('postgresql://postgres:1005@localhost/youtube')

import pandas as pd
import streamlit as st

# Extracting data from the YouTube Data API

def Channel_Document (Channel_Id):

    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=Channel_Id
        )
    response = request.execute()

    # Extract data from the channels list
    channel_data = {
            'channel_name': response['items'][0]['snippet']['title'],
            'channel_id': response['items'][0]['id'],
            'subscribers_count': response['items'][0]['statistics']['subscriberCount'],
            'total_video_count': response['items'][0]['statistics']['videoCount'],
            'playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }
    
    return channel_data


def Get_Video_ids (Channel_Id):
    
    Video_ids = []
    next_page_token = None
    channel = Channel_Document(Channel_Id)
    playlist_id = channel['playlist_id']

    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            maxResults=50,
            playlistId=playlist_id,
            pageToken=next_page_token
        )
        response = request.execute()
        
        # Extract Video ids from the playlistItems
        for item in response['items']:
            video_id = item['snippet']['resourceId']['videoId']
            Video_ids.append(video_id)

        # Check if there are more pages
        if 'nextPageToken' in response:
            next_page_token = response['nextPageToken']
        else:
            break
    
    return Video_ids


def Comments_Document(Video_ids):
  
  comments_info_list = []

  for Video_id in Video_ids:
        try:
          request = youtube.commentThreads().list(
                                                  part = "snippet,replies",
                                                  maxResults = 10,
                                                  videoId = Video_id
                                                  )
          response = request.execute()

          
          for item  in response.get('items'):
            top_level_comment = item['snippet']['topLevelComment']['snippet']
            if top_level_comment:
                      comment_data = {
                          'video_id': top_level_comment.get('videoId'),
                          'comment_text': top_level_comment.get('textOriginal'),
                          'comment_author': top_level_comment.get('authorDisplayName'),
                          'comment_publishedAt': top_level_comment.get('publishedAt')
                      }
                      comments_info_list.append(comment_data)
        except HttpError as e:
            if e.resp.status == 403:
                print(f"Comments are disabled for video ID {Video_id}.")
            else:
                print(f"An error occurred while fetching comments for video ID {Video_id}: {e}")
  
  return comments_info_list


def Videos_Document(Video_ids):
  
  videos_info_list = []

  for Video_id in Video_ids:
    request = youtube.videos().list(
                                    part = "snippet,contentDetails,statistics",
                                    id = Video_id
                                    )
    response = request.execute()

    for item in response['items']:
      video_data = dict(
                  channel_id = item['snippet']['channelId'],
                  video_id = item['id'],
                  video_name = item['snippet']['title'],
                  publishedat = item['snippet']['publishedAt'],
                  view_count = item['statistics'].get('viewCount'),
                  like_count = item['statistics'].get('likeCount'),
                  comment_count = item['statistics'].get('commentCount'),
                  duration = item['contentDetails']['duration'],
                  comments = Comments_Document([Video_id])
                  )
      videos_info_list.append(video_data)
  return videos_info_list

# Extraction of data from the YouTube Data API to the MongoDB data lake

def Store_data_in_MongoDB(Channel_Id):
    Channel_Collection = Channel_Document(Channel_Id)
    Video_ids = Get_Video_ids (Channel_Id)
    Comments_Collection = Comments_Document(Video_ids)
    Videos_Collection = Videos_Document(Video_ids)
    
    collection = db["Extracted_Data"]
    collection.insert_one({
                            "Channel": Channel_Collection,
                            "Videos": Videos_Collection
                            })
    return "Data stored successfully in MongoDB."

# From the MongoDB data lake, the data is stored in PostgreSQL databases.

def create_channel_table(selected_channel):
    channel_data = [ ( collection.find_one({'Channel.channel_name': selected_channel}, {"_id": 0, "Channel": 1}) ["Channel"] ) ]

    df = pd.DataFrame(channel_data)

    dtype_mapping = {
        'channel_name' : String(100),
        'channel_id' : String(80),
        'subscribers_count' : Integer,
        'total_Video_count' : Integer,
        'playlist_id' : String(80),
    }

    df.to_sql('channel', engine, if_exists='append', index=False, dtype=dtype_mapping)


def create_video_table(selected_channel):
    Videos_list = []
    for videos_data in collection.find({'Channel.channel_name': selected_channel},{"_id":0,"Videos":1}):
        for i in range(len(videos_data["Videos"])):
            Videos_list.append(videos_data["Videos"][i])
    df = pd.DataFrame(Videos_list)

    df_without_comments = df.drop(columns=['comments'])

    dtype_mapping = {
        'channel_id': String(50),
        'video_id': String(20),
        'video_name': String,
        'publishedat': DateTime,
        'view_count': Integer,
        'like_count': Integer,
        'comment_count': Integer,
        'duration': Interval,
    }

    df_without_comments.to_sql('videos', engine, if_exists='append', index=False, dtype=dtype_mapping)


def create_comment_table(selected_channel):
    Comments_list = []
    db = client['YouTube_Project']
    collection = db["Extracted_Data"]
    for document in collection.find({'Channel.channel_name': selected_channel}, {"_id": 0, "Videos.comments": 1}):
        videos = document.get("Videos", [])
        for video in videos:
            video_comments = video.get("comments", [])
            for comment in video_comments:
                Comments_list.append(comment)
    df = pd.DataFrame(Comments_list)

    dtype_mapping = {
            'video_id': String(20),
            'comment_text': Text,
            'comment_author': String(50),
            'comment_publishedAt': DateTime,
        }

    df.to_sql('videocomments', engine, if_exists='append', index=False, dtype=dtype_mapping)


def create_table(selected_channel):
    create_channel_table(selected_channel)
    create_video_table(selected_channel)
    create_comment_table(selected_channel)
    return "Table created successfully"

# Using streamlit for providing an interactive interface to query and analyze the collected data.

st.set_page_config(
    page_title="Youtube Data Harvesting",
    page_icon=":books:",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'About': """Welcome to the YouTube Data Harvesting project! This application is designed to collect,
                 store, and analyze data from various YouTube channels."""
    }
)

def styled_text(text, color="black", font_size=None, alignment="left", bold=False, background_color=None, bullet_points=False):
    style = ""
    if color:
        style += f"color: {color};"
    if font_size:
        style += f"font-size: {font_size}px;"
    if alignment:
        style += f"display: block; text-align: {alignment};"
    if bold:
        style += "font-weight: bold;"
    if background_color:
        style += f"background-color: {background_color};"

    if bullet_points:
        text = "<ul>" + "".join([f"<li>{line}</li>" for line in text.split("\n")]) + "</ul>"

    if style:
        text = f'<span style="{style}">{text}</span>'
    return text


def home_page():

    st.markdown(styled_text("Welcome to Youtube Data Harvesting & Warehousing", 
                            color="green", font_size="50", alignment="center", bold = True), unsafe_allow_html=True)

    row1 = st.columns(1)
    row2 = st.columns(1)
    row3 = st.columns(3)

    with row1[0]:
        with st.container():
            st.image('https://www.gstatic.com/youtube/img/branding/youtubelogo/svg/youtubelogo.svg', use_column_width=True)
    
    
    with row2[0]:
        st.title("**Instructions:**")
        st.write("Follow the below steps to use the application effectively:")
        st.write("---")
    
    add_channel_instructions = """
    <b>Navigate to "Add New Channel"</b>
    <ul>
    <li><b>Locate the Sidebar:</b> When you run the Streamlit application, you'll see a sidebar on the left-hand side of the screen. This sidebar contains options for navigation.</li>
    <li><b>Find the "Add & Migrate" Option:</b> In the sidebar, look for an option titled "Add & Migrate". This option should be one of the choices listed.</li>
    <li><b>Select "Add New Channel":</b> After clicking on "Add & Migrate", you'll see a dropdown or radio button list with options. Choose the "Add new channel" option from this list.</li>
    <li><b>Input Channel ID:</b> In the main area of the application, you'll be presented with a form where you can input the YouTube channel ID for the analysis.</li>
    <li><b>Submit Data:</b> Once you've entered the channel ID, there should be a button labeled "Collect data" or similar. Click on this button to initiate the process of collecting data from the specified YouTube channel.</li>
    <li><b>Confirmation:</b> If the data collection is successful, you should see a confirmation message indicating that the channel data has been uploaded successfully.</li>
    <li><b>Error Handling:</b> If there are any issues during the data collection process (such as invalid channel ID or API errors), appropriate error messages will be displayed on the interface to inform you about the problem.</li>
    </ul>
    """

    migrate_data_to_sql = """
    <b>Navigate to "Migrate"</b>
    <ul>
    <li><b>Locate the Sidebar:</b> When you run the Streamlit application, you'll see a sidebar on the left-hand side of the screen. This sidebar contains options for navigation.</li>
    <li><b>Find the "Add & Migrate" Option:</b> In the sidebar, look for an option titled "Add & Migrate". This option should be one of the choices listed.</li>
    <li><b>Select "Migrate":</b> After clicking on "Add & Migrate", you'll see a dropdown or radio button list with options. Choose the "Migrate" option from this list.</li>
    <li><b>Channel Selection:</b> In the main area of the application, you'll be presented with a form or dropdown where you can select the YouTube channel(s) you want to migrate to SQL.</li>
    <li><b>Initiate Migration:</b> Once you've selected the channel(s) for migration, there should be a button labeled "Migrate" or similar. Click on this button to start the migration process.</li>
    <li><b>Confirmation:</b> If the migration process is successful, you should see a confirmation message indicating that the channel information has been added to the SQL database successfully.</li>
    <li><b>Error Handling:</b> If there are any issues during the migration process (such as database connection errors or SQL query problems), appropriate error messages will be displayed on the interface to inform you about the problem.</li>
    </ul>
    """

    query_instructions = """
    <b>Navigate to "Query"</b>
    <ul>
    <li><b>Locate the Sidebar:</b> When you run the Streamlit application, you'll see a sidebar on the left-hand side of the screen. This sidebar contains options for navigation.</li>
    <li><b>Find the "Access data" Option:</b> In the sidebar, look for an option titled "Access data". This option should be one of the choices listed.</li>
    <li><b>Select "Query":</b> After clicking on "Access data", you'll see a dropdown or radio button list with options. Choose the "Query" option from this list.</li>
    <li><b>Select Channels:</b> In the main area of the application, you'll be prompted to select the YouTube channel(s) for which you want to execute queries.</li>
    <li><b>Choose Query:</b> Once you've selected the channel(s), you'll see a dropdown or list of predefined queries that you can choose from. Each query represents a specific type of analysis or data retrieval operation.</li>
    <li><b>Execute Query:</b> After selecting a query, there should be a button labeled "Show result" or similar. Click on this button to execute the chosen query.</li>
    <li><b>View Results:</b> If the query execution is successful, you'll see the results displayed in the application interface. The results might be presented in the form of tables or other visualizations, depending on the nature of the query.</li>
    <li><b>Error Handling:</b> If there are any issues during the query execution process (such as invalid SQL syntax or database connection errors), appropriate error messages will be displayed on the interface to inform you about the problem.</li>
    </ul>
    """
    
    with row3[0]:
        # Display the inserting new channel instructions in a container.
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid #ccc; padding: 10px; background-color: #f0f0f0; height: auto;">
            {styled_text(add_channel_instructions)}
            """, unsafe_allow_html=True)
    
    with row3[1]:
        # Display the migrating instructions in a container
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid #ccc; padding: 10px; background-color: #f0f0f0; height: auto;">
            {styled_text(migrate_data_to_sql)}
            """, unsafe_allow_html=True)

    with row3[2]:
        # Display the Query instructions in a container
        with st.container():
            st.markdown(f"""
            <div style="border: 1px solid #ccc; padding: 10px; background-color: #f0f0f0; height: auto;">
            {styled_text(query_instructions)}
            """, unsafe_allow_html=True)
    
def add_new_channel():
    
    st.markdown(styled_text("Add new channel", color = "green", alignment= "center", bold=True,
                            font_size= 28), unsafe_allow_html=True)

    st.write("Here, you can input the channel ID for the analysis..")
    
    with st.form(key='new_channel_form'):
        Channel_Id = st.text_input("Enter the channel ID")

        submit_button = st.form_submit_button(label='Collect data')

        if submit_button:
            ch_ids=[]
            db = client['YouTube_Project']
            collection = db["Extracted_Data"]
            for i in collection.find({},{"_id":0,"Channel":1}):
                ch_ids.append(i["Channel"]["channel_id"])

            if Channel_Id in ch_ids:
                st.error("Given channel already exists")
            else:
                Store_data_in_MongoDB(Channel_Id)
                st.success("Given channel is uploaded successfully")

def migrate():
    channel_names = []
    for doc in collection.find({}, {"_id": 0, "Channel": 1}):
        channel_names.append(doc["Channel"]["channel_name"])
    channel_names.sort()

    st.markdown(styled_text("Migrate to SQL", color = "green", alignment= "center", bold=True,
                            font_size= 28), unsafe_allow_html=True)

    st.write("Here, you can choose the channel to migrate..")
    
    with st.form(key='migrate_form'):
        selected_channel = st.selectbox("Choose channel to migrate", channel_names, placeholder="Choose channel")
        
        submit_button = st.form_submit_button(label='Migrate')

        if submit_button:
            channel_names = []
            query = "SELECT channel_name FROM channel"
            try:
                mycursor.execute(query)
                result = mycursor.fetchall()
                for row in result:
                    channel_names.append(row[0])
                
            except Exception:
                st.success("A table is created, and the first information in your analysis is updated.")

            if selected_channel in channel_names:
                st.error("Given channel already exists")
            else:
                create_table(selected_channel)
                st.success("Channel information added successfully")

def choose_channels_sql():
    channel_names = ['Select All']
    query = "SELECT channel_name FROM channel"
    mycursor.execute(query)
    result = mycursor.fetchall()
    for row in result:
        channel_names.append(row[0])
    channel_names.sort()

    st.markdown("### Select Channels")
    selected_channels = tuple(st.multiselect('Choose \"Select All\" if you want all the data',
                                             channel_names, placeholder="Choose channels for analysis"))
    if 'Select All' in selected_channels:
        select_all_index = channel_names.index('Select All')
        selected_channels = channel_names[:select_all_index] + channel_names[select_all_index+1:]
    return selected_channels


def show_channel_table(selected_channels, placeholders):
    query = f"SELECT * FROM channel WHERE channel_name IN ({placeholders})"
    mycursor.execute(query, selected_channels)
    table = mycursor.fetchall()
    dataframe = pd.DataFrame(table, columns=[i[0] for i in mycursor.description])
    st.write(dataframe)


def show_video_table(selected_channels, placeholders):
    query = f'''SELECT v.* FROM videos v 
                JOIN channel c ON v.channel_id = c.channel_id
                WHERE c.channel_name IN ({placeholders})'''
    mycursor.execute(query, selected_channels)
    table = mycursor.fetchall()
    dataframe = pd.DataFrame(table, columns=[i[0] for i in mycursor.description])
    st.write(dataframe)

def show_comment_table(selected_channels, placeholders):
    query = f'''SELECT v.video_name, vc.* FROM videocomments vc 
                JOIN videos v ON vc.video_id = v.video_id 
                JOIN channel c ON v.channel_id = c.channel_id 
                WHERE c.channel_name IN ({placeholders})'''
    mycursor.execute(query, selected_channels)
    table = mycursor.fetchall()
    dataframe = pd.DataFrame(table, columns=[i[0] for i in mycursor.description])
    st.write(dataframe)


def show_tables(selected_channels):

    placeholders = ', '.join(['%s'] * len(selected_channels))
    
    option = st.multiselect("Choose Table", ['Channel Table', 'Video Table', 'Comment Table'],
                            placeholder="Select the table you want")
    show_table = st.button(label='Show Table')

    if show_table:

        if option:

            if 'Channel Table' in option:
                show_channel_table(selected_channels, placeholders)
            
            if 'Video Table' in option:
                show_video_table(selected_channels, placeholders)
            
            if 'Comment Table' in option:
                show_comment_table(selected_channels, placeholders)
        
        else:
            st.warning("Please select at least one table to show.")



def query_page(selected_channels):

    placeholders = ', '.join(['%s'] * len(selected_channels))

    st.markdown("<h1 style='text-align: center; color: green;'>Choose your query</h1>", unsafe_allow_html=True)

    question = st.selectbox("Select a query",["1. All videos and their channels.",
                                           "2. Channels with the most videos are listed with their counts",
                                           "3. Top 10 viewed videos and their channels.",
                                           "4. Comments on each video are detailed with their titles.",
                                           "5. Videos with the most likes are matched with their channels.",
                                           "6. Total likes for each video are provided with their titles",
                                           "7. Total views for each channel are listed with their names.",
                                           "8. Channels that published videos in 2022.",
                                           "9. Average video duration of each channel.",
                                           "10. Videos with the most comments are matched with their channels."
                                           ])
    
    
    if st.button("Show result"):
        mydb = psycopg2.connect(
                                user = 'postgres',
                                password = '1005',
                                host = 'localhost',
                                database = 'youtube',
                                port = 5432)
        mycursor = mydb.cursor()

        

        if question == "1. All videos and their channels.":
            query = f"""
                        SELECT v.Video_Name, c.Channel_Name
                        FROM videos v
                        JOIN channel c ON v.Channel_Id = c.channel_id
                        WHERE c.channel_name IN ({placeholders})
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of video", "Name of channel"])
            st.write(dataframe)
        
        if question == "2. Channels with the most videos are listed with their counts":
            query = f"""
                        SELECT c.channel_name, COUNT(v.video_id) AS Num_Videos
                        FROM channel c
                        JOIN videos v ON c.channel_id = v.channel_id
                        WHERE c.channel_name IN ({placeholders})
                        GROUP BY c.channel_name
                        ORDER BY Num_Videos DESC
                        LIMIT 1
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of channel", "Total no. of videos"])
            st.write(dataframe)

        if question == "3. Top 10 viewed videos and their channels.":
            query = f"""
                        SELECT v.video_name, c.channel_name, v.View_Count
                        FROM videos v
                        JOIN channel c ON c.channel_Id = v.channel_Id
                        WHERE c.channel_name IN ({placeholders})
                        ORDER BY v.View_Count DESC
                        LIMIT 10
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of video", "Name of channel", "Total views" ])
            st.write(dataframe)

        if question == "4. Comments on each video are detailed with their titles.":
            query = f"""
                        SELECT v.video_name, v.comment_count
                        FROM videos v
                        JOIN channel c ON c.channel_Id = v.channel_Id
                        WHERE c.channel_name IN ({placeholders})
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of video", "Total comments"])
            st.write(dataframe)
        
        if question == "5. Videos with the most likes are matched with their channels.":
            query = f"""
                        SELECT v.video_name, c.channel_name, v.like_count
                        FROM videos v
                        JOIN channel c ON c.channel_Id = v.channel_Id
                        WHERE c.channel_name IN ({placeholders})
                        ORDER BY v.like_count DESC
                        LIMIT 1
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of video", "Name of channel", "Total no. of likes"])
            st.write(dataframe)
        
        if question == "6. Total likes for each video are provided with their titles":
            query = f"""
                        SELECT v.video_name, 
                            SUM(v.like_count) AS Total_Likes
                        FROM videos v
                        JOIN channel c ON c.channel_Id = v.channel_Id
                        WHERE c.channel_name IN ({placeholders})
                        GROUP BY v.video_name
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of video", "Total no. of likes"])
            st.write(dataframe)
        
        if question == "7. Total views for each channel are listed with their names.":
            query = f"""
                        SELECT c.channel_name, SUM(v.view_count) AS Total_Views
                        FROM channel c
                        JOIN videos v ON c.channel_Id = v.channel_Id
                        WHERE c.channel_name IN ({placeholders})
                        GROUP BY c.channel_name
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of channel", "Total no. of views"])
            st.write(dataframe)
        
        if question == "8. Channels that published videos in 2022.":
            query = f"""
                        SELECT DISTINCT c.channel_name
                        FROM channel c
                        JOIN videos v ON c.channel_id = v.channel_id
                        WHERE EXTRACT(YEAR FROM v.publishedat) = 2022
                        AND c.channel_name IN ({placeholders})
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of channel"])
            st.write(dataframe)

        if question == "9. Average video duration of each channel.":
            query = f"""
                        SELECT c.channel_name, AVG(v.duration) AS Average_Duration
                        FROM channel c
                        JOIN videos v ON c.channel_Id = v.channel_Id
                        WHERE c.channel_name IN ({placeholders})
                        GROUP BY c.channel_name
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of channel", "Average duration of all videos"])
            st.write(dataframe)
        
        if question == "10. Videos with the most comments are matched with their channels.":
            query = f"""
                        SELECT v.video_name, c.channel_name, v.comment_count
                        FROM videos v
                        JOIN channel c ON c.channel_Id = v.channel_Id
                        WHERE c.channel_name IN ({placeholders})
                        ORDER BY v.comment_count DESC
                        LIMIT 1
                    """
            mycursor.execute(query, selected_channels)
            table = mycursor.fetchall()
            dataframe = pd.DataFrame(table, columns=["Name of video", "Name of channel", "Total no. of comments"])
            st.write(dataframe)



page = st.sidebar.selectbox(":blue[Choose your page]", ["Home page", "Add & Migrate", "Access data"])



if page == "Home page":
    home_page()

elif page == "Add & Migrate":
    #access_data()
    option = st.sidebar.radio("Select option", ['Add new channel', 'Migrate'])

    if option == 'Add new channel':
        add_new_channel()
    
    elif option == 'Migrate':
        migrate()
        
        
elif page == "Access data":
    option = st.sidebar.radio("Select option", ['Query', 'Tables'])

    if option == 'Query':
        selected_channels = choose_channels_sql()
        if selected_channels:
            query_page(selected_channels)
    
    elif option == 'Tables':
        selected_channels = choose_channels_sql()
        if selected_channels:
            show_tables(selected_channels)
    
        


    
