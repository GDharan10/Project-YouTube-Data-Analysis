import pymongo

client = pymongo.MongoClient("mongodb+srv://Giri:giridharan@cluster0.oufax3z.mongodb.net/?retryWrites=true&w=majority")
db = client['YouTube_Data']

from pprint import pprint
from googleapiclient.discovery import build
api_key = 'AIzaSyAa2B9kn_-l_QiS02N10aVWxp0rVp9fihk'
api_service_name = "youtube"
api_version = "v3"

youtube = build(
        api_service_name, api_version, developerKey=api_key )

def channel_info(youtubeid):
  request = youtube.channels().list(
                                    part="snippet,contentDetails,statistics",
                                    id = youtubeid
                                    )
  response = request.execute()

  for i in response['items']:
    data = dict(
                Channel_Name = i['snippet']['title'],
                Channel_Id = i['id'],
                Subscription_Count = i['statistics']['subscriberCount'],
                Channel_Views = i['statistics']['viewCount'],
                Channel_Description = i['snippet']['description'],
                Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads']
                )
    return data

def video_ids(youtubeid):
  videoids = []
  channel = channel_info(youtubeid)
  playlist_id = channel['Playlist_Id']

  request = youtube.playlistItems().list(
                                        part = "snippet",
                                        maxResults = 10,
                                        playlistId = playlist_id
                                        )
  response = request.execute()
  for i in range(len(response['items'])):
    response = request.execute()['items'][i]['snippet']['resourceId']['videoId']
    videoids.append(response)
  return videoids


def video_info(videoids):
  videoinfo = []
  for i in videoids:
    request = youtube.videos().list(
                                    part = "snippet,contentDetails,statistics",
                                    id = i
                                    )
    response = request.execute()
    for j in response['items']:
      data = dict(
                  Video_Id = j['id'],
                  Video_Name = j['snippet']['title'],
                  Video_Description = j['snippet']['description'],
                  Tags = j.get('tags'),
                  PublishedAt = j['snippet']['publishedAt'],
                  View_Count = j['statistics']['viewCount'],
                  Like_Count = j.get('likeCount'),
                  Dislike_Count = j.get('dislikeCount'),
                  Favorite_Count = j.get('favoriteCount'),
                  Comment_Count = j.get('commentCount'),
                  Duration = j['contentDetails']['duration'],
                  Thumbnail = j.get('thumbnails'),
                  Caption_Status = j['contentDetails']['caption'],
                  Comments = comments_info(i)
                  )
      videoinfo.append(data)
  return videoinfo

def comments_info(videoids):
  commentsinfo = []
  try:
    for i in videoids:
      request = youtube.commentThreads().list(
                                              part = "snippet,replies",
                                              maxResults = 1,
                                              videoId = i
                                              )
      response = request.execute()

      for j in response['items']:
        data = dict(
                    Comment_Id = j['snippet']['topLevelComment']['id'],
                    Comment_Text = j['snippet']['topLevelComment']['snippet']['textOriginal'],
                    Comment_Author = j['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_PublishedAt = j['snippet']['topLevelComment']['snippet']['publishedAt']
                    )
        commentsinfo.append(data)
  except:
    pass
  return commentsinfo

def play_list(youtubeid):
  playlist = []
  request = youtube.playlists().list(
          part = "snippet,contentDetails",
          channelId = youtubeid,
          maxResults = 25
      )
  response = request.execute()

  for i in response['items']:
    data = dict(
                playlist_id = i['id'],
                Channel_Id = i['snippet']['channelId'],
                playlist_name = i['snippet']['title']
                )
    playlist.append(data)
  return playlist

def data_extraction(youtubeid):
  channelinfo = channel_info(youtubeid)
  videosids = video_ids(youtubeid)
  videoinfo = video_info(videosids)
  commentsinfo = comments_info(videosids)
  playlist = play_list(youtubeid)

  collection = db["Data Extraction from Youtube to MongoDB"]
  collection.insert_one({
                        "Channel": channelinfo,
                        "Playlist": playlist,
                        "Comments": commentsinfo,
                        "Videos": videoinfo
                        })
  return
