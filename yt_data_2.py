from googleapiclient.discovery import build

def get_channel_videos(api_key, channel_id):
    youtube = build('youtube', 'v3', developerKey=api_key)

    request = youtube.search().list(
        part='id',
        channelId=channel_id,
        maxResults=1,  # change this as per your requirement
        type='video'
    )
    response = request.execute()
    print(response)
    for item in response['items']:
        video_id = item['id']['videoId']
        video_request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        video_response = video_request.execute()
        for video in video_response['items']:
            print(1)

api_key = 'AIzaSyAiLu35C7PfSNssGea0emeToJFwkaqY1xc'
channel_id = 'UCCJsQKOKArvDksacfT2ryQw'
get_channel_videos(api_key, channel_id)
