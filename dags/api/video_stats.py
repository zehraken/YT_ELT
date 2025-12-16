import requests
import json
from datetime import date


from airflow.decorators import task
from airflow.models import Variable

#import os 
#from dotenv import load_dotenv
#load_dotenv(dotenv_path= "./.env")
#API_KEY = os.getenv("API_KEY")
#CHANNEL_NAME = "MrBeast"

#CALLING AIRFLOW VARS
API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50

@task
def get_playlist_id():
    
    try:

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        # Raise an exception for HTTP errors
        response.raise_for_status()

        #print(response.json())
        #print(response.status_code)
        #print(response.text)
        #print(response)
        #print(response.headers)
        #print(response.cookies)
        #print(response.elapsed)
        #print(response.content)

        data = response.json()
        #json_object = json.dumps(data, indent=4) -> gives the JSon output in a pretty format
        #print(json_object)


        #$["items"][0]["contentDetails"]["relatedPlaylists"] -> the dolar sign here is replaced by data
        channel_items = data["items"][0] 
        #get the uploads child from relatedPlaylists
        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        # data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"] = same output data.items[0].contentDetails.relatedPlaylists.uploads
        
        #to test results
        #print(channel_playlistId)
        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise
    
@task
def get_video_ids(playlist_id):

    video_ids = []

    pageToken = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlist_id}&key={API_KEY}"

    try:

        while True:

            url = base_url
            
            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            pageToken = data.get("nextPageToken")
            if not pageToken:
                break

        return video_ids
    
    except requests.exceptions.RequestException as e:
        raise e

def batch_list(video_id_lst, batch_size ):

    for video_id in range(0, len(video_id_lst), batch_size):
        yield video_id_lst[video_id: video_id + batch_size]



@task
def extract_video_data(video_ids):

    extracted_data = []

    def batch_list(video_id_lst, batch_size ):

        for video_id in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[video_id: video_id + batch_size]

    try:

        for batch in batch_list(video_ids, maxResults):

            video_ids_str = ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]   
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]

                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"], #snippet.get("publishedAt"),
                    "duration": contentDetails["duration"], #contentDetails.get("duration"),
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                }   
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e

@task
def save_to_json(extracted_data):

    file_path = f"./data/YT_data_{date.today().isoformat()}.json"
    #file_path = f"./data/YT_data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)

# Example usage
# if the main script is being run directly (not imported as a module) the main part will be run
if __name__ == "__main__":
    playlistId = get_playlist_id()      
    video_ids = get_video_ids(playlistId)
    video_data = extract_video_data(video_ids)
    save_to_json(video_data)
    #print(playlistId)
    #print("Module is run directly, function executed.")



#else:
    #import example
    # create a py file, and import this py file there
    # import video_stats
    # print("Module imported, function not executed.")
#    print("Module imported, function not executed.")
