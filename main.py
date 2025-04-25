import os
import time
from dotenv import load_dotenv
from googleapiclient.discovery import build

import isodate
from datetime import datetime

load_dotenv()

def fetch_trending_shorts(target_shorts_count=50):
    # 1. CONFIG
    API_KEY = os.getenv("GOOGLE_API_KEY")
    REGION = "US"
    MAX_RESULTS = 100  # max allowed per request
    
    # 2. BUILD SERVICE
    youtube = build("youtube", "v3", developerKey=API_KEY)
    
    # 3. FETCH AND ACCUMULATE SHORTS
    shorts_dict = {}
    attempts = 0
    max_attempts = 10  # to prevent infinite loops
    
    print(f"Starting to collect trending videos that are 60 seconds or less...")
    
    while len(shorts_dict) < target_shorts_count and attempts < max_attempts:
        try:
            response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                chart="mostPopular",
                regionCode=REGION,
                maxResults=MAX_RESULTS,
            ).execute()
            
            items = response.get("items", [])
            
            # Process videos
            for vid in items:
                if len(shorts_dict) >= target_shorts_count:
                    break
                    
                vid_id = vid["id"]
                snippet = vid["snippet"]
                stats = vid.get("statistics", {})
                # parse ISO 8601 duration into seconds
                duration = isodate.parse_duration(vid["contentDetails"]["duration"])
                sec = duration.total_seconds()
                
                if sec <= 60 and snippet["title"] not in shorts_dict:
                    title = snippet["title"]
                    url = f"https://youtu.be/{vid_id}"
                    metadata = {
                        "viewCount": int(stats.get("viewCount", 0)),
                        "likeCount": int(stats.get("likeCount", 0)),
                        "commentCount": int(stats.get("commentCount", 0)),
                        "publishedAt": snippet["publishedAt"],
                        "durationSeconds": sec,
                        "categoryId": snippet["categoryId"],
                    }
                    channel = snippet["channelTitle"]
                    shorts_dict[title] = [url, metadata, channel]
            
            print(f"Current count: {len(shorts_dict)} videos (60 seconds or less)")
            
            if len(shorts_dict) < target_shorts_count:
                print("Waiting 10 seconds before next request...")
                time.sleep(10)  # 10 second delay between requests
                
            attempts += 1
            
        except Exception as e:
            print(f"Error occurred: {e}")
            break
    
    # 5. RESULT
    print(f"\nFound {len(shorts_dict)} trending videos that are 60 seconds or less:")
    print("\nDetailed Results:")
    print("-" * 80)
    for t, (u, m, c) in shorts_dict.items():
        print(f"Title: {t}")
        print(f"Channel: {c}")
        print(f"URL: {u}")
        print(f"Duration: {m['durationSeconds']} seconds")
        print(f"Views: {m['viewCount']:,}")
        print(f"Likes: {m['likeCount']:,}")
        print(f"Comments: {m['commentCount']:,}")
        print(f"Published: {m['publishedAt']}")
        print("-" * 80)
    
    return shorts_dict

if __name__ == "__main__":
    shorts = fetch_trending_shorts(50)
