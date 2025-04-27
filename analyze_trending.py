import os
import time
import json
import csv
import logging
from datetime import datetime
from dotenv import load_dotenv
from googleapiclient.discovery import build
import isodate
from collections import defaultdict
import sys
import traceback
from db.firebase_storage import FirebaseStorage
from colorama import init, Fore, Style

# Initialize colorama
init()

# Custom formatter for colored logging
class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to different log levels"""
    
    def format(self, record):
        # Add colors based on log level
        if record.levelno >= logging.ERROR:
            color = Fore.RED
        elif record.levelno >= logging.WARNING:
            color = Fore.YELLOW
        elif record.levelno >= logging.INFO:
            color = Fore.GREEN
        else:
            color = Fore.WHITE
            
        # Add color to the message
        record.msg = f"{color}{record.msg}{Style.RESET_ALL}"
        return super().format(record)

# Set up logging with colors
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Create formatters
    colored_formatter = ColoredFormatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler (no colors)
    file_handler = logging.FileHandler('youtube_analysis.log')
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    # Console handler (with colors)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(colored_formatter)
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# Initialize logger
logger = setup_logging()

load_dotenv()

# Initialize Firebase storage
firebase_storage = FirebaseStorage()

# YouTube video categories mapping
YOUTUBE_CATEGORIES = {
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "19": "Travel & Events",
    "20": "Gaming",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology",
    "29": "Nonprofits & Activism"
}

def get_category_name(category_id):
    """
    Convert YouTube category ID to human-readable name.
    
    Args:
        category_id (str): YouTube category ID
    
    Returns:
        str: Human-readable category name
    """
    return YOUTUBE_CATEGORIES.get(str(category_id), f"Unknown Category ({category_id})")

def save_checkpoint(checkpoint_data, checkpoint_file='checkpoint.json'):
    """Save current progress to Firebase."""

    return firebase_storage.save_checkpoint(checkpoint_data)

def load_checkpoint(checkpoint_file='checkpoint.json'):
    """Load progress from Firebase."""
    return firebase_storage.load_checkpoint()

def fetch_trending_videos(total_videos=200, batch_size=50, region="US", delay=10, checkpoint_file='checkpoint.json'):
    """
    Fetches trending videos in batches using YouTube API.
    
    Args:
        total_videos (int): Total number of videos to fetch
        batch_size (int): Number of videos per request (max 50)
        region (str): Region code for trending videos
    
    Returns:
        list: List of video items from YouTube API
    """
    API_KEY = os.getenv("GOOGLE_API_KEY")
    if not API_KEY:
        logging.error("No API key found in environment variables")
        return None

    youtube = build("youtube", "v3", developerKey=API_KEY)
    all_items = []
    next_page_token = None
    request_count = 0  # Counter for API requests
    
    # Load checkpoint if exists
    checkpoint = load_checkpoint(checkpoint_file)
    if checkpoint:
        all_items = checkpoint.get('videos', [])
        next_page_token = checkpoint.get('next_page_token')
        request_count = checkpoint.get('request_count', 0)  # Load request count from checkpoint
        logging.info(f"Resuming from checkpoint with {len(all_items)} videos and {request_count} previous requests")

    try:
        while len(all_items) < total_videos:
            try:
                request_count += 1  # Increment request counter
                logging.info(f"Request #{request_count}: Fetching batch of {batch_size} videos... (Current total: {len(all_items)}/{total_videos})")
                request = youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    chart="mostPopular",
                    regionCode=region,
                    maxResults=batch_size,
                    pageToken=next_page_token
                )
                
                response = request.execute()
                items = response.get("items", [])
                all_items.extend(items)
                
                logging.info(f"Request #{request_count}: Fetched {len(items)} videos. Total: {len(all_items)}/{total_videos}")
                
                next_page_token = response.get("nextPageToken")
                
                # Save checkpoint with request count
                checkpoint_data = {
                    'videos': all_items,
                    'next_page_token': next_page_token,
                    'timestamp': datetime.now().isoformat(),
                    'request_count': request_count
                }
                save_checkpoint(checkpoint_data, checkpoint_file)
                
                if not next_page_token:
                    logging.warning("No more pages available from YouTube API")
                    break
                    
                if len(all_items) >= total_videos:
                    logging.info(f"Reached target of {total_videos} videos")
                    break
                    
                logging.info(f"Waiting {delay} seconds before next request...")
                time.sleep(delay)
                
            except Exception as e:
                if "quotaExceeded" in str(e):
                    logging.error(f"YouTube API quota exceeded after {request_count} requests. Please try again later.")
                    break
                logging.error(f"Error in batch request #{request_count}: {e}")
                logging.error(traceback.format_exc())
                # Wait longer on error
                time.sleep(delay * 2)
                continue
        
        if len(all_items) < total_videos:
            logging.warning(f"Could only fetch {len(all_items)} videos out of {total_videos} requested after {request_count} requests")
        
        return all_items[:total_videos]
        
    except Exception as e:
        logging.error(f"Fatal error in fetch_trending_videos after {request_count} requests: {e}")
        logging.error(traceback.format_exc())
        return all_items

def analyze_videos(videos):
    """
    Analyzes a list of videos and categorizes them into shorts and regular videos.
    
    Args:
        videos (list): List of video items from YouTube API
    
    Returns:
        dict: Analysis results including shorts, regular videos, and category stats
    """
    if not videos:
        logging.error("No videos to analyze")
        return None
        
    # 1. ANALYZE VIDEOS
    shorts = []
    regular_videos = []
    category_stats = defaultdict(int)
    
    for vid in videos:
        vid_id = vid["id"]
        snippet = vid["snippet"]
        stats = vid.get("statistics", {})
        duration = isodate.parse_duration(vid["contentDetails"]["duration"])
        sec = duration.total_seconds()
        
        video_data = {
            "title": snippet["title"],
            "channel": snippet["channelTitle"],
            "url": f"https://youtu.be/{vid_id}",
            "duration": sec,
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
            "publishedAt": snippet["publishedAt"],
            "categoryId": snippet["categoryId"],
            "categoryName": get_category_name(snippet["categoryId"])
        }
        
        # Categorize video
        if sec <= 60:
            shorts.append(video_data)
        else:
            regular_videos.append(video_data)
        
        category_stats[snippet["categoryId"]] += 1
    
    # 2. PRINT RESULTS
    print("\n=== TRENDING VIDEOS ANALYSIS ===")
    print(f"Total videos analyzed: {len(videos)}")
    print(f"Shorts (≤60s): {len(shorts)} ({len(shorts)/len(videos)*100:.1f}%)")
    print(f"Regular videos (>60s): {len(regular_videos)} ({len(regular_videos)/len(videos)*100:.1f}%)")
    
    print("\n=== TOP SHORTS ===")
    for i, short in enumerate(sorted(shorts, key=lambda x: x["views"], reverse=True)[:5], 1):
        print(f"\n{i}. {short['title']}")
        print(f"   Channel: {short['channel']}")
        print(f"   Category: {short['categoryName']}")
        print(f"   Duration: {short['duration']} seconds")
        print(f"   Views: {short['views']:,}")
        print(f"   URL: {short['url']}")
    
    print("\n=== TOP REGULAR VIDEOS ===")
    for i, video in enumerate(sorted(regular_videos, key=lambda x: x["views"], reverse=True)[:5], 1):
        print(f"\n{i}. {video['title']}")
        print(f"   Channel: {video['channel']}")
        print(f"   Category: {video['categoryName']}")
        print(f"   Duration: {video['duration']} seconds")
        print(f"   Views: {video['views']:,}")
        print(f"   URL: {video['url']}")
    
    print("\n=== CATEGORY DISTRIBUTION ===")
    for category_id, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
        category_name = get_category_name(category_id)
        print(f"{category_name}: {count} videos ({count/len(videos)*100:.1f}%)")
    
    return {
        "shorts": shorts,
        "regular_videos": regular_videos,
        "category_stats": dict(category_stats)
    }

def save_results_to_firebase(results):
    """
    Save analysis results to Firebase.
    
    Args:
        results (dict): Analysis results from analyze_videos()
    """
    if not results:
        logging.error("No results to save")
        return
    
    try:
        # Save to Firebase
        result_id = firebase_storage.save_analysis_results(results)
        if result_id:
            logging.info(f"Results saved successfully with ID: {result_id}")
        else:
            logging.error("Failed to save results to Firebase")
            
    except Exception as e:
        logging.error(f"Error saving results: {e}")
        logging.error(traceback.format_exc())

def cleanup():
    """Cleanup function to remove checkpoint file."""
    print('starting cleanup')
    if os.path.exists('checkpoint.json'):
        os.remove('checkpoint.json')
        print(" checkpoint.json removed")
    if os.path.exists('youtube_analysis.log'):
        os.remove('youtube_analysis.log')
        print(" youtube_analysis.log removed")
    if os.path.exists('trending.csv'):
        os.remove('trending.csv')
        print(" trending.csv removed")
    # logging.info("Checkpoint file removed")
    print(" cleanup dong Checkpoint file removed")


def main():
    try:
        total_videos = 100000  # Large number for continuous collection
        batch_size = 50
        region = "US"
        delay = 1  # Longer delay for VM
        
        logging.info("Starting YouTube trending videos analysis")
        logging.info(f"Configuration: total_videos={total_videos}, batch_size={batch_size}, region={region}, delay={delay}")
        
        # Fetch videos
        videos = fetch_trending_videos(
            total_videos=total_videos,
            batch_size=batch_size,
            region=region,
            delay=delay
        )
        
        if videos:
            # Analyze videos
            results = analyze_videos(videos)
            if results:
                # Save results to Firebase
                save_results_to_firebase(results)
            return results
            
    except Exception as e:
        logging.error(f"Fatal error in main: {e}")
        logging.error(traceback.format_exc())
        return None

if __name__ == "__main__":
    cleanup()
    results = main() 