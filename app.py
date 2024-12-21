import logging
import time
import json
import os
import shutil
import tempfile
import subprocess
import signal
import asyncio
from threading import Lock
from datetime import datetime, timedelta
from pytz import timezone
import re

# Patch eventlet to support asynchronous operations
import eventlet
eventlet.monkey_patch()

# Flask-related imports
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit

# Facebook Ads SDK
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.adimage import AdImage
from facebook_business.adobjects.campaign import Campaign

# External libraries
from tqdm import tqdm
from PIL import Image

# Concurrency tools
from concurrent.futures import ThreadPoolExecutor, as_completed

# Contant variable
MAX_WORKERS = 10

# Flask app setup
app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Global variables for tasks and locks
upload_tasks = {}
tasks_lock = Lock()
process_pids = {}
canceled_tasks = set()

# Custom Exception for canceled tasks
class TaskCanceledException(Exception):
    pass

# Utility function to handle error emission through socket
def emit_error(task_id, message):
    logging.error(f"Raw error message: {message}")  # Log the raw message

    # Default error response
    default_error = {"title": "Error", "message": "An unknown error occurred."}

    # Try extracting JSON part from the raw message using regex
    json_match = re.search(r'Response:\s*(\{.*\})', message, re.DOTALL)

    if json_match:
        try:
            error_data = json.loads(json_match.group(1))
            # Extract title and message from the parsed JSON
            title = error_data.get("error", {}).get("error_user_title", default_error["title"])
            msg = error_data.get("error", {}).get("error_user_msg", default_error["message"])
        except json.JSONDecodeError:
            logging.error("Failed to parse the error JSON from the response.")
            title, msg = default_error["title"], message  # Use raw message on JSON parse failure
    else:
        title, msg = default_error["title"], message  # Use raw message if no JSON found

    # Emit error to frontend
    socketio.emit('error', {
        'task_id': task_id,
        'title': title,
        'message': msg
    })

# Common cancellation check
def check_cancellation(task_id):
    with tasks_lock:
        if task_id in canceled_tasks:
            canceled_tasks.remove(task_id)
            raise TaskCanceledException(f"Task {task_id} has been canceled")

#function to check campaign budget optimization.
def get_campaign_budget_optimization(campaign_id, ad_account_id):
    try:
        # Fetch required fields in one API call
        fields = [
            Campaign.Field.name,
            Campaign.Field.effective_status,
            Campaign.Field.daily_budget,
            Campaign.Field.lifetime_budget,
            Campaign.Field.objective
        ]
        campaign = Campaign(campaign_id).api_get(fields=fields)

        # Determine if Campaign Budget Optimization (CBO) is enabled
        is_cbo = any([campaign.get('daily_budget'), campaign.get('lifetime_budget')])

        # Return campaign details
        return {
            "name": campaign.get('name'),
            "effective_status": campaign.get('effective_status'),
            "daily_budget": campaign.get('daily_budget'),
            "lifetime_budget": campaign.get('lifetime_budget'),
            "is_campaign_budget_optimization": is_cbo,
            "objective": campaign.get('objective', "OUTCOME_TRAFFIC")  # Default objective
        }
    except Exception as e:
        logging.error(f"Error fetching campaign details: {e}")
        return None


# Function to fetch campaign budget optimization status and return a boolean value
def is_campaign_budget_optimized(campaign_id, ad_account_id):
    existing_campaign_budget_optimization = get_campaign_budget_optimization(campaign_id, ad_account_id)
    return existing_campaign_budget_optimization.get('is_campaign_budget_optimization', False)

# Function to create a campaign
def create_campaign(name, objective, budget_optimization, budget_value, bid_strategy, buying_type, task_id, ad_account_id, app_id, app_secret, access_token, is_cbo):
    # Check for task cancellation
    check_cancellation(task_id)

    try:
        # Initialize Facebook API
        FacebookAdsApi.init(app_id, app_secret, access_token, api_version='v19.0')

        # Base campaign parameters
        campaign_params = {
            "name": name,
            "objective": objective,
            "special_ad_categories": ["NONE"],
            "buying_type": buying_type,
        }

        # Only handle budget and bid strategy if buying type is 'AUCTION'
        if buying_type == "AUCTION" and is_cbo:
            budget_value_cents = int(float(budget_value) * 100)  # Convert to cents
            campaign_params.update({
                "daily_budget": budget_value_cents if budget_optimization == "DAILY_BUDGET" else None,
                "lifetime_budget": budget_value_cents if budget_optimization == "LIFETIME_BUDGET" else None,
                "bid_strategy": bid_strategy
            })

        # Create the campaign
        campaign = AdAccount(ad_account_id).create_campaign(
            fields=[AdAccount.Field.id],
            params=campaign_params
        )

        # Log campaign creation
        logging.info(f"Created campaign with ID: {campaign['id']}")
        return campaign['id'], campaign

    except Exception as e:
        # Emit error and log
        error_msg = f"Error creating campaign: {e}"
        emit_error(task_id, error_msg)
        return None, None

#fetch ad_account timezone:
def get_ad_account_timezone(ad_account_id):
    ad_account = AdAccount(ad_account_id).api_get(fields=[AdAccount.Field.timezone_name])
    return ad_account.get('timezone_name')

def convert_to_utc(local_time_str, ad_account_timezone):
    local_tz = timezone(ad_account_timezone)
    local_time = local_tz.localize(datetime.strptime(local_time_str, '%Y-%m-%dT%H:%M:%S'))
    utc_time = local_time.astimezone(timezone('UTC'))
    return utc_time.strftime('%Y-%m-%dT%H:%M:%S')

def parse_datetime_with_seconds(timestamp):
    if isinstance(timestamp, datetime):
        # Convert datetime object to string
        return timestamp.strftime('%Y-%m-%dT%H:%M:%S')

    if isinstance(timestamp, str):
        try:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S')
        except ValueError:
            if len(timestamp) == 16:  # Missing seconds
                return (datetime.strptime(timestamp + ':00', '%Y-%m-%dT%H:%M:%S')
                        .strftime('%Y-%m-%dT%H:%M:%S'))
            raise ValueError(f"Invalid timestamp format: {timestamp}")
    
    raise TypeError(f"Expected a string or datetime object, but got {type(timestamp)}") 

# Function to create an ad set
def create_ad_set(campaign_id, folder_name, videos, config, task_id):
    check_cancellation(task_id)

    try:
        # Extract common configurations
        app_events = config.get('app_events')
        gender = config.get("gender", "All")
        attribution_setting = config.get('attribution_setting', '7d_click')
        event_type = config.get('event_type', 'PURCHASE')
        is_cbo = config.get('is_cbo')
        is_existing_cbo = config.get('is_existing_cbo')
        ad_account_timezone = config.get('ad_account_timezone')

        # Parse age range with defaults
        age_min, age_max = parse_age_range(config.get("age_range", '[18, 65]'))

        # Convert app events to UTC
        app_events = parse_datetime_with_seconds(app_events)
        app_events = convert_to_utc(app_events, ad_account_timezone)
        start_time = get_start_time(app_events)

        # Determine gender value
        gender_value = get_gender_value(gender)

        # Determine publisher platforms and positions
        publisher_platforms, facebook_positions, instagram_positions, messenger_positions, audience_network_positions = determine_placements(config)

        # Set up Advantage+ Targeting or standard targeting
        if config.get('targeting_type') == 'Advantage':
            ad_set_params = setup_advantage_targeting(campaign_id, folder_name, config, start_time, event_type)
        else:
            ad_set_params = setup_standard_targeting(campaign_id, folder_name, config, start_time, age_min, age_max, gender_value, publisher_platforms, facebook_positions, instagram_positions, messenger_positions, audience_network_positions, event_type)

        # Remove any None values from the parameters
        ad_set_params = {k: v for k, v in ad_set_params.items() if v is not None}

        # Handle bid strategy and budget if CBO is not used
        ad_set_params = handle_bid_and_budget(ad_set_params, config, is_cbo, is_existing_cbo, ad_account_timezone)

        # Create the ad set
        print("Ad set parameters before creation:", ad_set_params)
        ad_set = AdAccount(config['ad_account_id']).create_ad_set(
            fields=[AdSet.Field.name],
            params=ad_set_params,
        )

        print(f"Created ad set with ID: {ad_set.get_id()}")
        return ad_set

    except Exception as e:
        error_msg = f"Error creating ad set: {e}"
        emit_error(task_id, error_msg)
        return None


def parse_age_range(age_range_str):
    try:
        age_range = json.loads(age_range_str)
        return age_range[0], age_range[1]
    except (ValueError, IndexError):
        return 18, 65  # Defaults if parsing fails


def get_start_time(app_events):
    if app_events:
        return datetime.strptime(app_events, '%Y-%m-%dT%H:%M:%S')
    else:
        return (datetime.now() + timedelta(days=1)).replace(hour=4, minute=0, second=0, microsecond=0)


def get_gender_value(gender):
    if gender == "Male":
        return [1]
    elif gender == "Female":
        return [2]
    else:
        return [1, 2]


def determine_placements(config):
    publisher_platforms, facebook_positions, instagram_positions, messenger_positions, audience_network_positions = [], [], [], [], []

    # Handle Facebook placements
    if config['platforms'].get('facebook'):
        publisher_platforms.append('facebook')
        facebook_positions = get_facebook_positions(config)

    # Handle Instagram placements
    if config['platforms'].get('instagram'):
        publisher_platforms.append('instagram')
        instagram_positions = get_instagram_positions(config)

    # Handle Audience Network placements
    if config['platforms'].get('audience_network'):
        publisher_platforms.append('audience_network')
        audience_network_positions = get_audience_network_positions(config)

    return publisher_platforms, facebook_positions, instagram_positions, messenger_positions, audience_network_positions


def get_facebook_positions(config):
    positions = ['feed']
    placements = config['placements']
    if placements.get('profile_feed'):
        positions.append('profile_feed')
    if placements.get('marketplace'):
        positions.append('marketplace')
    if placements.get('video_feeds'):
        positions.append('video_feeds')
    if placements.get('right_column'):
        positions.append('right_hand_column')
    if placements.get('stories'):
        positions.append('story')
    if placements.get('reels'):
        positions.append('facebook_reels')
    if placements.get('in_stream'):
        positions.append('instream_video')
    if placements.get('search'):
        positions.append('search')
    return positions


def get_instagram_positions(config):
    positions = ['stream']
    placements = config['placements']
    if placements.get('instagram_feeds'):
        positions.append('stream')
    if placements.get('explore'):
        positions.append('explore')
    if placements.get('explore_home'):
        positions.append('explore_home')
    if placements.get('instagram_stories'):
        positions.append('story')
    if placements.get('instagram_reels'):
        positions.append('reels')
    if placements.get('instagram_search'):
        positions.append('ig_search')
    return positions


def get_audience_network_positions(config):
    positions = []
    if config['placements'].get('native_banner_interstitial'):
        positions.append('classic')
    if config['placements'].get('rewarded_videos'):
        positions.append('rewarded_video')
    return positions


def setup_advantage_targeting(campaign_id, folder_name, config, start_time, event_type):
    return {
        "name": folder_name,
        "campaign_id": campaign_id,
        "billing_event": "IMPRESSIONS",
        "optimization_goal": config.get("optimization_goal", "OFFSITE_CONVERSIONS"),
        "targeting_optimization_type": "TARGETING_OPTIMIZATION_ADVANTAGE_PLUS",
        "targeting": {
            "geo_locations": {"countries": [config["location"]]},
        },
        "start_time": start_time.strftime('%Y-%m-%dT%H:%M:%S'),
        "dynamic_ad_image_enhancement": True,
        "dynamic_ad_voice_enhancement": True,
        "promoted_object": {
            "pixel_id": config["pixel_id"],
            "custom_event_type": event_type,
            "object_store_url": config["object_store_url"] if config["objective"] == "OUTCOME_APP_PROMOTION" else None
        }
    }


def setup_standard_targeting(campaign_id, folder_name, config, start_time, age_min, age_max, gender_value, publisher_platforms, facebook_positions, instagram_positions, messenger_positions, audience_network_positions, event_type):
    return {
        "name": folder_name,
        "campaign_id": campaign_id,
        "billing_event": "IMPRESSIONS",
        "optimization_goal": config.get("optimization_goal", "OFFSITE_CONVERSIONS"),
        "targeting": {
            "geo_locations": {"countries": config["location"]},
            "age_min": age_min,
            "age_max": age_max,
            "genders": gender_value,
            "publisher_platforms": publisher_platforms,
            "facebook_positions": facebook_positions or None,
            "instagram_positions": instagram_positions or None,
            "messenger_positions": messenger_positions or None,
            "audience_network_positions": audience_network_positions or None,
            "custom_audiences": config.get("custom_audiences"),
            "flexible_spec": [{"interests": [{"id": spec["value"], "name": spec.get("label", "Unknown Interest")}]} for spec in config.get("flexible_spec", [])],
        },
        "attribution_spec": [{
            "event_type": 'CLICK_THROUGH',
            "window_days": int(attribution_setting.split('_')[0].replace('d', ''))
        }],
        "start_time": start_time.strftime('%Y-%m-%dT%H:%M:%S'),
        "dynamic_ad_image_enhancement": False,
        "dynamic_ad_voice_enhancement": False,
        "promoted_object": {
            "pixel_id": config["pixel_id"],
            "custom_event_type": event_type,
            "object_store_url": config["object_store_url"] if config["objective"] == "OUTCOME_APP_PROMOTION" else None
        }
    }


def handle_bid_and_budget(ad_set_params, config, is_cbo, is_existing_cbo, ad_account_timezone):
    if not is_cbo and not is_existing_cbo:
        if config.get('buying_type') == 'RESERVED':
            ad_set_params["bid_strategy"] = None
            ad_set_params["rf_prediction_id"] = config.get('prediction_id')
        else:
            ad_set_params["bid_strategy"] = config.get('ad_set_bid_strategy', 'LOWEST_COST_WITHOUT_CAP')

        if config.get('ad_set_bid_strategy') in ['COST_CAP', 'LOWEST_COST_WITH_BID_CAP']:
            ad_set_params["bid_amount"] = int(float(config['bid_amount']) * 100)  # Convert to cents

        # Handle budget values
        if config.get('ad_set_budget_optimization') == "DAILY_BUDGET":
            ad_set_params["daily_budget"] = int(float(config['ad_set_budget_value']) * 100)
        elif config.get('ad_set_budget_optimization') == "LIFETIME_BUDGET":
            ad_set_params["lifetime_budget"] = int(float(config['ad_set_budget_value']) * 100)
            ad_set_params["end_time"] = get_end_time(config, ad_account_timezone)

    return ad_set_params


def get_end_time(config, ad_account_timezone):
    end_time = config.get('ad_set_end_time')
    if end_time:
        end_time = end_time + ":00" if len(end_time) == 16 else end_time
        end_time = convert_to_utc(end_time, ad_account_timezone)
        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S')
        return end_time.strftime('%Y-%m-%dT%H:%M:%S')
    return None

# Helper functions for video and image uploads
async def upload_video(video_file, task_id, config):
    check_cancellation(task_id)
    
    try:
            
        # Upload the video to the ad account
        video = AdVideo(parent_id=config['ad_account_id'])
        video[AdVideo.Field.filepath] = video_file
        await asyncio.to_thread(video.remote_create)

        video_id = video.get_id()
        logging.info(f"Video {video_id} is ready for use.")
        return video_id

    except Exception as e:
        error_msg = f"Error uploading video: {e}"
        emit_error(task_id, error_msg)
        return None


def is_video_ready(video_id, max_retries=10, initial_delay=10):
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            ready_video = AdVideo(fbid=video_id).api_get(fields=['status'])
            if ready_video.get('status', {}).get('video_status') == 'ready':
                return True
        except Exception as retry_error:
            logging.error(f"Retry {attempt} failed for video {video_id}: {retry_error}")

        # Exponential backoff for retries
        time.sleep(delay)
        delay *= 2  # Increase delay for each retry

    return False


async def upload_image(image_file, task_id, config):
    check_cancellation(task_id)
    
    try:    
        # Upload the image to the ad account using the image data
        image = AdImage(parent_id=config['ad_account_id'])
        image[AdImage.Field.filename] = image_file
        await asyncio.to_thread(image.remote_create)

        image_hash = image.get(AdImage.Field.hash)
        logging.info(f"Uploaded image successfully with hash: {image_hash}")
        return image_hash

    except Exception as e:
        error_msg = f"Error uploading image for task {task_id}: {e}"
        logging.error(error_msg)
        emit_error(task_id, error_msg)
        return None


# Function to generate thumbnails for videos
async def generate_thumbnail(video_file, thumbnail_file, task_id):
    check_cancellation(task_id)
    command = ['ffmpeg', '-i', video_file, '-ss', '00:00:01.000', '-vframes', '1', '-update', '1', thumbnail_file]
    
    try:
        proc = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        with tasks_lock:
            process_pids.setdefault(task_id, []).append(proc.pid)
        
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, command, output=stdout, stderr=stderr)

    except subprocess.CalledProcessError as e:
        error_msg = f"Error generating thumbnail: {e.cmd} returned non-zero exit status {e.returncode}"
        emit_error(task_id, error_msg)
        raise
    
def get_video_duration(video_file, task_id):
    check_cancellation(task_id)

    command = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        video_file
    ]

    try:
        # Run the command to get video duration with a timeout
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
        
        # Register process PID for task management
        register_task_process(task_id, result.pid)
        
        # Handle task cancellation (if needed)
        if result.returncode == -signal.SIGTERM:
            logging.warning(f"Process for task {task_id} was terminated.")
            raise TaskCanceledException(f"Task {task_id} has been canceled")

        # Raise error if process didn't complete successfully
        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, command, output=result.stdout, stderr=result.stderr)

        # Return the video duration as a float
        return float(result.stdout.strip())

    except subprocess.TimeoutExpired as e:
        logging.error(f"Timeout expired for task {task_id}: {e}")
        handle_subprocess_error(e, task_id)
    except subprocess.CalledProcessError as e:
        handle_subprocess_error(e, task_id)
    except Exception as e:
        logging.error(f"An unexpected error occurred while getting video duration for task {task_id}: {e}")
        raise

def register_task_process(task_id, pid):
    #Registers the process PID for a given task
    with tasks_lock:
        if task_id not in process_pids:
            process_pids[task_id] = []
        process_pids[task_id].append(pid)


def handle_subprocess_error(e, task_id):
    #Handles errors from subprocess and logs the appropriate messages
    if e.returncode == -signal.SIGTERM:
        logging.warning(f"Process for task {task_id} was terminated by signal.")
        raise TaskCanceledException(f"Task {task_id} has been canceled")
    else:
        logging.error(f"Error getting video duration: {e.cmd} returned non-zero exit status {e.returncode}")
        logging.error(f"Stdout: {e.output.decode().strip()}")
        logging.error(f"Stderr: {e.stderr.decode().strip()}")
        raise

tasks_lock = Lock()
process_pids = {}  # Assuming this is a shared resource for managing PIDs

def register_task_process(task_id, pid):
    with tasks_lock:
        if task_id not in process_pids:
            process_pids[task_id] = []
        process_pids[task_id].append(pid)

async def trim_video(input_file, output_file, duration, task_id):
    check_cancellation(task_id)
    command = [
        'ffmpeg',
        '-i', input_file,
        '-t', str(duration),
        '-c', 'copy',
        output_file
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, command, output=stdout, stderr=stderr)

    except asyncio.TimeoutError as e:
        logging.error(f"Timeout expired for task {task_id}: {e}")
        raise TaskTimeoutException(f"Task {task_id} timed out after 60 seconds") from e
    except subprocess.CalledProcessError as e:
        logging.error(f"Error trimming video for task {task_id}: {e.cmd} returned non-zero exit status {e.returncode}")
        raise

def parse_config(config_text):
    config = {}
    lines = config_text.strip().split('\n')
    for line in lines:
        key, value = line.split(':', 1)
        config[key.strip()] = value.strip()
    return config

def convert_webp_to_jpeg(webp_file):
    jpeg_file = os.path.splitext(webp_file)[0] + ".jpg"
    with Image.open(webp_file) as img:
        img.convert("RGB").save(jpeg_file, "JPEG")
    return jpeg_file

def create_ad(ad_set_id, media_file, config, task_id):
    check_cancellation(task_id)
    
    try:
        ad_format = config.get('ad_format', 'Single image or video')

        # Generate link with UTM parameters
        link = generate_link_with_utm(config)

        if ad_format == 'Single image or video':
            media_file = handle_media_conversion(media_file)

            if media_file.lower().endswith(('.jpg', '.png', '.jpeg')):
                create_image_ad(ad_set_id, media_file, config, link, task_id)
            else:
                create_video_ad(ad_set_id, media_file, config, link, task_id)

    except TaskCanceledException:
        logging.warning(f"Task {task_id} has been canceled during ad creation.")
    except Exception as e:
        if isinstance(e, subprocess.CalledProcessError) and e.returncode == -signal.SIGTERM:
            logging.error(f"Task {task_id} process was terminated by signal.")
        else:
            error_msg = f"Error creating ad for task {task_id}: {e}"
            emit_error(task_id, error_msg)

# Helper functions

def generate_link_with_utm(config):
    #Generates the link with UTM parameters
    base_link = config.get('link', 'https://kyronaclinic.com/pages/review-1')
    utm_parameters = config.get('url_parameters', 'utm_source=Facebook&utm_medium={{adset.name}}&utm_campaign={{campaign.name}}&utm_content={{ad.name}}')

    if utm_parameters and not utm_parameters.startswith('?'):
        utm_parameters = '?' + utm_parameters

    return base_link + utm_parameters

def handle_media_conversion(media_file):
    #Handles media file conversion if needed
    if media_file.lower().endswith('.webp'):
        logging.info("Converting webp to jpeg")
        return convert_webp_to_jpeg(media_file)
    return media_file

async def create_image_ad(ad_set_id, media_file, config, link, task_id):
    #Handles the creation of an image ad
    logging.info("Creating image ad")
    image_hash = await upload_image(media_file, task_id, config)
    if not image_hash:
        error_msg = f"Failed to upload image: {media_file}"
        emit_error(task_id, error_msg)
        return

    object_story_spec = generate_image_object_story_spec(config, image_hash, link)
    create_ad_creative(ad_set_id, object_story_spec, config, task_id)

def create_video_ad(ad_set_id, media_file, config, link, task_id):
    #Handles the creation of a video ad
    logging.info("Creating video ad")
    video_id, image_hash = handle_video_upload(media_file, task_id, config)
    if not video_id:
        emit_error(task_id, f"Failed to upload video: {media_file}")
        return

    object_story_spec = generate_video_object_story_spec(config, video_id, image_hash, link)
    create_ad_creative(ad_set_id, object_story_spec, config, task_id)

async def handle_video_upload(video_file, task_id, config):
    #Uploads video and thumbnail
    thumbnail_path = f"{os.path.splitext(video_file)[0]}.jpg"
    await generate_thumbnail(video_file, thumbnail_path, task_id)

    image_hash = await upload_image(thumbnail_path, task_id, config)
    video_id = await upload_video(video_file, task_id, config)

    return video_id, image_hash

def generate_image_object_story_spec(config, image_hash, link):
    #Generates the object story spec for image ads
    return {
        "page_id": config.get('facebook_page_id', '102076431877514'),
        "link_data": {
            "image_hash": image_hash,
            "link": link,
            "message": config.get('ad_creative_primary_text', 'default text'),
            "name": config.get('ad_creative_headline', 'Your Headline Here'),
            "description": config.get('ad_creative_description', 'Your Description Here'),
            "call_to_action": {
                "type": config.get('call_to_action', 'SHOP_NOW'),
                "value": {
                    "link": link
                }
            }
        }
    }

def generate_video_object_story_spec(config, video_id, image_hash, link):
    #Generates the object story spec for video ads
    return {
        "page_id": config.get('facebook_page_id', '102076431877514'),
        "video_data": {
            "video_id": video_id,
            "call_to_action": {
                "type": config.get('call_to_action', 'SHOP_NOW'),
                "value": {
                    "link": link
                }
            },
            "message": config.get('ad_creative_primary_text', 'default text'),
            "title": config.get('ad_creative_headline', 'Your Headline Here'),
            "image_hash": image_hash,
            "link_description": config.get('ad_creative_description', 'FREE Shipping & 60-Day Money-Back Guarantee')
        }
    }

def create_ad_creative(ad_set_id, object_story_spec, config, task_id):
    #Creates and uploads the ad creative
    degrees_of_freedom_spec = {
        "creative_features_spec": {
            "standard_enhancements": {
                "enroll_status": "OPT_OUT"
            }
        }
    }

    ad_creative = AdCreative(parent_id=config['ad_account_id'])
    params = {
        AdCreative.Field.name: "Creative Name",
        AdCreative.Field.object_story_spec: object_story_spec,
        AdCreative.Field.degrees_of_freedom_spec: degrees_of_freedom_spec
    }
    ad_creative.update(params)
    ad_creative.remote_create()

    ad = Ad(parent_id=config['ad_account_id'])
    ad[Ad.Field.name] = os.path.splitext(os.path.basename(object_story_spec['page_id']))[0]
    ad[Ad.Field.adset_id] = ad_set_id
    ad[Ad.Field.creative] = {"creative_id": ad_creative.get_id()}
    ad[Ad.Field.status] = "PAUSED"
    ad.remote_create()

    logging.info(f"Created ad with ID: {ad.get_id()}")

def create_carousel_ad(ad_set_id, media_files, config, task_id):
    check_cancellation(task_id)
    
    try:
        if config.get('ad_format', 'Carousel') != 'Carousel':
            logging.error(f"Invalid ad format for task {task_id}")
            return
        
        carousel_cards = []
        for media_file in media_files:
            check_cancellation(task_id)  # Periodically check for task cancellation

            media_file = handle_media_conversion(media_file)
            if not media_file:
                continue

            if is_video_file(media_file):
                card = process_video_file(media_file, config, task_id)
            elif is_image_file(media_file):
                card = process_image_file(media_file, config, task_id)
            else:
                logging.error(f"Unsupported media file format: {media_file}")
                continue

            # Add UTM parameters to the card
            card['link'] = generate_link_with_utm(card['link'], config)
            carousel_cards.append(card)

        # Create the carousel ad
        object_story_spec = generate_carousel_object_story_spec(carousel_cards, config)
        create_ad_creative(ad_set_id, object_story_spec, config, task_id)

        logging.info(f"Created carousel ad for task {task_id}")
        
    except TaskCanceledException:
        logging.warning(f"Task {task_id} has been canceled during carousel ad creation.")
    except Exception as e:
        handle_error(e, task_id)

# Helper Functions
def handle_media_conversion(media_file):
    #Converts webp to jpeg if necessary
    if media_file.lower().endswith('.webp'):
        logging.info(f"Converting webp to jpeg for file: {media_file}")
        return convert_webp_to_jpeg(media_file)
    return media_file

def is_video_file(media_file):
    #Returns True if the file is a video
    return media_file.lower().endswith(('.mp4', '.mov', '.avi'))

def is_image_file(media_file):
    #Returns True if the file is an image
    return media_file.lower().endswith(('.jpg', '.jpeg', '.png'))

async def process_video_file(video_file, config, task_id):
    #Handles the video file processing and returns a carousel card
    thumbnail_path = f"{os.path.splitext(video_file)[0]}.jpg"
    await generate_thumbnail(video_file, thumbnail_path, task_id)

    image_hash = await upload_image(thumbnail_path, task_id, config)
    if not image_hash:
        logging.error(f"Failed to upload thumbnail: {thumbnail_path}")
        return None

    video_id = await upload_video(video_file, task_id, config)
    if not video_id:
        logging.error(f"Failed to upload video: {video_file}")
        return None

    return {
        "link": config.get('link', 'https://kyronaclinic.com/pages/review-1'),
        "video_id": video_id,
        "call_to_action": {
            "type": config.get('call_to_action', 'SHOP_NOW'),
            "value": {"link": config.get('link', 'https://kyronaclinic.com/pages/review-1')}
        },
        "image_hash": image_hash
    }

async def process_image_file(image_file, config, task_id):
    #Handles the image file processing and returns a carousel card
    image_hash = await upload_image(image_file, task_id, config)
    if not image_hash:
        logging.error(f"Failed to upload image: {image_file}")
        return None

    return {
        "link": config.get('link', 'https://kyronaclinic.com/pages/review-1'),
        "image_hash": image_hash,
        "call_to_action": {
            "type": config.get('call_to_action', 'SHOP_NOW'),
            "value": {"link": config.get('link', 'https://kyronaclinic.com/pages/review-1')}
        }
    }

def generate_link_with_utm(link, config):
    #Generates a link with UTM parameters if provided
    utm_parameters = config.get('url_parameters', 'utm_source=Facebook&utm_medium={{adset.name}}&utm_campaign={{campaign.name}}&utm_content={{ad.name}}')
    if utm_parameters and not utm_parameters.startswith('?'):
        utm_parameters = '?' + utm_parameters
    return link + utm_parameters

def generate_carousel_object_story_spec(carousel_cards, config):
    #Generates the object story spec for carousel ads
    return {
        "page_id": config.get('facebook_page_id', '102076431877514'),
        "link_data": {
            "link": config.get('link', 'https://kyronaclinic.com/pages/review-1'),
            "child_attachments": carousel_cards,
            "multi_share_optimized": True,
            "multi_share_end_card": False,
            "name": config.get('ad_creative_headline', 'No More Neuropathic Foot Pain'),
            "description": config.get('ad_creative_description', 'FREE Shipping & 60-Day Money-Back Guarantee'),
            "caption": config.get('ad_creative_primary_text', 'default text')
        }
    }

def create_ad_creative(ad_set_id, object_story_spec, config, task_id):
    #Creates and uploads the ad creative
    degrees_of_freedom_spec = {
        "creative_features_spec": {
            "standard_enhancements": {
                "enroll_status": "OPT_OUT"
            }
        }
    }

    ad_creative = AdCreative(parent_id=config['ad_account_id'])
    params = {
        AdCreative.Field.name: "Carousel Ad Creative",
        AdCreative.Field.object_story_spec: object_story_spec,
        AdCreative.Field.degrees_of_freedom_spec: degrees_of_freedom_spec
    }
    ad_creative.update(params)
    ad_creative.remote_create()

    ad = Ad(parent_id=config['ad_account_id'])
    ad[Ad.Field.name] = "Carousel Ad"
    ad[Ad.Field.adset_id] = ad_set_id
    ad[Ad.Field.creative] = {"creative_id": ad_creative.get_id()}
    ad[Ad.Field.status] = "PAUSED"
    ad.remote_create()

def handle_error(e, task_id):
    #Handles errors and emits error message
    if isinstance(e, subprocess.CalledProcessError) and e.returncode == -signal.SIGTERM:
        logging.error(f"Task {task_id} process was terminated by signal.")
    else:
        error_msg = f"Error creating carousel ad for task {task_id}: {e}"
        emit_error(task_id, error_msg)
           
def find_campaign_by_id(campaign_id, ad_account_id):
    try:
        campaign = AdAccount(ad_account_id).get_campaigns(
            fields=['name'],
            params={
                'filtering': [{'field': 'id', 'operator': 'EQUAL', 'value': campaign_id}]
            }
        )
        if campaign:
            return campaign_id
        else:
            return None
    except Exception as e:
        print(f"Error finding campaign by ID: {e}")
        return None

def get_all_video_files(directory):
    video_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.mp4', '.mov', '.avi')):
                video_files.append(os.path.join(root, file))
    return video_files

def get_all_image_files(directory):
    image_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                image_files.append(os.path.join(root, file))
    return image_files

@app.route('/create_campaign', methods=['POST'])
def handle_create_campaign():
    try:
        config = {}

        def parse_custom_audiences(audience_str):
            try:
                # Parse the JSON string into a list of dicts
                audiences = json.loads(audience_str)
                # Extract only the `value` (which is the `id`)
                return [{"id": audience["value"]} for audience in audiences]
            except json.JSONDecodeError as e:
                print(f"Error parsing custom audiences: {e}")
                return []  # Return an empty list if parsing fails
        try:
            flexible_spec = json.loads(request.form.get("interests", "[]"))
            print(request.form.get("interests", "[]"))
            print(f"Flexible Spec: {flexible_spec}")
        except (TypeError, json.JSONDecodeError):
            flexible_spec = []  # Default to an empty list if parsing fails
            print("Failed to parse flexible_spec")

                
        custom_audiences_str = request.form.get('custom_audiences', '[]')
        custom_audiences = parse_custom_audiences(custom_audiences_str)
        print(custom_audiences)

        campaign_name = request.form.get('campaign_name')
        campaign_id = request.form.get('campaign_id')
        print("campaign id:")
        print(campaign_id)
        upload_folder = request.files.getlist('uploadFolders')
        task_id = request.form.get('task_id')

        ad_account_id = request.form.get('ad_account_id', 'act_2945173505586523')
        pixel_id = request.form.get('pixel_id', '466400552489809')
        facebook_page_id = request.form.get('facebook_page_id', '102076431877514')
        app_id = request.form.get('app_id', '314691374966102')
        app_secret = request.form.get('app_secret', '88d92443cfcfc3922cdea79b384a116e')
        access_token = request.form.get('access_token', 'EAAEeNcueZAVYBO0NvEUMo378SikOh70zuWuWgimHhnE5Vk7ye8sZCaRtu9qQGWNDvlBZBBnZAT6HCuDlNc4OeOSsdSw5qmhhmtKvrWmDQ8ZCg7a1BZAM1NS69YmtBJWGlTwAmzUB6HuTmb3Vz2r6ig9Xz9ZADDDXauxFCry47Fgh51yS1JCeo295w2V')
        ad_format = request.form.get('ad_format', 'Single image or video')

        print(access_token)
        print(app_id)
        print(ad_account_id)
        objective = request.form.get('objective', 'OUTCOME_SALES')
        campaign_budget_optimization = request.form.get('campaign_budget_optimization', 'DAILY_BUDGET')
        budget_value = request.form.get('campaign_budget_value', '50.73')
        bid_strategy = request.form.get('campaign_bid_strategy', 'LOWEST_COST_WITHOUT_CAP')
        buying_type = request.form.get('buying_type', 'AUCTION')
        object_store_url = request.form.get('object_store_url', '')
        bid_amount = request.form.get('bid_amount', '0.0')
        is_cbo = request.form.get('isCBO', 'false').lower() == 'true'
        
        # Receive the JavaScript objects directly
        platforms = request.form.get('platforms')
        placements = request.form.get('placements')
        # Check if the received platforms and placements are in a valid format
        if not isinstance(platforms, dict):
            try:
                platforms = json.loads(platforms)
            except (TypeError, json.JSONDecodeError) as e:
                logging.error(f"Error decoding platforms JSON: {e}")
                logging.error(f"Received platforms JSON: {platforms}")
                return jsonify({"error": "Invalid platforms JSON"}), 400

        if not isinstance(placements, dict):
            try:
                placements = json.loads(placements)
            except (TypeError, json.JSONDecodeError) as e:
                logging.error(f"Error decoding placements JSON: {e}")
                logging.error(f"Received placements JSON: {placements}")
                return jsonify({"error": "Invalid placements JSON"}), 400

        logging.info(f"Platforms after processing: {platforms}")
        logging.info(f"Placements after processing: {placements}")
        FacebookAdsApi.init(app_id, app_secret, access_token, api_version='v20.0')

        ad_account_timezone = get_ad_account_timezone(ad_account_id)


        with tasks_lock:
            upload_tasks[task_id] = True
            process_pids[task_id] = []

        config = {
            'ad_account_id': ad_account_id,
            'facebook_page_id': facebook_page_id,
            'headline': request.form.get('headline', 'No More Neuropathic Foot Pain'),
            'link': request.form.get('destination_url', 'https://kyronaclinic.com/pages/review-1'),
            'utm_parameters': request.form.get('url_parameters', '?utm_source=Facebook&utm_medium={{adset.name}}&utm_campaign={{campaign.name}}&utm_content={{ad.name}}'),
            'object_store_url': object_store_url,
            'budget_value': budget_value,
            'bid_strategy': bid_strategy,
            'location': request.form.get('location', 'GB'),
            'age_range': request.form.get('age_range',),
            'age_range_max': request.form.get('age_range_max', '65'),
            'pixel_id': pixel_id,
            'objective': objective,
            'ad_creative_primary_text': request.form.get('ad_creative_primary_text', ''),
            'ad_creative_headline': request.form.get('ad_creative_headline', 'No More Neuropathic Foot Pain'),
            'ad_creative_description': request.form.get('ad_creative_description', 'FREE Shipping & 60-Day Money-Back Guarantee'),
            'call_to_action': request.form.get('call_to_action', 'SHOP_NOW'),
            'destination_url': request.form.get('destination_url', 'https://kyronaclinic.com/pages/review-1'),
            'app_events': request.form.get('app_events', (datetime.now() + timedelta(days=1)).replace(hour=4, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S')),
            'language_customizations': request.form.get('language_customizations', 'en'),
            'url_parameters': request.form.get('url_parameters', '?utm_source=Facebook&utm_medium={{adset.name}}&utm_campaign={{campaign.name}}&utm_content={{ad.name}}'),
            'gender': request.form.get('gender', 'All'),
            'ad_set_budget_optimization': request.form.get('ad_set_budget_optimization', 'DAILY_BUDGET'),
            'ad_set_budget_value': request.form.get('ad_set_budget_value', '50.73'),
            'ad_set_bid_strategy': request.form.get('ad_set_bid_strategy', 'LOWEST_COST_WITHOUT_CAP'),
            'campaign_budget_optimization': request.form.get('campaign_budget_optimization', 'AD_SET_BUDGET_OPTIMIZATION'),
            'ad_format': ad_format,
            'bid_amount': bid_amount,
            'ad_set_end_time': request.form.get('ad_set_end_time', ''),
            'buying_type': request.form.get('buying_type', 'AUCTION'),
            'platforms': platforms,
            'placements': placements,
            'flexible_spec': flexible_spec,  # Include the parsed flexible_spec
            'geo_locations': request.form.get('location'),
            'optimization_goal': request.form.get('performance_goal', 'OFFSITE_CONVERSIONS'),
            'event_type': request.form.get('event_type', 'PURCHASE'),
            'is_cbo': request.form.get('isCBO', 'false').lower() == 'true',
            'custom_audiences': custom_audiences,
            'attribution_setting': request.form.get('attribution_setting', '7d_click'),
            'ad_account_timezone': ad_account_timezone,
            'instagram_actor_id': request.form.get('instagram_account', '')
        }

        if campaign_id:
            campaign_id = find_campaign_by_id(campaign_id, ad_account_id)
            existing_campaign_budget_optimization = get_campaign_budget_optimization(campaign_id, ad_account_id)
            is_existingCBO = existing_campaign_budget_optimization.get('is_campaign_budget_optimization', False)
            config['is_existing_cbo'] = is_existingCBO
            if not campaign_id:
                logging.error(f"Campaign ID {campaign_id} not found for ad account {ad_account_id}")
                print(campaign_id)
                print(ad_account_id)
                return jsonify({"error": "Campaign ID not found"}), 404
        else:
            print(objective)
            print("Objective")
            campaign_id, campaign = create_campaign(campaign_name, objective, campaign_budget_optimization, budget_value, bid_strategy, buying_type, task_id, ad_account_id, app_id, app_secret, access_token, is_cbo)
            if not campaign_id:
                logging.error(f"Failed to create campaign with name {campaign_name}")
                return jsonify({"error": "Failed to create campaign"}), 500

        temp_dir = tempfile.mkdtemp()
        for file in upload_folder:
            file_path = os.path.join(temp_dir, file.filename)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            if not file.filename.startswith('.'):  # Skip hidden files like .DS_Store
                file.save(file_path)

        folders = [f for f in os.listdir(temp_dir) if os.path.isdir(os.path.join(temp_dir, f))]

        def has_subfolders(folder):
            for item in os.listdir(folder):
                item_path = os.path.join(folder, item)
                if os.path.isdir(item_path):
                    return True
            return False

        total_videos = 0
        total_images = 0
        for folder in folders:
            folder_path = os.path.join(temp_dir, folder)
            total_videos += len(get_all_video_files(folder_path))
            total_images += len(get_all_image_files(folder_path))

        def process_videos(task_id, campaign_id, folders, config, total_videos):
            try:
                # Emit initial progress
                emit_progress(task_id, 0, total_videos, step=0)

                processed_videos = 0
                last_update_time = time.time()

                with tqdm(total=total_videos, desc="Processing videos") as pbar:
                    for folder in folders:
                        check_cancellation(task_id)

                        folder_path = os.path.join(temp_dir, folder)
                        if has_subfolders(folder_path):
                            process_folder_with_subfolders_video(task_id, campaign_id, folder_path, config, pbar, total_videos)
                        else:
                            process_folder_video(task_id, campaign_id, folder_path, config, pbar, total_videos)

                # Emit final progress and completion event
                emit_progress(task_id, 100, total_videos, step=total_videos)
                emit_task_complete(task_id)

            except TaskCanceledException:
                logging.warning(f"Task {task_id} has been canceled during video processing.")
            except Exception as e:
                handle_processing_error_video(e, task_id)
            finally:
                cleanup_task(task_id, temp_dir)


        ### Helper Functions

        def process_folder_with_subfolders_video(task_id, campaign_id, folder_path, config, pbar, total_videos):
            #Process a folder that contains subfolders
            for subfolder in os.listdir(folder_path):
                subfolder_path = os.path.join(folder_path, subfolder)
                if os.path.isdir(subfolder_path):
                    video_files = get_all_video_files(subfolder_path)
                    if not video_files:
                        continue

                    ad_set = create_ad_set(campaign_id, subfolder, video_files, config, task_id)
                    if not ad_set:
                        continue

                    process_video_files(task_id, ad_set, video_files, config, pbar, total_videos)


        def process_folder_video(task_id, campaign_id, folder_path, config, pbar, total_videos):
            #Process a folder that does not contain subfolders
            video_files = get_all_video_files(folder_path)
            if not video_files:
                return

            ad_set = create_ad_set(campaign_id, folder_path, video_files, config, task_id)
            if not ad_set:
                return

            process_video_files(task_id, ad_set, video_files, config, pbar, total_videos)


        def process_video_files(task_id, ad_set, video_files, config, pbar, total_videos):
            #Process a list of video files for a given ad set
            ad_format = config.get('ad_format', 'Single image or video')

            if ad_format == 'Single image or video':
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_video = {
                        executor.submit(create_ad, ad_set.get_id(), video, config, task_id): video
                        for video in video_files
                    }

                    for future in as_completed(future_to_video):
                        check_cancellation(task_id)
                        video = future_to_video[future]
                        try:
                            future.result()
                        except TaskCanceledException:
                            logging.warning(f"Task {task_id} was canceled during processing of video {video}.")
                            return
                        except Exception as e:
                            handle_video_processing_error(e, video, task_id)
                        finally:
                            update_progress(pbar, task_id, total_videos)

            elif ad_format == 'Carousel':
                create_carousel_ad(ad_set.get_id(), video_files, config, task_id)


        def update_progress(pbar, task_id, total_images):
            #Update progress in the progress bar and emit progress to the client
            pbar.update(1)
            current_time = time.time()
            if current_time - last_update_time >= 0.5:
                emit_progress(task_id, pbar.n / total_images * 100, total_images, step=pbar.n)


        def handle_video_processing_error(exception, video, task_id):
            #Handle errors that occur during video processing
            logging.error(f"Error processing video {video}: {exception}")
            socketio.emit('error', {
                'task_id': task_id,
                'message': str(exception)
            })


        def handle_processing_error_video(exception, task_id):
            #Handle general errors during the processing
            logging.error(f"Error in processing videos for task {task_id}: {exception}")
            socketio.emit('error', {
                'task_id': task_id,
                'message': str(exception)
            })
            
        def process_images(task_id, campaign_id, folders, config, total_images):
            try:
                # Emit initial progress
                emit_progress(task_id, 0, total_images, step=0)

                processed_images = 0
                last_update_time = time.time()

                with tqdm(total=total_images, desc="Processing images") as pbar:
                    for folder in folders:
                        check_cancellation(task_id)

                        folder_path = os.path.join(temp_dir, folder)
                        if has_subfolders(folder_path):
                            process_folder_with_subfolders_image(task_id, campaign_id, folder_path, config, pbar, total_images)
                        else:
                            process_folder_image(task_id, campaign_id, folder_path, config, pbar, total_images)

                # Emit final progress and task complete events
                emit_progress(task_id, 100, total_images, step=total_images)
                emit_task_complete(task_id)

            except TaskCanceledException:
                logging.warning(f"Task {task_id} has been canceled during image processing.")
            except Exception as e:
                handle_processing_error_image(e, task_id)
            finally:
                cleanup_task(task_id, temp_dir)


        ### Helper Functions


        def process_folder_with_subfolders_image(task_id, campaign_id, folder_path, config, pbar, total_images):
            #Process a folder that contains subfolders
            for subfolder in os.listdir(folder_path):
                subfolder_path = os.path.join(folder_path, subfolder)
                if os.path.isdir(subfolder_path):
                    image_files = get_all_image_files(subfolder_path)
                    if not image_files:
                        continue

                    ad_set = create_ad_set(campaign_id, subfolder, image_files, config, task_id)
                    if not ad_set:
                        continue

                    process_image_files(task_id, ad_set, image_files, config, pbar, total_images)


        def process_folder_image(task_id, campaign_id, folder_path, config, pbar, total_images):
            #Process a folder that does not contain subfolders
            image_files = get_all_image_files(folder_path)
            if not image_files:
                return

            ad_set = create_ad_set(campaign_id, folder_path, image_files, config, task_id)
            if not ad_set:
                return

            process_image_files(task_id, ad_set, image_files, config, pbar, total_images)


        def process_image_files(task_id, ad_set, image_files, config, pbar, total_images):
            #Process a list of image files for a given ad set
            ad_format = config.get('ad_format', 'Single image or video')

            if ad_format == 'Single image or video':
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_image = {
                        executor.submit(create_ad, ad_set.get_id(), image, config, task_id): image
                        for image in image_files
                    }

                    for future in as_completed(future_to_image):
                        check_cancellation(task_id)
                        image = future_to_image[future]
                        try:
                            future.result()
                        except TaskCanceledException:
                            logging.warning(f"Task {task_id} was canceled during processing of image {image}.")
                            return
                        except Exception as e:
                            handle_image_processing_error(e, image, task_id)
                        finally:
                            update_progress(pbar, task_id, total_images)

            elif ad_format == 'Carousel':
                create_carousel_ad(ad_set.get_id(), image_files, config, task_id)


        def handle_image_processing_error(exception, image, task_id):
            #Handle errors that occur during image processing
            logging.error(f"Error processing image {image}: {exception}")
            socketio.emit('error', {
                'task_id': task_id,
                'message': str(exception)
            })


        def handle_processing_error_image(exception, task_id):
            #Handle general errors during the processing
            logging.error(f"Error in processing images for task {task_id}: {exception}")
            socketio.emit('error', {
                'task_id': task_id,
                'message': str(exception)
            })
        
        def process_mixed_media(task_id, campaign_id, folders, config, total_videos, total_images):
            try:
                total_files = total_videos + total_images
                emit_progress(task_id, 0, total_files, 0)

                processed_files = 0
                last_update_time = time.time()

                with tqdm(total=total_files, desc="Processing mixed media") as pbar:
                    for folder in folders:
                        check_cancellation(task_id)
                        folder_path = os.path.join(temp_dir, folder)

                        if has_subfolders(folder_path):
                            process_subfolders_media(task_id, campaign_id, folder_path, config, pbar, total_files, processed_files, last_update_time)
                        else:
                            process_folder_media(task_id, campaign_id, folder_path, config, pbar, total_files, processed_files, last_update_time)

                emit_progress(task_id, 100, total_files, total_files)
                emit_task_complete(task_id)

            except TaskCanceledException:
                logging.warning(f"Task {task_id} was canceled during mixed media processing.")
            except Exception as e:
                handle_processing_error(e, task_id)
            finally:
                cleanup_task(task_id, temp_dir)


        ### Helper Functions

        def emit_progress(task_id, progress, total, step):
            #Emit progress updates to the client
            socketio.emit('progress', {
                'task_id': task_id,
                'progress': progress,
                'step': f"{step}/{total}"
            })


        def emit_task_complete(task_id):
            #Emit task completion event
            socketio.emit('task_complete', {'task_id': task_id})


        def process_subfolders_media(task_id, campaign_id, folder_path, config, pbar, total_files, processed_files, last_update_time):
            #Process all subfolders within a given folder
            for subfolder in os.listdir(folder_path):
                subfolder_path = os.path.join(folder_path, subfolder)
                if os.path.isdir(subfolder_path):
                    video_files = get_all_video_files(subfolder_path)
                    image_files = get_all_image_files(subfolder_path)
                    media_files = video_files + image_files

                    if media_files:
                        ad_set = create_ad_set(campaign_id, subfolder, media_files, config, task_id)
                        if not ad_set:
                            continue

                        process_media_files(task_id, ad_set, media_files, config, pbar, total_files, processed_files, last_update_time)


        def process_folder_media(task_id, campaign_id, folder_path, config, pbar, total_files, processed_files, last_update_time):
            #Process media files within a single folder
            video_files = get_all_video_files(folder_path)
            image_files = get_all_image_files(folder_path)
            media_files = video_files + image_files

            if media_files:
                ad_set = create_ad_set(campaign_id, folder_path, media_files, config, task_id)
                if not ad_set:
                    return

                process_media_files(task_id, ad_set, media_files, config, pbar, total_files, processed_files, last_update_time)


        def process_media_files(task_id, ad_set, media_files, config, pbar, total_files, processed_files, last_update_time):
            #Process a list of media files for a given ad set
            ad_format = config.get('ad_format', 'Single image or video')

            if ad_format == 'Single image or video':
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_media = {
                        executor.submit(create_ad, ad_set.get_id(), media, config, task_id): media
                        for media in media_files
                    }

                    for future in as_completed(future_to_media):
                        check_cancellation(task_id)
                        media = future_to_media[future]
                        try:
                            future.result()
                        except TaskCanceledException:
                            logging.warning(f"Task {task_id} was canceled during processing media {media}.")
                            return
                        except Exception as e:
                            handle_media_processing_error(e, media, task_id)
                        finally:
                            processed_files += 1
                            update_progress_media(pbar, task_id, total_files, processed_files, last_update_time)

            elif ad_format == 'Carousel':
                create_carousel_ad(ad_set.get_id(), media_files, config, task_id)


        def update_progress_media(pbar, task_id, total_files, processed_files, last_update_time):
            #Update progress in the progress bar and emit progress to the client
            pbar.update(1)
            current_time = time.time()
            if current_time - last_update_time >= 0.5:
                emit_progress(task_id, processed_files / total_files * 100, total_files, step=processed_files)


        def handle_media_processing_error(exception, media, task_id):
            #Handle errors that occur during media processing
            logging.error(f"Error processing media {media}: {exception}")
            socketio.emit('error', {
                'task_id': task_id,
                'message': str(exception)
            })


        def handle_processing_error(exception, task_id):
            #Handle general errors during the processing
            logging.error(f"Error in processing mixed media for task {task_id}: {exception}")
            socketio.emit('error', {
                'task_id': task_id,
                'message': str(exception)
            })


        def cleanup_task(task_id, temp_dir):
            #Clean up after task processing
            with tasks_lock:
                process_pids.pop(task_id, None)
            shutil.rmtree(temp_dir, ignore_errors=True)

        # Call the appropriate processing function based on media types
        if total_videos > 0 and total_images > 0:
            socketio.start_background_task(target=process_mixed_media, task_id=task_id, campaign_id=campaign_id, folders=folders, config=config, total_videos=total_videos, total_images=total_images)
        elif total_videos > 0:
            socketio.start_background_task(target=process_videos, task_id=task_id, campaign_id=campaign_id, folders=folders, config=config, total_videos=total_videos)
        elif total_images > 0:
            socketio.start_background_task(target=process_images, task_id=task_id, campaign_id=campaign_id, folders=folders, config=config, total_images=total_images)

        return jsonify({"message": "Campaign processing started", "task_id": task_id})

    except Exception as e:
        logging.error(f"Error in handle_create_campaign: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/cancel_task', methods=['POST'])
def cancel_task():
    try:
        task_id = request.json.get('task_id')
        print(f"Received request to cancel task: {task_id}")
        with tasks_lock:
            if task_id in canceled_tasks:
                print(f"Task {task_id} already marked for cancellation")
            canceled_tasks.add(task_id)
            if task_id in upload_tasks:
                upload_tasks[task_id] = False
                # Kill the PIDs associated with this task
                for pid in process_pids.get(task_id, []):
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                process_pids.pop(task_id, None)
                print(f"Task {task_id} set to be canceled")
        return jsonify({"message": "Task cancellation request processed"}), 200
    except Exception as e:
        print(f"Error handling cancel task request: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
@app.route('/get_campaign_budget_optimization', methods=['POST'])
def handle_get_campaign_budget_optimization():
    try:
        data = request.json        
        campaign_id = data.get('campaign_id')
        ad_account_id = data.get('ad_account_id')
        app_id = data.get('app_id')
        app_secret = data.get('app_secret')
        access_token = data.get('access_token')

        if not campaign_id or not ad_account_id or not app_id or not app_secret or not access_token:
            return jsonify({"error": "Campaign ID, Ad Account ID, App ID, App Secret, and Access Token are required"}), 400

        FacebookAdsApi.init(app_id, app_secret, access_token, api_version='v19.0')
        campaign_budget_optimization = is_campaign_budget_optimized(campaign_id, ad_account_id)

        if campaign_budget_optimization is not None:
            return jsonify({"campaign_budget_optimization": campaign_budget_optimization}), 200
        else:
            return jsonify({"error": "Failed to retrieve campaign budget optimization details"}), 500

    except Exception as e:
        logging.error(f"Error in handle_get_campaign_budget_optimization: {e}")
        return jsonify({"error": "Internal server error"}), 500
    
if __name__ == "__main__":
    socketio.run(app, debug=True, host='0.0.0.0',port=5001)
    