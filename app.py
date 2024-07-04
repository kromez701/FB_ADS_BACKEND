import os
import math
import subprocess
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from flask import Flask, request, jsonify
from flask_cors import CORS
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.adimage import AdImage

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Initialize Facebook Ads API
app_id = "314691374966102"
app_secret = "88d92443cfcfc3922cdea79b384a116e"
access_token = "EAAEeNcueZAVYBO0NvEUMo378SikOh70zuWuWgimHhnE5Vk7ye8sZCaRtu9qQGWNDvlBZBBnZAT6HCuDlNc4OeOSsdSw5qmhhmtKvrWmDQ8ZCg7a1BZAM1NS69YmtBJWGlTwAmzUB6HuTmb3Vz2r6ig9Xz9ZADDDXauxFCry47Fgh51yS1JCeo295w2V"
ad_account_id = "act_2945173505586523"
pixel_id = "466400552489809"  # Replace this with your actual Facebook Pixel ID

FacebookAdsApi.init(app_id, app_secret, access_token, api_version='v19.0')

def parse_config(config_text):
    config = {}
    lines = config_text.strip().split('\n')
    for line in lines:
        key, value = line.split(':', 1)
        config[key.strip()] = value.strip()
    return config

@app.route('/create_campaign', methods=['POST'])
def create_campaign():
    data = request.form
    campaign_name = data.get('campaign_name')
    campaign_id = data.get('campaign_id')
    folder_path = data.get('folder_path')

    config_text = data.get('config_text')
    if config_text:
        config = parse_config(config_text)
    else:
        config = {
            'facebook_page_id': '102076431877514',
            'Headline': 'No More Neuropathic Foot Pain',
            'link': 'https://kyronaclinic.com/pages/review-1',
            'utm_parameters': '?utm_source=Facebook&utm_medium={{adset.name}}&utm_campaign={{campaign.name}}&utm_content={{ad.name}}'
        }

    facebook_page_id = config.get('facebook_page_id')
    headline = config.get('Headline')
    base_link = config.get('link')
    utm_parameters = config.get('utm_parameters')

    if campaign_id:
        campaign = AdAccount(ad_account_id).get_campaigns(params={"fields": ["id", "name"]})
        campaign = next((c for c in campaign if c['id'] == campaign_id), None)
        if not campaign:
            return jsonify({"error": "Campaign ID not found"}), 400
    else:
        campaign_id, campaign = create_new_campaign(campaign_name)
        if not campaign_id:
            return jsonify({"error": "Failed to create campaign"}), 500

    ad_sets = create_ad_sets(campaign_id, campaign_name, folder_path)
    if not ad_sets:
        return jsonify({"error": "Failed to create ad sets"}), 500

    process_videos(ad_sets, folder_path, facebook_page_id, headline, base_link, utm_parameters)

    return jsonify({"message": "Campaign and Ad Sets created successfully", "campaign_id": campaign_id, "ad_sets": [ad_set.get_id() for ad_set in ad_sets]})

def create_new_campaign(name):
    try:
        campaign = AdAccount(ad_account_id).create_campaign(
            fields=[AdAccount.Field.id],
            params={
                "name": name,
                "objective": "OUTCOME_SALES",
                "special_ad_categories": ["NONE"],
            },
        )
        print(f"Created campaign with ID: {campaign.get_id()}")
        return campaign.get_id(), campaign
    except Exception as e:
        print(f"Error creating campaign: {e}")
        return None, None

def create_ad_sets(campaign_id, folder_name, folder_path):
    try:
        videos = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.lower().endswith(('.mp4', '.mov', '.avi'))]
        num_ads_per_set = 5
        num_sets = math.ceil(len(videos) / num_ads_per_set)
        start_time = (datetime.now() + timedelta(days=1)).replace(
            hour=4, minute=0, second=0, microsecond=0
        )
        ad_sets = []
        for i in range(num_sets):
            ad_set_name = f"{folder_name} - Ad Set {i+1}"
            ad_set_params = {
                "name": ad_set_name,
                "campaign_id": campaign_id,
                "billing_event": "IMPRESSIONS",
                "optimization_goal": "OFFSITE_CONVERSIONS",
                "daily_budget": 5073,  # Adjust the budget to 50.73 in minor units
                "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
                "targeting": {
                    "geo_locations": {"countries": ["GB"]},
                    "age_min": 30,
                    "age_max": 65,
                    "publisher_platforms": ["facebook"],
                    "facebook_positions": ["feed", "profile_feed", "video_feeds"]
                },
                "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "dynamic_ad_image_enhancement": False,
                "dynamic_ad_voice_enhancement": False,
                "promoted_object": {
                    "pixel_id": pixel_id,
                    "custom_event_type": "PURCHASE"
                }
            }
            print(f"Ad set parameters: {ad_set_params}")
            ad_set = AdAccount(ad_account_id).create_ad_set(
                fields=[AdSet.Field.name],
                params=ad_set_params,
            )
            print(f"Created ad set with ID: {ad_set.get_id()}")
            ad_sets.append(ad_set)
        return ad_sets
    except Exception as e:
        print(f"Error creating ad sets: {e}")
        return []

def upload_video(video_file):
    try:
        video = AdVideo(parent_id=ad_account_id)
        video[AdVideo.Field.filepath] = video_file
        video.remote_create()
        print(f"Uploaded video with ID: {video.get_id()}")
        return video.get_id()
    except Exception as e:
        print(f"Error uploading video: {e}")
        return None

def generate_thumbnail(video_file, thumbnail_file):
    command = [
        'ffmpeg',
        '-i', video_file,
        '-ss', '00:00:01.000',
        '-vframes', '1',
        thumbnail_file
    ]
    subprocess.run(command, check=True)

def upload_image(image_file):
    try:
        image = AdImage(parent_id=ad_account_id)
        image[AdImage.Field.filename] = image_file
        image.remote_create()
        print(f"Uploaded image with hash: {image[AdImage.Field.hash]}")
        return image[AdImage.Field.hash]
    except Exception as e:
        print(f"Error uploading image: {e}")
        return None

def get_video_duration(video_file):
    command = [
        'ffprobe',
        '-v', 'error',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        video_file
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    return float(result.stdout)

def trim_video(input_file, output_file, duration):
    command = [
        'ffmpeg',
        '-i', input_file,
        '-t', str(duration),
        '-c', 'copy',
        output_file
    ]
    subprocess.run(command, check=True)

def create_ad(ad_set_id, video_file, facebook_page_id, headline, base_link, utm_parameters):
    try:
        video_path = video_file
        thumbnail_path = f"{os.path.splitext(video_file)[0]}.jpg"
        
        generate_thumbnail(video_path, thumbnail_path)
        image_hash = upload_image(thumbnail_path)
        
        if not image_hash:
            print(f"Failed to upload thumbnail: {thumbnail_path}")
            return

        max_duration = 240 * 60  # 240 minutes
        video_duration = get_video_duration(video_path)
        if video_duration > max_duration:
            trimmed_video_path = f"./trimmed_{os.path.basename(video_file)}"
            trim_video(video_path, trimmed_video_path, max_duration)
            video_path = trimmed_video_path

        video_id = upload_video(video_path)
        if not video_id:
            print(f"Failed to upload video: {video_file}")
            return
        
        link = base_link + utm_parameters

        object_story_spec = {
            "page_id": facebook_page_id,
            "video_data": {
                "video_id": video_id,
                "call_to_action": {
                    "type": "SHOP_NOW",
                    "value": {
                        "link": link
                    }
                },
                "message": "Finding it difficult to deal with neuropathic foot pain...",
                "title": headline,
                "image_hash": image_hash,
                "link_description": "FREE Shipping & 60-Day Money-Back Guarantee"
            }
        }
        degrees_of_freedom_spec = {
            "creative_features_spec": {
                "standard_enhancements": {
                    "enroll_status": "OPT_OUT"
                }
            }
        }
        ad_creative = AdCreative(parent_id=ad_account_id)
        params = {
            AdCreative.Field.name: "Creative Name",
            AdCreative.Field.object_story_spec: object_story_spec,
            AdCreative.Field.degrees_of_freedom_spec: degrees_of_freedom_spec
        }
        ad_creative.update(params)
        ad_creative.remote_create()

        ad = Ad(parent_id=ad_account_id)
        ad[Ad.Field.name] = os.path.basename(video_file)
        ad[Ad.Field.adset_id] = ad_set_id
        ad[Ad.Field.creative] = {"creative_id": ad_creative.get_id()}
        ad[Ad.Field.status] = "PAUSED"
        ad.remote_create()
        
        os.remove(thumbnail_path)
        
        print(f"Created ad with ID: {ad.get_id()}")
    except Exception as e:
        print(f"Error creating ad: {e}")

def process_videos(ad_sets, folder_path, facebook_page_id, headline, base_link, utm_parameters):
    video_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.lower().endswith(('.mp4', '.mov', '.avi'))]
    if not video_files:
        print("No video files found in the specified folder.")
        return

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_video = {executor.submit(create_ad, ad_sets[i // 5].get_id(), video, facebook_page_id, headline, base_link, utm_parameters): video for i, video in enumerate(video_files)}
        total_videos = len(video_files)
        
        with tqdm(total=total_videos, desc="Processing videos") as pbar:
            for future in as_completed(future_to_video):
                video = future_to_video[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing video {video}: {e}")
                finally:
                    pbar.update(1)

if __name__ == "__main__":
    app.run(debug=True)
    