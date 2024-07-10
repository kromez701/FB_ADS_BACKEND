from flask import Flask, request, jsonify
import shutil
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import os
import tempfile
import subprocess
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.advideo import AdVideo
from facebook_business.adobjects.adimage import AdImage
from threading import Lock
import signal
from tqdm import tqdm
import time  # Add this import

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Initialize Facebook Ads API
app_id = "1164502704759812"
app_secret = "5d176dbaa5b71a2c8554ef7b0ebb2d96"
access_token = "EAAQjGZBoOpAQBOZBb6hkntMEcCBJ27DXSKJHCcmSz5BpxlZA80HfKScZCstuc6kMmupAWt1KaubNhCQ09c4mYIYhHnKLVdVELZBjaZCB4awlJnAZAdR6e6qPZAtVd8nZCFZCwTB439jqDeSTKlZBgn11tvSCrXVRvW9qlZA8PIko9zdvaOtSkhkfeaKBxtcgfgZDZD"
ad_account_id = "act_820964716847008"
pixel_id = "1164502704759812"  # Replace this with your actual Facebook Pixel ID

FacebookAdsApi.init(app_id, app_secret, access_token, api_version='v19.0')

# Replace this with your actual Facebook Page ID
facebook_page_id = "363083913554414"

upload_tasks = {}
tasks_lock = Lock()
process_pids = {}
canceled_tasks = set()

class TaskCanceledException(Exception):
    pass

def create_campaign(name, task_id):
    check_cancellation(task_id)
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

def create_ad_set(campaign_id, folder_name, videos, task_id):
    check_cancellation(task_id)
    try:
        start_time = (datetime.now() + timedelta(days=1)).replace(
            hour=4, minute=0, second=0, microsecond=0
        )
        ad_set_name = folder_name
        ad_set_params = {
            "name": ad_set_name,
                "campaign_id": campaign_id,
                "billing_event": "IMPRESSIONS",
                "optimization_goal": "LINK_CLICKS",
                "daily_budget": 507300,  # Adjust the budget to 50.73 in minor units
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
                "dynamic_ad_voice_enhancement": False
        }
        # print(f"Ad set parameters: {ad_set_params}")
        ad_set = AdAccount(ad_account_id).create_ad_set(
            fields=[AdSet.Field.name],
            params=ad_set_params,
        )
        print(f"Created ad set with ID: {ad_set.get_id()}")
        return ad_set
    except Exception as e:
        print(f"Error creating ad set: {e}")
        return None

def upload_video(video_file, task_id):
    check_cancellation(task_id)
    try:
        video = AdVideo(parent_id=ad_account_id)
        video[AdVideo.Field.filepath] = video_file
        video.remote_create()
        print(f"Uploaded video with ID: {video.get_id()}")
        return video.get_id()
    except Exception as e:
        print(f"Error uploading video: {e}")
        return None

def upload_image(image_file, task_id):
    check_cancellation(task_id)
    try:
        image = AdImage(parent_id=ad_account_id)
        image[AdImage.Field.filename] = image_file
        image.remote_create()
        print(f"Uploaded image with hash: {image[AdImage.Field.hash]}")
        return image[AdImage.Field.hash]
    except Exception as e:
        print(f"Error uploading image: {e}")
        return None

def generate_thumbnail(video_file, thumbnail_file, task_id):
    check_cancellation(task_id)
    command = [
        'ffmpeg',
        '-i', video_file,
        '-ss', '00:00:01.000',
        '-vframes', '1',
        '-update', '1',  # Ensure single image output
        thumbnail_file
    ]
    try:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with tasks_lock:
            if task_id not in process_pids:
                process_pids[task_id] = []
            process_pids[task_id].append(proc.pid)
        stdout, stderr = proc.communicate()
        if proc.returncode == -signal.SIGTERM:
            print(f"Process for task {task_id} was terminated.")
            raise TaskCanceledException(f"Task {task_id} has been canceled")
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, command, output=stdout, stderr=stderr)
    except subprocess.CalledProcessError as e:
        if e.returncode == -signal.SIGTERM:
            print(f"Process for task {task_id} was terminated by signal.")
            raise TaskCanceledException(f"Task {task_id} has been canceled")
        else:
            print(f"Error generating thumbnail: {e.cmd} returned non-zero exit status {e.returncode}")
            print(f"Stdout: {e.output.decode()}")
            print(f"Stderr: {e.stderr.decode()}")
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
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with tasks_lock:
            if task_id not in process_pids:
                process_pids[task_id] = []
            process_pids[task_id].append(proc.pid)
        stdout, stderr = proc.communicate()
        if proc.returncode == -signal.SIGTERM:
            print(f"Process for task {task_id} was terminated.")
            raise TaskCanceledException(f"Task {task_id} has been canceled")
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, command, output=stdout, stderr=stderr)
        return float(stdout)
    except subprocess.CalledProcessError as e:
        if e.returncode == -signal.SIGTERM:
            print(f"Process for task {task_id} was terminated by signal.")
            raise TaskCanceledException(f"Task {task_id} has been canceled")
        else:
            print(f"Error getting video duration: {e.cmd} returned non-zero exit status {e.returncode}")
            print(f"Stdout: {e.output.decode()}")
            print(f"Stderr: {e.stderr.decode()}")
            raise

def trim_video(input_file, output_file, duration, task_id):
    check_cancellation(task_id)
    command = [
        'ffmpeg',
        '-i', input_file,
        '-t', str(duration),
        '-c', 'copy',
        output_file
    ]
    try:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with tasks_lock:
            if task_id not in process_pids:
                process_pids[task_id] = []
            process_pids[task_id].append(proc.pid)
        stdout, stderr = proc.communicate()
        if proc.returncode == -signal.SIGTERM:
            print(f"Process for task {task_id} was terminated.")
            raise TaskCanceledException(f"Task {task_id} has been canceled")
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, command, output=stdout, stderr=stderr)
    except subprocess.CalledProcessError as e:
        if e.returncode == -signal.SIGTERM:
            print(f"Process for task {task_id} was terminated by signal.")
            raise TaskCanceledException(f"Task {task_id} has been canceled")
        else:
            print(f"Error trimming video: {e.cmd} returned non-zero exit status {e.returncode}")
            print(f"Stdout: {e.output.decode()}")
            print(f"Stderr: {e.stderr.decode()}")
            raise

def parse_config(config_text):
    config = {}
    lines = config_text.strip().split('\n')
    for line in lines:
        key, value = line.split(':', 1)
        config[key.strip()] = value.strip()
    return config

def create_ad(ad_set_id, video_file, config, task_id):
    check_cancellation(task_id)
    try:
        video_path = video_file
        thumbnail_path = f"{os.path.splitext(video_file)[0]}.jpg"
        
        generate_thumbnail(video_path, thumbnail_path, task_id)
        image_hash = upload_image(thumbnail_path, task_id)
        
        if not image_hash:
            print(f"Failed to upload thumbnail: {thumbnail_path}")
            return

        max_duration = 240 * 60  # 240 minutes
        video_duration = get_video_duration(video_path, task_id)
        if video_duration > max_duration:
            trimmed_video_path = f"./trimmed_{os.path.basename(video_file)}"
            trim_video(video_path, trimmed_video_path, max_duration, task_id)
            video_path = trimmed_video_path

        video_id = upload_video(video_path, task_id)
        if not video_id:
            print(f"Failed to upload video: {video_file}")
            return
        
        base_link = config['link']
        utm_parameters = config['utm_parameters']
        link = base_link + utm_parameters

        object_story_spec = {
            "page_id": config['facebook_page_id'],
            "video_data": {
                "video_id": video_id,
                "call_to_action": {
                    "type": "SHOP_NOW",
                    "value": {
                        "link": link
                    }
                },
                "message": ("Finding it difficult to deal with neuropathic foot pain, as well as stiff and painful joints?"
                            "\n\nNo matter whether your neuropathy is caused by diabetes, chemo-induced, autoimmune disease, or idiopathic conditions... Tingling neuropathy has the potential to completely disrupt your life."
                            "\n\nIt is essential to take action right now, without delay, before it is too late..."
                            "\n\nThrough the promotion of blood circulation and the healing of damaged tissue in your foot, the Kyrona Clinics NMES Foot Massager utilises cutting-edge technology that provides almost instantaneous relief from neuropathic foot pain, stiffness, and swelling."
                            "\n\nTo use it, all you need is fifteen minutes per day, and you can do it from the convenience of your own home."
                            "\n\nAfter only fourteen days of use, you will experience a significant increase in your level of energy, and you will be able to once again take pleasure in living life to the fullest."
                            "\n\nIn addition, the Kyrona Clinics NMES Foot Massager comes with a money-back guarantee for a period of 60 Days and free shipping. You will either receive results or see a full refund of your money, guaranteed."
                            "\n\n✅ Stimulates blood flow"
                            "\n✅ Naturally promotes nerve regeneration process (no harsh medication)"
                            "\n✅ Designed & recommended by Dr. Campbell, a top renowned Chicago doctor with over 10 years of experience"
                            "\n\nGet yours now risk-free> https://kyronaclinic.com/pages/review-1"
                            "\n\nFast shipping from the UK warehouse - only 4-7 days!"),
                "title": config['headline'],
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
        ad_name = os.path.splitext(os.path.basename(video_file))[0]  # Remove file extension
        ad[Ad.Field.name] = ad_name
        ad[Ad.Field.adset_id] = ad_set_id
        ad[Ad.Field.creative] = {"creative_id": ad_creative.get_id()}
        ad[Ad.Field.status] = "PAUSED"
        ad.remote_create()
        
        os.remove(thumbnail_path)
        
        print(f"Created ad with ID: {ad.get_id()}")
    except TaskCanceledException:
        print(f"Task {task_id} has been canceled during ad creation.")
    except Exception as e:
        if isinstance(e, subprocess.CalledProcessError) and e.returncode == -signal.SIGTERM:
            print(f"Task {task_id} process was terminated by signal.")
        else:
            print(f"Error creating ad: {e}")
            socketio.emit('error', {'task_id': task_id, 'message': str(e)})

def find_campaign_by_id(campaign_id):
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

def check_cancellation(task_id):
    with tasks_lock:
        if task_id in canceled_tasks:
            canceled_tasks.remove(task_id)
            raise TaskCanceledException(f"Task {task_id} has been canceled")

def get_all_video_files(directory):
    video_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(('.mp4', '.mov', '.avi')):
                video_files.append(os.path.join(root, file))
    return video_files

@app.route('/create_campaign', methods=['POST'])
def handle_create_campaign():
    campaign_name = request.form.get('campaign_name')
    campaign_id = request.form.get('campaign_id')
    config_text = request.form.get('config_text')
    upload_folder = request.files.getlist('uploadFolders')
    task_id = request.form.get('task_id')

    # Track task ID immediately
    with tasks_lock:
        upload_tasks[task_id] = True
        process_pids[task_id] = []

    if campaign_id:
        campaign_id = find_campaign_by_id(campaign_id)
        if not campaign_id:
            return jsonify({"error": "Campaign ID not found"}), 404
    else:
        campaign_id, campaign = create_campaign(campaign_name, task_id)
        if not campaign_id:
            return jsonify({"error": "Failed to create campaign"}), 500
    
    config = {
        'facebook_page_id': request.form.get('facebook_page_id', '363083913554414'),
        'headline': request.form.get('headline', 'No More Neuropathic Foot Pain'),
        'link': request.form.get('link', 'https://kyronaclinic.com/pages/review-1'),
        'utm_parameters': request.form.get('utm_parameters', '?utm_source=Facebook&utm_medium={{adset.name}}&utm_campaign={{campaign.name}}&utm_content={{ad.name}}')
    }

    temp_dir = tempfile.mkdtemp()
    for file in upload_folder:
        file_path = os.path.join(temp_dir, file.filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        if not file.filename.startswith('.'):  # Skip hidden files like .DS_Store
            file.save(file_path)
    
    folders = [f for f in os.listdir(temp_dir) if os.path.isdir(os.path.join(temp_dir, f))]

    # Check if any folder has subfolders
    def has_subfolders(folder):
        for item in os.listdir(folder):
            item_path = os.path.join(folder, item)
            if os.path.isdir(item_path):
                return True
        return False

    total_videos = 0
    for folder in folders:
        folder_path = os.path.join(temp_dir, folder)
        total_videos += len(get_all_video_files(folder_path))

    def process_videos(task_id, campaign_id, folders, config, total_videos):
        try:
            socketio.emit('progress', {'task_id': task_id, 'progress': 0, 'step': f"0/{total_videos}"})
            processed_videos = 0

            with tqdm(total=total_videos, desc="Processing videos") as pbar:
                last_update_time = time.time()  # Initialize the last update time
                for folder in folders:
                    check_cancellation(task_id)
                    folder_path = os.path.join(temp_dir, folder)

                    if has_subfolders(folder_path):
                        for subfolder in os.listdir(folder_path):
                            subfolder_path = os.path.join(folder_path, subfolder)
                            if os.path.isdir(subfolder_path):
                                video_files = get_all_video_files(subfolder_path)
                                if not video_files:
                                    continue

                                ad_set = create_ad_set(campaign_id, subfolder, video_files, task_id)
                                if not ad_set:
                                    continue

                                with ThreadPoolExecutor(max_workers=5) as executor:
                                    future_to_video = {executor.submit(create_ad, ad_set.get_id(), video, config, task_id): video for video in video_files}

                                    for future in as_completed(future_to_video):
                                        check_cancellation(task_id)
                                        video = future_to_video[future]
                                        try:
                                            future.result()
                                        except TaskCanceledException:
                                            print(f"Task {task_id} has been canceled during processing video {video}.")
                                            return
                                        except Exception as e:
                                            print(f"Error processing video {video}: {e}")
                                            socketio.emit('error', {'task_id': task_id, 'message': str(e)})
                                        finally:
                                            processed_videos += 1
                                            pbar.update(1)
                                            
                                            # Periodically emit progress updates
                                            current_time = time.time()
                                            if current_time - last_update_time >= 0.5:  # Update every 0.5 seconds
                                                socketio.emit('progress', {'task_id': task_id, 'progress': processed_videos / total_videos * 100, 'step': f"{processed_videos}/{total_videos}"})
                                                last_update_time = current_time

                    else:
                        video_files = get_all_video_files(folder_path)
                        if not video_files:
                            continue

                        ad_set = create_ad_set(campaign_id, folder, video_files, task_id)
                        if not ad_set:
                            continue

                        with ThreadPoolExecutor(max_workers=5) as executor:
                            future_to_video = {executor.submit(create_ad, ad_set.get_id(), video, config, task_id): video for video in video_files}

                            for future in as_completed(future_to_video):
                                check_cancellation(task_id)
                                video = future_to_video[future]
                                try:
                                    future.result()
                                except TaskCanceledException:
                                    print(f"Task {task_id} has been canceled during processing video {video}.")
                                    return
                                except Exception as e:
                                    print(f"Error processing video {video}: {e}")
                                    socketio.emit('error', {'task_id': task_id, 'message': str(e)})
                                finally:
                                    processed_videos += 1
                                    pbar.update(1)
                                    
                                    # Periodically emit progress updates
                                    current_time = time.time()
                                    if current_time - last_update_time >= 0.5:
                                        print(f"Emitting progress: {processed_videos / total_videos * 100}% ({processed_videos}/{total_videos})")  # Add this line
                                        socketio.emit('progress', {'task_id': task_id, 'progress': processed_videos / total_videos * 100, 'step': f"{processed_videos}/{total_videos}"})
                                        last_update_time = current_time

            socketio.emit('progress', {'task_id': task_id, 'progress': 100, 'step': f"{total_videos}/{total_videos}"})
            socketio.emit('task_complete', {'task_id': task_id})
        except TaskCanceledException:
            print(f"Task {task_id} has been canceled during video processing.")
        except Exception as e:
            print(f"Error in processing videos: {e}")
            socketio.emit('error', {'task_id': task_id, 'message': str(e)})
        finally:
            with tasks_lock:
                process_pids.pop(task_id, None)
            # Clean up temporary files
            shutil.rmtree(temp_dir, ignore_errors=True)

    socketio.start_background_task(target=process_videos, task_id=task_id, campaign_id=campaign_id, folders=folders, config=config, total_videos=total_videos)

    return jsonify({"message": "Campaign processing started", "task_id": task_id})

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

if __name__ == "__main__":
    socketio.run(app, debug=True, host='0.0.0.0')
