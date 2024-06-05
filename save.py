import paramiko
import os
import time
import requests
from db_client import get_client
from config import SSH_CREDENTIALS, NETWORK_DB_CREDENTIALS, PRIVATE_KEY_PATH, LOCAL_SAVE_FOLDER, SFTP_SAVE_FOLDER


"""
Download podcasts
    1)save mp3 to sftp server
    2)log ep info to db 
"""


def download_episodes_and_save_remotely(episodes):
    # meant to be run on a remote pc that wishes to save to sftp pc
    # episodes should be a list of dicts 
    # expected dict format can be found in rss.py
    client = get_client()
    private_key = paramiko.RSAKey(filename=PRIVATE_KEY_PATH)
    transport = paramiko.Transport((SSH_CREDENTIALS['host'], SSH_CREDENTIALS['port']))
    transport.connect(username=SSH_CREDENTIALS['username'], pkey=private_key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    
    consecutive_fails = 0 
    for episode in episodes:
        try:
            download_url = episode['downloadUrl']
            filename = make_filename(episode)
            remote_path = os.path.join(SFTP_SAVE_FOLDER, filename)
            download_and_upload_episode(download_url, remote_path, sftp)
            client.insert_episode(episode)
            consecutive_fails = 0
        except Exception as e:
            consecutive_fails += 1
            time.sleep(2**consecutive_fails)
            print(e)
    sftp.close()
    transport.close()
    client.close()
    

def download_and_upload_episode(download_url, remote_path, sftp):
    with requests.get(download_url, stream=True) as r:
        r.raise_for_status()
        with sftp.file(remote_path, 'wb') as remote_file:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    remote_file.write(chunk)
    
        
def download_episodes_and_save_locally(episodes):
    # meant to be run on the sftp pc, not a remote pc
    client = get_client()
    consecutive_fails = 0 
    for episode in episodes:
        try:
            download_url = episode['downloadUrl']
            # os.makedirs(LOCAL_SAVE_FOLDER, exist_ok=True)
            filename = make_filename(episode)
            save_location = os.path.join(LOCAL_SAVE_FOLDER, filename)
            download_episode_locally(download_url, save_location)
            client.insert_episode(episode)
            consecutive_fails = 0
        except Exception as e:
            consecutive_fails += 1
            time.sleep(2**consecutive_fails)
            print(e)
    client.close()
    
    
def download_episode_locally(download_url, save_location):
    with requests.get(download_url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(save_location, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
                

def make_filename(episode):
    filename = episode['downloadUrl'].split('/')[-1]  # this filename is only referenced to get the extension. a custom filename will be used for the rest of the name
    extension = filename.split('.')[-1]
    extension = extension.split('?')[0] # remove any ? data that might be on the end of the url
    return episode['unique_id'] + "." + extension
    

#####
# alternate dl/ul things that aren't really part of the application:
def bulk_upload_files(local_directory, remote_directory, sftp):
    for filename in os.listdir(local_directory):
        if filename.endswith(".mp3"):
            local_path = os.path.join(local_directory, filename)
            remote_path = os.path.join(remote_directory, filename)
            sftp.put(local_path, remote_path)
            
            
def download_episodes(episodes):    
    for episode in episodes:
        download_url = episode['downloadUrl']
        os.makedirs(LOCAL_SAVE_FOLDER, exist_ok=True)
        filename = make_filename(episode)
        save_location = os.path.join(LOCAL_SAVE_FOLDER, filename)
        download_episode_locally(download_url, save_location)


def delete_remote_files(to_delete):    
    private_key = paramiko.RSAKey(filename=PRIVATE_KEY_PATH)
    transport = paramiko.Transport((SSH_CREDENTIALS['host'], SSH_CREDENTIALS['port']))
    transport.connect(username=SSH_CREDENTIALS['username'], pkey=private_key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    for remote_path in to_delete:
        sftp.remove(remote_path)
         
         
def transfer_single_file(filepath):
    private_key = paramiko.RSAKey(filename=PRIVATE_KEY_PATH)
    transport = paramiko.Transport((SSH_CREDENTIALS['host'], SSH_CREDENTIALS['port']))
    transport.connect(username=SSH_CREDENTIALS['username'], pkey=private_key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    filename = os.path.basename(filepath)
    remote_path = os.path.join(SFTP_SAVE_FOLDER, filename)
    sftp.put(filepath, remote_path)