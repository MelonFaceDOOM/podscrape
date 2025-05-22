from db_client import get_client
from config import SFTP_CREDENTIALS
import paramiko

"""some quick testing to check if the most recently-uploaded files can be found on sftp"""


client = get_client()

episodes = [ep for ep in client.get_episodes_with_no_transcript()]  # will be ordered as newest first
episodes = episodes[:10]
print(f'{len(episodes)} episodes')
for episode in episodes:
    print(episode['audio_path'])

# 2. Re‑use the same Whisper model and SFTP connection for the whole batch
transport = paramiko.Transport((SFTP_CREDENTIALS["host"],
                                SFTP_CREDENTIALS["port"]))
transport.connect(username=SFTP_CREDENTIALS["username"],
                  password=SFTP_CREDENTIALS["password"])
sftp = paramiko.SFTPClient.from_transport(transport)

for episode in episodes:
    remote_path = episode['audio_path']
    try:
        sftp.stat(remote_path)
        print(f"✅ Found: {remote_path}")
    except FileNotFoundError:
        print(f"❌ Missing: {remote_path}")