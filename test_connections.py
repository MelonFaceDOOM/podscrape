from db_client import get_db_client
from sftp_client import get_sftp_client
from dotenv import load_dotenv
import os
load_dotenv()


def test_db():
    with get_db_client as db:
        print(db.ep_count())


def test_sftp():
    sftp = get_sftp_client()
    sftp.list_root_directory()
    podcast_dir = os.getenv("SFTP_SAVE_FOLDER")
    sftp.list_directory(podcast_dir)


if __name__ == "__main__":
    test_db()
    # test_sftp()
