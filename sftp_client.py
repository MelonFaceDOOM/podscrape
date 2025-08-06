import paramiko
from dotenv import load_dotenv
import os

load_dotenv()


def get_sftp_client():
    # psycopg2 var: .env var
    sftp_credential_map = {
        "username": "SFTP_USERNAME",
        "password": "SFTP_PASSWORD",
        "host": "SFTP_HOST",
        "port": "SFTP_PORT",
    }
    sftp_credentials = {}
    missing = []
    for k, v in sftp_credential_map.items():
        loaded = os.getenv(v)
        if loaded is None:
            missing.append(k)
        else:
            sftp_credentials[k] = loaded
    if missing:
        raise RuntimeError(
            f"Cannot connect to SFTP: missing required environment variables: "
            f"{', '.join(missing)}"
        )
    sftp_credentials["port"] = int(sftp_credentials["port"])
    return SFTPClient(**sftp_credentials)


class SFTPClient:
    """
    usage should generally be:
        with get_db_client() as db:
            db.dostuff()
    this way conn auto-closes once the with-context is exited
    """

    def __init__(self, username, password, host, port):
        self.transport = paramiko.Transport(host, port)
        self.transport.connect(username=username, password=password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def close(self):
        self.sftp.close()
        self.transport.close()

    def list_root_directory(self, limit=10):
        """List up to `limit` items at the root directory `/`."""
        try:
            items = self.sftp.listdir('/')
            print(f"Items at root (/): {items[:limit]}")
            return items[:limit]
        except Exception as e:
            print(f"Error listing root directory: {e}")
            return []

    def list_directory(self, path, limit=10):
        """List up to `limit` items at the given directory path."""
        try:
            items = self.sftp.listdir(path)
            print(f"Items at '{path}': {items[:limit]}")
            return items[:limit]
        except Exception as e:
            print(f"Error listing directory '{path}': {e}")
            return []

    def locate_files(self, expected_filenames, sftp_dir = '/'):
        """
        checks whether expected_filenames exist in sftp_dir
        """
        actual_filenames = self.sftp.listdir(sftp_dir)
        sftp_urls = set(os.path.join(sftp_dir.strip('/'), name) for name in actual_filenames)
        normalized_expected = [
            os.path.join(
                sftp_dir.strip('/'),
                p.replace('\\', '/').lstrip('/')
            ) for p in expected_filenames
        ]

        found = [p for p in normalized_expected if p in sftp_urls]
        missing = [p for p in normalized_expected if p not in sftp_urls]
        return found, missing

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()