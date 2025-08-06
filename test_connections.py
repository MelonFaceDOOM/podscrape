from db_client import get_db_client
from sftp_client import get_sftp_client
from dotenv import load_dotenv
import os
import torch

load_dotenv()


def test_torch_device():
    print(f"PyTorch version: {torch.__version__}")
    print(torch.backends.cudnn.is_available())  # should be True
    # Check for CUDA availability
    if torch.cuda.is_available():
        print(f"CUDA is available. Device count: {torch.cuda.device_count()}")
        for i in range(torch.cuda.device_count()):
            print(f"Device {i}: {torch.cuda.get_device_name(i)}")
        device = torch.device("cuda")
    else:
        print("CUDA is NOT available. Using CPU.")
        device = torch.device("cpu")

    # Perform a simple tensor operation on the selected device
    try:
        a = torch.rand(3, 3, device=device)
        b = torch.rand(3, 3, device=device)
        c = a + b
        print(f"Tensor operation successful on {device}:")
        print(c)
    except Exception as e:
        print(f"Tensor operation failed on {device}: {e}")


def troubleshoot_torch():
    print(torch.__version__)
    print(torch.version.cuda)
    print(torch.cuda.is_available())
    print(torch.cuda.get_device_name(0)
          if torch.cuda.is_available() else "No GPU")


def test_db():
    try:
        with get_db_client() as db:
            ep_count = db.ep_count()
        print(f"SUCCESS: connected to db. {ep_cont} episodes found")
    except Exception as e: 
        print(f"FAIL: could not connect to DB: {e}")


def test_sftp():
    try:
        with get_sftp_client() as sftp:
            root_dirs = sftp.list_root_directory()
        print(f"SUCCESS: connected to sftp. Root dir shows {root_dirs}")
    except Exception as e:
        print(f"FAIL: could not connect to SFTP: {e}")


if __name__ == "__main__":
    # test_db()
    # test_sftp()
    test_torch_device()
    # troubleshoot_torch()
