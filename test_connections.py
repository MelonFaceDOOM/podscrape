# test_connections.py
from dotenv import load_dotenv
load_dotenv()

from db_client import get_db_client
from sftp_client import get_sftp_client
import os

def test_db():
    try:
        with get_db_client() as db:
            ep_count = db.ep_count()
        print(f"SUCCESS: connected to DB. {ep_count} episodes found")
    except Exception as e:
        print(f"FAIL: DB connection: {e}")

def test_sftp():
    try:
        with get_sftp_client() as sftp:
            root_dirs = sftp.list_root_directory()
        print(f"SUCCESS: connected to SFTP. Root dir shows {root_dirs}")
    except Exception as e:
        print(f"FAIL: SFTP connection: {e}")

def test_torch_device():
    # Only import torch here, after env is set, and WITHOUT importing faster_whisper.
    import torch
    print(f"PyTorch: {torch.__version__}")
    print("cuDNN available:", torch.backends.cudnn.is_available())
    if torch.cuda.is_available():
        print(f"CUDA devices: {torch.cuda.device_count()}")
        for i in range(torch.cuda.device_count()):
            print(f"  {i}: {torch.cuda.get_device_name(i)}")
        dev = torch.device("cuda")
    else:
        print("CUDA not available; using CPU")
        dev = torch.device("cpu")
    try:
        a = torch.rand(3,3, device=dev); b = torch.rand(3,3, device=dev); c = a+b
        print("Tensor op OK on", dev, "\n", c)
    except Exception as e:
        print("Tensor op failed:", e)

def test_asr_load():
    """make sure ASR model loads without torch import conflicts."""
    from whisper_runtime import get_model
    name = os.getenv("ASR_MODEL", "fw_base")
    device = os.getenv("ASR_DEVICE", "cuda")
    try:
        _, model, _ = get_model(name, device=device)
        print(f"SUCCESS: loaded ASR model {name} on {device}")
    except Exception as e:
        print(f"FAIL: ASR load for {name} on {device}: {e}")

if __name__ == "__main__":
    test_db()
    test_sftp()
    # Run torch test if you care about PyTorch specifically:
    test_torch_device()
    # And/or verify ASR loads:
    test_asr_load()
