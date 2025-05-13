import subprocess
import psutil
import platform
import sys


# Path to the thing.py script
SCRIPT_NAME = "podscrape.py"
try:
    save_location = sys.argv[1]
except:
    raise ValueError("must provide 'local' or 'remote' argument to specify where to save data created from this script")
if save_location not in ["local", "remote"]:
    raise ValueError("must provide 'local' or 'remote' argument to specify where to save data created from this script")


def main():
    if is_process_running(SCRIPT_NAME):
        raise Exception(f"{SCRIPT_NAME} is already running.")
    else:
        current_os = platform.system()
        if current_os == "Windows":
            subprocess.Popen(['python', SCRIPT_NAME, f"update_{save_location}"], creationflags=subprocess.CREATE_NEW_CONSOLE)
            print(f"Started {SCRIPT_NAME} on Windows")
        elif current_os == "Linux":
            with open('log.txt', 'a') as log_file:
                subprocess.Popen(['nohup', 'python', SCRIPT_NAME, f"update_{save_location}"], stdout=log_file, stderr=log_file)
            print(f"Started {SCRIPT_NAME} on Linux with nohup, logging to log.txt")
        else:
            raise Exception("Unsupported operating system")


def is_process_running(script_name):
    """Check if a process with the given script name is already running."""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and script_name in cmdline:
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False

    
if __name__ == "__main__":
    main()
