# check_env.py

import sys
import subprocess

def check_python():
    print(f"✅ Python version: {sys.version}")

def check_pip():
    try:
        subprocess.run([sys.executable, "-m", "pip", "--version"], check=True)
    except subprocess.CalledProcessError:
        print("❌ pip not found")

def check_requirements():
    try:
        subprocess.run([sys.executable, "-m", "pip", "check"], check=True)
    except subprocess.CalledProcessError:
        print("❌ Issues found with installed packages")

if __name__ == "__main__":
    check_python()
    check_pip()
    check_requirements()
