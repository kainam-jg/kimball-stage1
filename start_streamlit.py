#!/usr/bin/env python3
"""
Kimball Stage1 Streamlit Application Launcher
Cross-platform script to start the Streamlit application
"""

import os
import sys
import subprocess
import signal
import time
import platform
from pathlib import Path

def print_banner():
    """Print application banner."""
    print("=" * 50)
    print("    Kimball Stage1 Streamlit App")
    print("=" * 50)
    print()
    print("Starting Streamlit application...")
    print()
    print("The app will be available at:")
    print("http://localhost:8501")
    print()
    print("Press Ctrl+C to stop the application")
    print()
    print("=" * 50)

def check_python():
    """Check if Python is available."""
    try:
        version = sys.version_info
        print(f"✓ Python {version.major}.{version.minor}.{version.micro} detected")
        return True
    except Exception as e:
        print(f"✗ Error checking Python: {e}")
        return False

def check_dependencies():
    """Check if required packages are installed."""
    required_packages = ['streamlit', 'pandas', 'pymongo', 'duckdb']
    missing_packages = []
    
    print("Checking dependencies...")
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package} is installed")
        except ImportError:
            print(f"✗ {package} is missing")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\nMissing packages: {', '.join(missing_packages)}")
        print("Please run: pip install -r requirements.txt")
        return False
    
    return True

def check_config():
    """Check if configuration file exists."""
    config_file = Path("config.json")
    if not config_file.exists():
        print("⚠️  Warning: config.json not found")
        print("Please copy config.sample.json to config.json and update with your settings")
        return False
    print("✓ Configuration file found")
    return True

def start_streamlit():
    """Start the Streamlit application."""
    try:
        # Get the directory of this script
        script_dir = Path(__file__).parent
        app_file = script_dir / "01_Find_Collections.py"
        
        if not app_file.exists():
            print(f"✗ Error: {app_file} not found")
            return False
        
        print("Starting Streamlit...")
        
        # Start Streamlit with specific settings
        cmd = [
            sys.executable, "-m", "streamlit", "run",
            str(app_file),
            "--server.port", "8501",
            "--server.address", "localhost",
            "--server.headless", "true",
            "--browser.gatherUsageStats", "false"
        ]
        
        # Run the command
        process = subprocess.Popen(cmd, cwd=script_dir)
        
        print(f"✓ Streamlit started with PID: {process.pid}")
        print("✓ Application is running at http://localhost:8501")
        print("\nPress Ctrl+C to stop the application...")
        
        # Wait for the process to complete
        try:
            process.wait()
        except KeyboardInterrupt:
            print("\n\nStopping Streamlit...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            print("✓ Streamlit stopped")
        
        return True
        
    except Exception as e:
        print(f"✗ Error starting Streamlit: {e}")
        return False

def main():
    """Main function."""
    print_banner()
    
    # Check prerequisites
    if not check_python():
        input("Press Enter to exit...")
        return 1
    
    if not check_dependencies():
        input("Press Enter to exit...")
        return 1
    
    check_config()  # Warning only, don't exit
    
    print()
    
    # Start Streamlit
    if start_streamlit():
        return 0
    else:
        input("Press Enter to exit...")
        return 1

if __name__ == "__main__":
    sys.exit(main())
