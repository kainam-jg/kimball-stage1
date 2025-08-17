import json
import os
from pathlib import Path

# Load configuration from JSON file
def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

config = load_config()

# MongoDB Configuration
MONGODB_URI = config['mongodb']['uri']
DATABASES = config['mongodb']['databases']

# Export Configuration
EXPORT_BASE_DIR = config['export']['base_directory']
STAGE1_DIR = config['export']['stage1_directory']

# Logging Configuration
LOG_LEVEL = config['logging']['level']
LOG_FORMAT = config['logging']['format']
STAGE1_LOG_FILE = config['logging']['stage1_log_file']

def get_export_path():
    """Get the base export directory path"""
    return EXPORT_BASE_DIR

def get_stage1_path():
    """Get the Stage1 export directory path"""
    return os.path.join(EXPORT_BASE_DIR, STAGE1_DIR)

# Ensure directories exist
def ensure_directories():
    """Create necessary directories if they don't exist"""
    Path(EXPORT_BASE_DIR).mkdir(exist_ok=True)
    Path(get_stage1_path()).mkdir(exist_ok=True)
    Path('logs').mkdir(exist_ok=True)

# Create directories on import
ensure_directories()
