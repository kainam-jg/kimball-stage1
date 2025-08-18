# Streamlit Application Scripts

This directory contains scripts to easily start and stop the Kimball Stage1 Streamlit application.

## Available Scripts

### Windows Batch Files
- `start_streamlit.bat` - Start the Streamlit application on Windows
- `stop_streamlit.bat` - Stop the Streamlit application on Windows

### Ubuntu Shell Scripts
- `start_streamlit.sh` - Start the Streamlit application on Ubuntu/Linux with nohup
- `stop_streamlit.sh` - Stop the Streamlit application on Ubuntu/Linux

### Cross-Platform Python Scripts
- `start_streamlit.py` - Start the Streamlit application (works on all platforms)
- `stop_streamlit.py` - Stop the Streamlit application (works on all platforms)

## Quick Start

### Windows Users
```bash
# Start the application
start_streamlit.bat

# Stop the application
stop_streamlit.bat
```

### Ubuntu/Linux Users
```bash
# Start the application (runs in background with nohup)
./start_streamlit.sh

# Stop the application
./stop_streamlit.sh

# View logs
tail -f streamlit.log
```

### All Platforms
```bash
# Start the application
python start_streamlit.py

# Stop the application
python stop_streamlit.py
```

## Features

### Start Scripts
- ✅ **Dependency Check**: Verifies Python and required packages are installed
- ✅ **Configuration Check**: Warns if config.json is missing
- ✅ **Automatic Setup**: Sets optimal Streamlit settings
- ✅ **Error Handling**: Clear error messages and guidance
- ✅ **Graceful Shutdown**: Handles Ctrl+C properly
- ✅ **Background Execution**: Ubuntu scripts use nohup for background operation

### Stop Scripts
- ✅ **Process Detection**: Finds Streamlit processes by name and port
- ✅ **Graceful Termination**: Attempts graceful shutdown first
- ✅ **Force Kill**: Forcefully stops stubborn processes
- ✅ **Port Verification**: Confirms port 8501 is freed
- ✅ **Cross-Platform**: Works on Windows, macOS, and Linux
- ✅ **PID Management**: Ubuntu scripts manage PID files for reliable process tracking

## Application Access

Once started, the application will be available at:
- **URL**: http://localhost:8501
- **Port**: 8501 (configurable in scripts)

## Troubleshooting

### Port Already in Use
If you get a "port already in use" error:
1. Run the stop script first: `python stop_streamlit.py`
2. Wait a few seconds
3. Run the start script again: `python start_streamlit.py`

### Missing Dependencies
If you get dependency errors:
```bash
pip install -r requirements.txt
```

### Configuration Issues
If you get configuration warnings:
1. Copy `config.sample.json` to `config.json`
2. Update the MongoDB connection details in `config.json`

## Manual Commands

If you prefer to run Streamlit manually:

```bash
# Start manually
streamlit run "01_Find_Collections.py" --server.port 8501 --server.address localhost

# Stop manually (Ctrl+C in the terminal)
```

## Script Configuration

You can modify the scripts to change:
- **Port**: Change `8501` to any available port
- **Host**: Change `localhost` to `0.0.0.0` for network access
- **Settings**: Add additional Streamlit command-line options

## Security Notes

- The application runs on `localhost` by default (local access only)
- For network access, change `--server.address localhost` to `--server.address 0.0.0.0`
- Always use HTTPS in production environments
