#!/bin/bash
# Kimball Stage1 Streamlit Application Launcher for Ubuntu
# This script starts the Streamlit application in the background using nohup

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_NAME="Kimball Stage1 Streamlit App"
APP_FILE="01_Find_Collections.py"
PORT=8501
HOST="localhost"
PID_FILE="streamlit.pid"
LOG_FILE="streamlit.log"

echo -e "${BLUE}========================================"
echo -e "    $APP_NAME"
echo -e "========================================${NC}"
echo ""
echo -e "${BLUE}Starting Streamlit application...${NC}"
echo ""
echo -e "The app will be available at:"
echo -e "${GREEN}http://$HOST:$PORT${NC}"
echo ""
echo -e "Process will run in background with nohup"
echo -e "Log file: ${YELLOW}$LOG_FILE${NC}"
echo -e "PID file: ${YELLOW}$PID_FILE${NC}"
echo ""
echo -e "${BLUE}========================================${NC}"

# Function to check if Python is available
check_python() {
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}ERROR: Python3 is not installed or not in PATH${NC}"
        echo "Please install Python3 and try again"
        exit 1
    fi
    
    python_version=$(python3 --version 2>&1)
    echo -e "${GREEN}✓ $python_version detected${NC}"
}

# Function to check if required packages are installed
check_dependencies() {
    echo -e "${BLUE}Checking dependencies...${NC}"
    
    required_packages=("streamlit" "pandas" "pymongo" "duckdb")
    missing_packages=()
    
    for package in "${required_packages[@]}"; do
        if python3 -c "import $package" 2>/dev/null; then
            echo -e "${GREEN}✓ $package is installed${NC}"
        else
            echo -e "${RED}✗ $package is missing${NC}"
            missing_packages+=("$package")
        fi
    done
    
    if [ ${#missing_packages[@]} -ne 0 ]; then
        echo ""
        echo -e "${RED}Missing packages: ${missing_packages[*]}${NC}"
        echo "Please run: pip3 install -r requirements.txt"
        exit 1
    fi
}

# Function to check if configuration file exists
check_config() {
    if [ ! -f "config.json" ]; then
        echo -e "${YELLOW}⚠️  Warning: config.json not found${NC}"
        echo "Please copy config.sample.json to config.json and update with your settings"
        echo -e "${YELLOW}Continuing anyway...${NC}"
    else
        echo -e "${GREEN}✓ Configuration file found${NC}"
    fi
}

# Function to check if port is already in use
check_port() {
    if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  Warning: Port $PORT is already in use${NC}"
        echo "You may want to stop any existing Streamlit processes first"
        echo -e "${YELLOW}Continuing anyway...${NC}"
    fi
}

# Function to start Streamlit
start_streamlit() {
    echo -e "${BLUE}Starting Streamlit...${NC}"
    
    # Create log directory if it doesn't exist
    mkdir -p logs
    
    # Start Streamlit with nohup
    nohup python3 -m streamlit run "$APP_FILE" \
        --server.port $PORT \
        --server.address $HOST \
        --server.headless true \
        --browser.gatherUsageStats false \
        > "$LOG_FILE" 2>&1 &
    
    # Save PID to file
    echo $! > "$PID_FILE"
    
    # Wait a moment for the process to start
    sleep 2
    
    # Check if process is running
    if kill -0 $(cat "$PID_FILE") 2>/dev/null; then
        echo -e "${GREEN}✓ Streamlit started successfully!${NC}"
        echo -e "${GREEN}✓ PID: $(cat $PID_FILE)${NC}"
        echo -e "${GREEN}✓ Application is running at http://$HOST:$PORT${NC}"
        echo ""
        echo -e "${BLUE}To stop the application, run:${NC}"
        echo -e "${YELLOW}./stop_streamlit.sh${NC}"
        echo ""
        echo -e "${BLUE}To view logs:${NC}"
        echo -e "${YELLOW}tail -f $LOG_FILE${NC}"
    else
        echo -e "${RED}✗ Failed to start Streamlit${NC}"
        echo -e "${YELLOW}Check the log file: $LOG_FILE${NC}"
        exit 1
    fi
}

# Main execution
main() {
    # Check prerequisites
    check_python
    check_dependencies
    check_config
    check_port
    
    echo ""
    
    # Start Streamlit
    start_streamlit
}

# Run main function
main
