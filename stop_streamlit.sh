#!/bin/bash
# Kimball Stage1 Streamlit Application Stopper for Ubuntu
# This script stops the Streamlit application that was started with nohup

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_NAME="Kimball Stage1 Streamlit App"
PORT=8501
PID_FILE="streamlit.pid"
LOG_FILE="streamlit.log"

echo -e "${BLUE}========================================"
echo -e "    Stopping $APP_NAME"
echo -e "========================================${NC}"
echo ""

# Function to find Streamlit processes
find_streamlit_processes() {
    local processes=()
    
    # Find processes by PID file
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            processes+=("$pid")
        fi
    fi
    
    # Find processes by port
    local port_pids=$(lsof -ti :$PORT 2>/dev/null)
    if [ ! -z "$port_pids" ]; then
        for pid in $port_pids; do
            if [[ ! " ${processes[@]} " =~ " ${pid} " ]]; then
                processes+=("$pid")
            fi
        done
    fi
    
    # Find Python processes running Streamlit
    local streamlit_pids=$(pgrep -f "streamlit.*run.*$APP_FILE" 2>/dev/null)
    if [ ! -z "$streamlit_pids" ]; then
        for pid in $streamlit_pids; do
            if [[ ! " ${processes[@]} " =~ " ${pid} " ]]; then
                processes+=("$pid")
            fi
        done
    fi
    
    echo "${processes[@]}"
}

# Function to stop a process gracefully
stop_process() {
    local pid=$1
    local force=$2
    
    if [ -z "$pid" ]; then
        return 1
    fi
    
    # Check if process exists
    if ! kill -0 "$pid" 2>/dev/null; then
        echo -e "${YELLOW}Process $pid is not running${NC}"
        return 1
    fi
    
    # Get process info
    local process_info=$(ps -p "$pid" -o pid,ppid,cmd --no-headers 2>/dev/null)
    
    if [ "$force" = "true" ]; then
        echo -e "${YELLOW}Forcefully killing process $pid${NC}"
        kill -9 "$pid" 2>/dev/null
        echo -e "${GREEN}✓ Forcefully killed process $pid${NC}"
    else
        echo -e "${BLUE}Gracefully terminating process $pid${NC}"
        echo -e "${YELLOW}Process info: $process_info${NC}"
        kill "$pid" 2>/dev/null
        
        # Wait for graceful shutdown
        local count=0
        while kill -0 "$pid" 2>/dev/null && [ $count -lt 10 ]; do
            sleep 1
            ((count++))
        done
        
        # Force kill if still running
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${YELLOW}Process $pid did not terminate gracefully, force killing...${NC}"
            kill -9 "$pid" 2>/dev/null
            echo -e "${GREEN}✓ Forcefully killed process $pid${NC}"
        else
            echo -e "${GREEN}✓ Gracefully terminated process $pid${NC}"
        fi
    fi
    
    return 0
}

# Function to stop all Streamlit processes
stop_streamlit() {
    echo -e "${BLUE}Looking for Streamlit processes...${NC}"
    
    # Get all Streamlit processes
    local processes=($(find_streamlit_processes))
    
    if [ ${#processes[@]} -eq 0 ]; then
        echo -e "${YELLOW}No Streamlit processes found running${NC}"
        return 0
    fi
    
    echo -e "${BLUE}Found ${#processes[@]} Streamlit process(es):${NC}"
    
    # Stop each process
    for pid in "${processes[@]}"; do
        stop_process "$pid" false
    done
    
    # Wait a moment and check for any remaining processes
    sleep 2
    
    # Check for any remaining processes and force kill them
    local remaining_processes=($(find_streamlit_processes))
    if [ ${#remaining_processes[@]} -gt 0 ]; then
        echo -e "${YELLOW}Forcefully stopping ${#remaining_processes[@]} remaining process(es):${NC}"
        for pid in "${remaining_processes[@]}"; do
            stop_process "$pid" true
        done
    fi
}

# Function to clean up files
cleanup_files() {
    # Remove PID file if it exists
    if [ -f "$PID_FILE" ]; then
        rm -f "$PID_FILE"
        echo -e "${GREEN}✓ Removed PID file: $PID_FILE${NC}"
    fi
    
    # Check if log file exists and show its size
    if [ -f "$LOG_FILE" ]; then
        local log_size=$(du -h "$LOG_FILE" | cut -f1)
        echo -e "${BLUE}Log file exists: $LOG_FILE (size: $log_size)${NC}"
        echo -e "${YELLOW}To view logs: tail -f $LOG_FILE${NC}"
    fi
}

# Function to check if port is now free
check_port_status() {
    if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  Warning: Port $PORT is still in use${NC}"
        local remaining_pids=$(lsof -ti :$PORT 2>/dev/null)
        echo -e "${YELLOW}Remaining processes on port $PORT: $remaining_pids${NC}"
    else
        echo -e "${GREEN}✓ Port $PORT is now free${NC}"
    fi
}

# Main execution
main() {
    # Stop Streamlit processes
    stop_streamlit
    
    echo ""
    
    # Clean up files
    cleanup_files
    
    echo ""
    
    # Check port status
    check_port_status
    
    echo ""
    echo -e "${GREEN}✓ Streamlit application stopped.${NC}"
    echo ""
}

# Run main function
main
