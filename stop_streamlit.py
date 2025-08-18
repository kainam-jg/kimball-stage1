#!/usr/bin/env python3
"""
Kimball Stage1 Streamlit Application Stopper
Cross-platform script to stop the Streamlit application
"""

import os
import sys
import subprocess
import signal
import platform
import psutil
from pathlib import Path

def print_banner():
    """Print application banner."""
    print("=" * 50)
    print("    Stopping Streamlit Application")
    print("=" * 50)
    print()

def find_streamlit_processes():
    """Find all running Streamlit processes."""
    streamlit_processes = []
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # Check if it's a Python process running Streamlit
            if proc.info['name'] and 'python' in proc.info['name'].lower():
                cmdline = proc.info['cmdline']
                if cmdline and any('streamlit' in arg.lower() for arg in cmdline):
                    streamlit_processes.append(proc)
            
            # Also check for direct streamlit processes
            elif proc.info['name'] and 'streamlit' in proc.info['name'].lower():
                streamlit_processes.append(proc)
                
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    
    return streamlit_processes

def find_processes_on_port(port):
    """Find processes using a specific port."""
    processes = []
    
    for conn in psutil.net_connections():
        if conn.laddr.port == port and conn.status == 'LISTEN':
            try:
                proc = psutil.Process(conn.pid)
                processes.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    
    return processes

def stop_process(proc, force=False):
    """Stop a process gracefully or forcefully."""
    try:
        if force:
            proc.kill()
            print(f"✓ Forcefully killed process {proc.pid}")
        else:
            proc.terminate()
            print(f"✓ Terminated process {proc.pid}")
        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
        print(f"✗ Could not stop process {proc.pid}: {e}")
        return False

def stop_streamlit():
    """Stop all Streamlit processes."""
    print("Looking for Streamlit processes...")
    
    # Find processes on port 8501
    port_processes = find_processes_on_port(8501)
    if port_processes:
        print(f"Found {len(port_processes)} process(es) on port 8501:")
        for proc in port_processes:
            print(f"  - PID {proc.pid}: {proc.name()}")
            stop_process(proc)
    else:
        print("No processes found on port 8501")
    
    # Find Streamlit processes
    streamlit_processes = find_streamlit_processes()
    if streamlit_processes:
        print(f"\nFound {len(streamlit_processes)} Streamlit process(es):")
        for proc in streamlit_processes:
            print(f"  - PID {proc.pid}: {proc.name()}")
            stop_process(proc)
    else:
        print("No Streamlit processes found")
    
    # Wait a moment and check if any processes are still running
    time.sleep(1)
    
    # Force kill any remaining processes on port 8501
    remaining_port_processes = find_processes_on_port(8501)
    if remaining_port_processes:
        print(f"\nForcefully stopping {len(remaining_port_processes)} remaining process(es) on port 8501:")
        for proc in remaining_port_processes:
            stop_process(proc, force=True)
    
    # Force kill any remaining Streamlit processes
    remaining_streamlit_processes = find_streamlit_processes()
    if remaining_streamlit_processes:
        print(f"\nForcefully stopping {len(remaining_streamlit_processes)} remaining Streamlit process(es):")
        for proc in remaining_streamlit_processes:
            stop_process(proc, force=True)

def main():
    """Main function."""
    print_banner()
    
    try:
        # Check if psutil is available
        import psutil
    except ImportError:
        print("✗ Error: psutil package is required")
        print("Please install it with: pip install psutil")
        input("Press Enter to exit...")
        return 1
    
    # Stop Streamlit processes
    stop_streamlit()
    
    print("\n✓ Streamlit application stopped.")
    print()
    
    # Check if port is now free
    remaining = find_processes_on_port(8501)
    if not remaining:
        print("✓ Port 8501 is now free")
    else:
        print(f"⚠️  Warning: {len(remaining)} process(es) still using port 8501")
    
    input("Press Enter to exit...")
    return 0

if __name__ == "__main__":
    import time
    sys.exit(main())
