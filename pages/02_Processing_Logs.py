import streamlit as st
import os
import time
from datetime import datetime
from config import STAGE1_LOG_FILE


# Page configuration
st.set_page_config(
    page_title="Processing Logs - Kimball Stage1",
    page_icon="📋",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .log-container {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 0.5rem;
        padding: 1rem;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
        max-height: 600px;
        overflow-y: auto;
    }
    .log-line {
        margin: 0.1rem 0;
        padding: 0.1rem 0;
    }
    .log-info { color: #0066cc; }
    .log-success { color: #28a745; }
    .log-warning { color: #ffc107; }
    .log-error { color: #dc3545; }
    .log-start { color: #17a2b8; }
    .log-complete { color: #28a745; }
</style>
""", unsafe_allow_html=True)


def read_log_file(log_file_path, max_lines=1000):
    """Read the log file and return the last N lines."""
    try:
        if not os.path.exists(log_file_path):
            return []
        
        with open(log_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            return lines[-max_lines:] if len(lines) > max_lines else lines
    except Exception as e:
        st.error(f"Error reading log file: {e}")
        return []


def format_log_line(line):
    """Format a log line with appropriate styling."""
    line = line.strip()
    if not line:
        return ""
    
    # Add CSS classes based on log level
    if "INFO" in line:
        return f'<div class="log-line log-info">{line}</div>'
    elif "SUCCESS" in line or "Completed" in line:
        return f'<div class="log-line log-success">{line}</div>'
    elif "WARNING" in line:
        return f'<div class="log-line log-warning">{line}</div>'
    elif "ERROR" in line:
        return f'<div class="log-line log-error">{line}</div>'
    elif "Starting" in line:
        return f'<div class="log-line log-start">{line}</div>'
    else:
        return f'<div class="log-line">{line}</div>'


def main():
    """Main function for the Processing Logs page."""
    st.markdown('<h1 class="main-header">📋 Processing Logs</h1>', unsafe_allow_html=True)
    
    # Navigation
    if st.button("← Back to Home"):
        st.markdown("Navigate to the Home page using the sidebar menu.")
    
    # Check if collections are being processed
    if 'collections_to_process' in st.session_state:
        st.info(f"📊 Monitoring processing for {len(st.session_state.collections_to_process)} collections")
        st.write("**Collections being processed:**")
        for i, collection in enumerate(st.session_state.collections_to_process, 1):
            st.write(f"{i}. {collection}")
    
    # Log file controls
    col1, col2, col3 = st.columns(3)
    
    with col1:
        auto_refresh = st.checkbox("🔄 Auto-refresh logs", value=True)
    
    with col2:
        refresh_interval = st.selectbox(
            "Refresh interval:",
            [1, 2, 5, 10, 30],
            index=1,
            format_func=lambda x: f"{x} seconds"
        )
    
    with col3:
        if st.button("🔄 Manual Refresh"):
            st.experimental_rerun()
    
    # Log file display
    st.markdown("### 📄 Stage1 Processing Logs")
    
    # Check if log file exists
    if not os.path.exists(STAGE1_LOG_FILE):
        st.warning("📋 No log file found. Start processing collections to see logs here.")
        st.info("Go back to the Home page and select collections to process.")
        return
    
    # Get log file info
    log_file_size = os.path.getsize(STAGE1_LOG_FILE) / 1024  # KB
    log_file_modified = datetime.fromtimestamp(os.path.getmtime(STAGE1_LOG_FILE))
    
    # Display log file info
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Log File Size", f"{log_file_size:.1f} KB")
    with col2:
        st.metric("Last Modified", log_file_modified.strftime("%H:%M:%S"))
    with col3:
        st.metric("Status", "Active" if auto_refresh else "Manual")
    
    # Read and display logs
    log_lines = read_log_file(STAGE1_LOG_FILE, max_lines=500)
    
    if log_lines:
        # Format log lines
        formatted_logs = []
        for line in log_lines:
            formatted_line = format_log_line(line)
            if formatted_line:
                formatted_logs.append(formatted_line)
        
        # Display logs
        st.markdown(f"""
        <div class="log-container">
            {''.join(formatted_logs)}
        </div>
        """, unsafe_allow_html=True)
        
        # Show log statistics
        st.markdown("### 📊 Log Statistics")
        
        info_count = sum(1 for line in log_lines if "INFO" in line)
        success_count = sum(1 for line in log_lines if "SUCCESS" in line or "Completed" in line)
        warning_count = sum(1 for line in log_lines if "WARNING" in line)
        error_count = sum(1 for line in log_lines if "ERROR" in line)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Info", info_count)
        with col2:
            st.metric("Success", success_count)
        with col3:
            st.metric("Warnings", warning_count)
        with col4:
            st.metric("Errors", error_count)
        
        # Check if processing is complete
        if any("Stage1 processing complete" in line for line in log_lines):
            st.success("🎉 Processing completed! You can now explore the generated Parquet files.")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("📊 View Parquet Files", type="primary"):
                    st.markdown("Navigate to the Parquet Explorer page using the sidebar menu.")
            with col2:
                if st.button("🔄 Process More Collections"):
                    st.markdown("Navigate to the Home page using the sidebar menu.")
    
    else:
        st.info("📋 Log file is empty. Start processing collections to see logs here.")
    
    # Auto-refresh functionality
    if auto_refresh:
        time.sleep(refresh_interval)
        st.experimental_rerun()


if __name__ == "__main__":
    main()
