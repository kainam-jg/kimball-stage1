import streamlit as st
import pandas as pd
import os
import glob
from datetime import datetime
from config import get_stage1_path


# Page configuration
st.set_page_config(
    page_title="Parquet Explorer - Kimball Stage1",
    page_icon="📊",
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
    .file-info {
        background-color: #e8f4fd;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
</style>
""", unsafe_allow_html=True)


def get_parquet_files():
    """Get all Parquet files in the stage1 directory."""
    stage1_path = get_stage1_path()
    pattern = os.path.join(stage1_path, "*.parquet")
    return glob.glob(pattern)


def load_parquet_data(file_path):
    """Load data from a Parquet file."""
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        st.error(f"Error loading file: {e}")
        return None


def display_file_overview(df, filename):
    """Display overview information about the loaded file."""
    st.markdown("### 📋 File Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Rows", f"{len(df):,}")
    
    with col2:
        st.metric("Columns", f"{len(df.columns):,}")
    
    with col3:
        memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
        st.metric("Memory Usage", f"{memory_usage:.2f} MB")
    
    with col4:
        file_size = os.path.getsize(filename) / 1024 / 1024  # MB
        st.metric("File Size", f"{file_size:.2f} MB")
    
    # Extract collection name and timestamp
    if '_stage1_' in filename:
        collection_name = os.path.basename(filename).split('_stage1_')[0]
        timestamp_part = os.path.basename(filename).split('_stage1_')[1].replace('.parquet', '')
        try:
            dt = datetime.strptime(timestamp_part, "%Y%m%d_%H%M%S")
            timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            timestamp = timestamp_part
    else:
        collection_name = os.path.basename(filename).replace('.parquet', '')
        timestamp = "Unknown"
    
    st.markdown(f"""
    <div class="file-info">
        <strong>Collection:</strong> {collection_name}<br>
        <strong>Processed:</strong> {timestamp}<br>
        <strong>Filename:</strong> {os.path.basename(filename)}
    </div>
    """, unsafe_allow_html=True)


def display_data_preview(df):
    """Display a preview of the data."""
    st.markdown("### 📊 Data Preview")
    
    # Show first few rows
    st.dataframe(df.head(100), use_container_width=True)
    
    # Show data types
    st.markdown("#### Data Types")
    dtype_df = pd.DataFrame({
        'Column': df.columns,
        'Data Type': df.dtypes.astype(str),
        'Non-Null Count': df.count(),
        'Null Count': df.isnull().sum()
    })
    st.dataframe(dtype_df, use_container_width=True)


def display_column_analysis(df):
    """Display analysis of columns."""
    st.markdown("### 📈 Column Analysis")
    
    # Data type distribution
    st.markdown("#### Data Type Distribution")
    dtype_counts = df.dtypes.value_counts()
    
    # Display as a simple table instead of chart to avoid serialization issues
    dtype_df = pd.DataFrame({
        'Data Type': dtype_counts.index.astype(str),
        'Count': dtype_counts.values
    })
    st.dataframe(dtype_df, use_container_width=True)
    
    # Column statistics for numeric columns
    numeric_columns = df.select_dtypes(include=['number']).columns
    if len(numeric_columns) > 0:
        st.markdown("#### Numeric Column Statistics")
        numeric_stats = df[numeric_columns].describe()
        st.dataframe(numeric_stats, use_container_width=True)
    
    # Categorical column analysis
    categorical_columns = df.select_dtypes(include=['object']).columns
    if len(categorical_columns) > 0:
        st.markdown("#### Categorical Column Analysis")
        
        # Show unique values for categorical columns
        for col in categorical_columns[:5]:  # Limit to first 5 columns
            unique_count = df[col].nunique()
            if unique_count <= 20:  # Only show if reasonable number of unique values
                st.markdown(f"**{col}** ({unique_count} unique values):")
                value_counts = df[col].value_counts().head(10)
                st.dataframe(value_counts.reset_index().rename(columns={'index': 'Value', col: 'Count'}), use_container_width=True)


def display_search_and_filter(df):
    """Display search and filter functionality."""
    st.markdown("### 🔍 Search and Filter")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Column filter
        selected_columns = st.multiselect(
            "Select columns to display:",
            df.columns.tolist(),
            default=df.columns.tolist()[:10]  # Default to first 10 columns
        )
    
    with col2:
        # Search functionality
        search_term = st.text_input("Search in all columns:", "")
    
    # Apply filters
    filtered_df = df[selected_columns] if selected_columns else df
    
    if search_term:
        # Search across all string columns
        mask = pd.DataFrame([filtered_df[col].astype(str).str.contains(search_term, case=False, na=False) 
                           for col in filtered_df.select_dtypes(include=['object']).columns]).any()
        filtered_df = filtered_df[mask]
    
    st.dataframe(filtered_df.head(50), use_container_width=True)
    st.markdown(f"Showing {len(filtered_df)} rows (filtered from {len(df)} total)")


def display_data_summary(df, selected_file_path):
    """Display data summary."""
    st.markdown("### 📋 Data Summary")
    
    # Basic statistics
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Basic Information")
        st.write(f"**Total Rows:** {len(df):,}")
        st.write(f"**Total Columns:** {len(df.columns):,}")
        st.write(f"**Memory Usage:** {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        st.write(f"**File Size:** {os.path.getsize(selected_file_path) / 1024 / 1024:.2f} MB")
    
    with col2:
        st.markdown("#### Data Quality")
        total_cells = len(df) * len(df.columns)
        null_cells = df.isnull().sum().sum()
        null_percentage = (null_cells / total_cells) * 100 if total_cells > 0 else 0
        
        st.write(f"**Total Cells:** {total_cells:,}")
        st.write(f"**Null Cells:** {null_cells:,}")
        st.write(f"**Null Percentage:** {null_percentage:.2f}%")
        st.write(f"**Duplicate Rows:** {df.duplicated().sum():,}")
    
    # Column information
    st.markdown("#### Column Information")
    column_info = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        null_count = df[col].isnull().sum()
        null_pct = (null_count / len(df)) * 100
        unique_count = df[col].nunique()
        
        column_info.append({
            'Column': col,
            'Data Type': dtype,
            'Null Count': null_count,
            'Null %': f"{null_pct:.2f}%",
            'Unique Values': unique_count
        })
    
    column_df = pd.DataFrame(column_info)
    st.dataframe(column_df, use_container_width=True)


def main():
    """Main function for the Parquet Explorer page."""
    st.markdown('<h1 class="main-header">📊 Parquet File Explorer</h1>', unsafe_allow_html=True)
    
    # Navigation
    if st.button("← Back to Home"):
        st.switch_page("streamlit_app.py")
    
    # Get available files
    parquet_files = get_parquet_files()
    
    if not parquet_files:
        st.warning("📊 No Parquet files found in the stage1 directory.")
        st.info("Process some collections first to generate Parquet files.")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Process Collections"):
                st.switch_page("streamlit_app.py")
        with col2:
            if st.button("📋 View Processing Logs"):
                st.switch_page("pages/02_Processing_Logs.py")
        return
    
    # File selection
    st.sidebar.markdown("## 🎛️ Controls")
    st.sidebar.markdown("### 📂 Select File")
    
    # Create a mapping of display names to file paths
    file_options = {}
    for file_path in parquet_files:
        filename = os.path.basename(file_path)
        if '_stage1_' in filename:
            collection_name = filename.split('_stage1_')[0]
            timestamp_part = filename.split('_stage1_')[1].replace('.parquet', '')
            try:
                dt = datetime.strptime(timestamp_part, "%Y%m%d_%H%M%S")
                timestamp = dt.strftime("%Y-%m-%d %H:%M")
            except:
                timestamp = timestamp_part
            display_name = f"{collection_name} ({timestamp})"
        else:
            display_name = filename.replace('.parquet', '')
        
        file_options[display_name] = file_path
    
    selected_file_display = st.sidebar.selectbox(
        "Choose a Parquet file:",
        list(file_options.keys()),
        index=0
    )
    
    selected_file_path = file_options[selected_file_display]
    
    # Load data
    df = load_parquet_data(selected_file_path)
    if df is None:
        return
    
    # Main content
    st.markdown(f"## 📁 {selected_file_display}")
    
    # File overview
    display_file_overview(df, selected_file_path)
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Data Preview", "📈 Analysis", "🔍 Search & Filter", "📋 Summary"])
    
    with tab1:
        display_data_preview(df)
    
    with tab2:
        display_column_analysis(df)
    
    with tab3:
        display_search_and_filter(df)
    
    with tab4:
        display_data_summary(df, selected_file_path)
    
    # Navigation to other pages
    st.markdown("---")
    st.markdown("### 📄 Navigation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Process More Collections"):
            st.switch_page("streamlit_app.py")
    
    with col2:
        if st.button("📋 View Processing Logs"):
            st.switch_page("pages/02_Processing_Logs.py")


if __name__ == "__main__":
    main()
