import streamlit as st
import pandas as pd
import duckdb
import os
import glob
from pathlib import Path
from config import get_stage1_path


# Page configuration
st.set_page_config(
    page_title="DuckDB Query - Kimball Stage1",
    page_icon="🦆",
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
    .query-container {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .join-builder {
        background-color: #e8f4fd;
        border: 1px solid #1f77b4;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .stButton > button {
        width: 100%;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)


def get_parquet_files():
    """Get all Parquet files from the stage1 directory."""
    try:
        stage1_path = get_stage1_path()
        parquet_files = glob.glob(os.path.join(stage1_path, "*.parquet"))
        
        # Debug info
        st.sidebar.info(f"Stage1 path: {stage1_path}")
        st.sidebar.info(f"Found {len(parquet_files)} Parquet files")
        
        # Create mapping of simple names to full filenames
        file_mapping = {}
        for full_path in parquet_files:
            filename = os.path.basename(full_path)
            # Extract simple name (remove .parquet extension)
            simple_name = filename.replace('.parquet', '')
            file_mapping[simple_name] = filename
        
        return file_mapping
    except Exception as e:
        st.error(f"Error getting Parquet files: {e}")
        return {}


def get_file_schema(full_filename):
    """Get the schema of a Parquet file."""
    try:
        stage1_path = get_stage1_path()
        full_path = os.path.join(stage1_path, full_filename)
        
        # Convert to absolute path and normalize
        full_path = os.path.abspath(full_path)
        
        # Convert Windows backslashes to forward slashes for DuckDB
        full_path = full_path.replace('\\', '/')
        
        # Use DuckDB to get schema
        con = duckdb.connect(':memory:')
        con.execute(f"CREATE VIEW file_view AS SELECT * FROM read_parquet('{full_path}')")
        schema = con.execute("DESCRIBE file_view").fetchdf()
        con.close()
        
        return schema
    except Exception as e:
        st.error(f"Error getting schema for {full_filename}: {e}")
        return pd.DataFrame()


def execute_query(query, selected_simple_names, file_mapping):
    """Execute a DuckDB query on selected Parquet files."""
    try:
        stage1_path = get_stage1_path()
        con = duckdb.connect(':memory:')
        
        # Register all selected files as views using simple names
        for simple_name in selected_simple_names:
            if simple_name not in file_mapping:
                st.error(f"File mapping not found for: {simple_name}")
                return pd.DataFrame()
            
            full_filename = file_mapping[simple_name]
            full_path = os.path.join(stage1_path, full_filename)
            
            # Convert to absolute path and normalize
            full_path = os.path.abspath(full_path)
            
            # Convert Windows backslashes to forward slashes for DuckDB
            full_path = full_path.replace('\\', '/')
            
            # Check if file exists
            if not os.path.exists(full_path):
                st.error(f"File not found: {full_path}")
                return pd.DataFrame()
            
            # Register with simple name as view
            con.execute(f"CREATE VIEW {simple_name} AS SELECT * FROM read_parquet('{full_path}')")
        
        # Execute the query
        result = con.execute(query).fetchdf()
        con.close()
        
        return result
    except Exception as e:
        st.error(f"Query execution error: {e}")
        st.error(f"Selected files: {selected_simple_names}")
        st.error(f"Stage1 path: {get_stage1_path()}")
        return pd.DataFrame()


def build_join_query(joins, selected_simple_names):
    """Build a SQL query from the visual join builder."""
    if not joins:
        return f"SELECT * FROM {selected_simple_names[0]}"
    
    # Start with the first file
    query = f"SELECT * FROM {selected_simple_names[0]}"
    
    # Add joins
    for i, join in enumerate(joins):
        left_file = join['left_file']
        right_file = join['right_file']
        left_col = join['left_column']
        right_col = join['right_column']
        join_type = join['join_type']
        
        query += f"\n{join_type} JOIN {right_file} ON {left_file}.{left_col} = {right_file}.{right_col}"
    
    return query


def main():
    """Main function for the DuckDB Query page."""
    st.markdown('<h1 class="main-header">🦆 DuckDB Query Explorer</h1>', unsafe_allow_html=True)
    
    # Navigation
    if st.button("← Back to Home"):
        st.markdown("Navigate to the Home page using the sidebar menu.")
    
    # Get available Parquet files
    file_mapping = get_parquet_files()
    
    if not file_mapping:
        st.warning("🦆 No Parquet files found in the stage1 directory.")
        st.info("Go back to the Find Collections page and process some collections first.")
        return
    
    # File selection
    st.markdown("### 📁 Select Parquet Files")
    simple_names = list(file_mapping.keys())
    selected_simple_names = st.multiselect(
        "Choose files to query:",
        simple_names,
        help="Select one or more Parquet files to include in your query"
    )
    
    if not selected_simple_names:
        st.info("Please select at least one Parquet file to continue.")
        return
    
    # Display file information
    st.markdown("### 📋 Selected Files")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Files Selected", len(selected_simple_names))
    
    with col2:
        total_size = sum(os.path.getsize(os.path.join(get_stage1_path(), file_mapping[name])) for name in selected_simple_names)
        st.metric("Total Size", f"{total_size / (1024*1024):.1f} MB")
    
    with col3:
        st.metric("Status", "Ready to Query")
    
    # Show file mapping info
    st.info(f"💡 **Query Tip:** You can now use simple names like 'carts' instead of full filenames in your SQL queries!")
    
    # File schemas
    st.markdown("### 🗂️ File Schemas")
    
    for simple_name in selected_simple_names:
        full_filename = file_mapping[simple_name]
        with st.expander(f"📄 {simple_name} ({full_filename})"):
            schema = get_file_schema(full_filename)
            if not schema.empty:
                st.dataframe(schema, use_container_width=True)
            else:
                st.error(f"Could not read schema for {simple_name}")
    
    # Visual Join Builder
    if len(selected_simple_names) > 1:
        st.markdown("### 🔗 Visual Join Builder")
        st.info("Build joins between your selected files visually, or write SQL directly below.")
        
        # Initialize joins in session state
        if 'joins' not in st.session_state:
            st.session_state.joins = []
        
        # Add new join
        with st.container():
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                left_file = st.selectbox("Left File", selected_simple_names, key="left_file")
            
            with col2:
                left_schema = get_file_schema(file_mapping[left_file])
                left_columns = left_schema['column_name'].tolist() if not left_schema.empty else []
                left_col = st.selectbox("Left Column", left_columns, key="left_col")
            
            with col3:
                join_type = st.selectbox("Join Type", ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"], key="join_type")
            
            with col4:
                right_file = st.selectbox("Right File", [f for f in selected_simple_names if f != left_file], key="right_file")
            
            with col5:
                right_schema = get_file_schema(file_mapping[right_file])
                right_columns = right_schema['column_name'].tolist() if not right_schema.empty else []
                right_col = st.selectbox("Right Column", right_columns, key="right_col")
            
            if st.button("➕ Add Join"):
                new_join = {
                    'left_file': left_file,
                    'left_column': left_col,
                    'join_type': join_type,
                    'right_file': right_file,
                    'right_column': right_col
                }
                st.session_state.joins.append(new_join)
                st.success(f"Added join: {left_file}.{left_col} {join_type} {right_file}.{right_col}")
                st.experimental_rerun()
        
        # Display existing joins
        if st.session_state.joins:
            st.markdown("**Current Joins:**")
            for i, join in enumerate(st.session_state.joins):
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.write(f"{i+1}. {join['left_file']}.{join['left_column']} {join['join_type']} {join['right_file']}.{join['right_column']}")
                with col2:
                    if st.button("❌", key=f"remove_join_{i}"):
                        st.session_state.joins.pop(i)
                        st.experimental_rerun()
            
            if st.button("🔄 Generate SQL from Joins"):
                generated_query = build_join_query(st.session_state.joins, selected_simple_names)
                st.session_state.generated_sql = generated_query
                st.success("SQL generated from joins!")
    
    # SQL Query Editor
    st.markdown("### 🔍 SQL Query Editor")
    
    # Initialize SQL in session state
    if 'sql_query' not in st.session_state:
        st.session_state.sql_query = ""
    if 'generated_sql' not in st.session_state:
        st.session_state.generated_sql = ""
    
    # Use generated SQL if available
    if st.session_state.generated_sql:
        st.session_state.sql_query = st.session_state.generated_sql
        st.session_state.generated_sql = ""  # Clear after using
    
    # SQL editor
    sql_query = st.text_area(
        "Enter your SQL query:",
        value=st.session_state.sql_query,
        height=200,
        help=f"Write SQL to query your selected Parquet files. Use simple names like: {', '.join(selected_simple_names)}"
    )
    
    # Query controls
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🚀 Execute Query", type="primary"):
            if sql_query.strip():
                st.session_state.sql_query = sql_query
                with st.spinner("Executing query..."):
                    result = execute_query(sql_query, selected_simple_names, file_mapping)
                    st.session_state.query_result = result
                    st.success(f"Query executed successfully! Returned {len(result)} rows.")
            else:
                st.warning("Please enter a SQL query.")
    
    with col2:
        if st.button("💾 Save Query"):
            if sql_query.strip():
                st.session_state.sql_query = sql_query
                st.success("Query saved!")
            else:
                st.warning("No query to save.")
    
    with col3:
        if st.button("🗑️ Clear Query"):
            st.session_state.sql_query = ""
            st.session_state.query_result = None
            st.experimental_rerun()
    
    # Display results
    if 'query_result' in st.session_state and st.session_state.query_result is not None:
        st.markdown("### 📊 Query Results")
        
        result = st.session_state.query_result
        
        # Results info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Rows", len(result))
        with col2:
            st.metric("Columns", len(result.columns))
        with col3:
            st.metric("Memory", f"{result.memory_usage(deep=True).sum() / 1024:.1f} KB")
        
        # Results table
        st.dataframe(result, use_container_width=True)
        
        # Download options
        col1, col2 = st.columns(2)
        with col1:
            csv = result.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name="query_results.csv",
                mime="text/csv"
            )
        
        with col2:
            # Convert to Parquet for download
            try:
                parquet_data = result.to_parquet(index=False)
                st.download_button(
                    label="📥 Download as Parquet",
                    data=parquet_data,
                    file_name="query_results.parquet",
                    mime="application/octet-stream"
                )
            except Exception as e:
                st.error(f"Could not create Parquet download: {e}")
    
    # Sample queries
    st.markdown("### 💡 Sample Queries")
    
    with st.expander("Click to see sample queries"):
        st.markdown("""
        **Basic queries you can try:**
        
        ```sql
        -- View all data from a file (replace 'carts' with your selected file name)
        SELECT * FROM carts LIMIT 10;
        
        -- Count records in each file
        SELECT 'carts' as file_name, COUNT(*) as record_count FROM carts
        UNION ALL
        SELECT 'profiles' as file_name, COUNT(*) as record_count FROM profiles;
        
        -- Join two files (if you have common columns)
        SELECT c.*, p.* 
        FROM carts c 
        INNER JOIN profiles p ON c.user_id = p.id;
        
        -- Aggregation example
        SELECT column_name, COUNT(*) as count
        FROM carts 
        GROUP BY column_name;
        ```
        """)


if __name__ == "__main__":
    main()
