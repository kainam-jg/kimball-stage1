import streamlit as st
import pandas as pd
import duckdb
import os
import glob
from pathlib import Path
from config import get_stage1_path
from auth_utils import check_authentication, show_user_info


# Page configuration
st.set_page_config(
    page_title="DuckDB Query - Kimball Stage1",
    page_icon="🦆",
    layout="wide"
)

# Check authentication
check_authentication()

# Show user info in sidebar
show_user_info()

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





def execute_query(query):
    """Execute a DuckDB query on all available Parquet files."""
    try:
        stage1_path = get_stage1_path()
        con = duckdb.connect(':memory:')
        
        # Get all Parquet files and register them as views
        parquet_files = glob.glob(os.path.join(stage1_path, "*.parquet"))
        
        if not parquet_files:
            st.error("No Parquet files found in the stage1 directory.")
            return pd.DataFrame()
        
        # Register all files as views
        registered_tables = []
        for full_path in parquet_files:
            filename = os.path.basename(full_path)
            simple_name = filename.replace('.parquet', '')
            
            # Convert to absolute path and normalize
            full_path = os.path.abspath(full_path)
            full_path = full_path.replace('\\', '/')
            
            # Clean table name (remove any special characters that might cause issues)
            clean_table_name = simple_name.replace('-', '_').replace(' ', '_')
            
            # Register with clean table name as view
            try:
                con.execute(f"CREATE VIEW {clean_table_name} AS SELECT * FROM read_parquet('{full_path}')")
                registered_tables.append(clean_table_name)
            except Exception as view_error:
                st.error(f"Error creating view for {clean_table_name}: {view_error}")
                continue
        
        # Show available tables in sidebar
        if registered_tables:
            st.sidebar.success(f"Registered {len(registered_tables)} tables")
            st.sidebar.info(f"Available tables: {', '.join(registered_tables)}")
        
        # Execute the query
        result = con.execute(query).fetchdf()
        con.close()
        
        return result
    except Exception as e:
        st.error(f"Query execution error: {e}")
        
        # Show available tables for debugging
        try:
            stage1_path = get_stage1_path()
            parquet_files = glob.glob(os.path.join(stage1_path, "*.parquet"))
            if parquet_files:
                st.error(f"Available Parquet files:")
                for file in parquet_files:
                    simple_name = os.path.basename(file).replace('.parquet', '')
                    clean_name = simple_name.replace('-', '_').replace(' ', '_')
                    st.error(f"  - {simple_name} (use '{clean_name}' in queries)")
        except Exception as debug_e:
            st.error(f"Debug error: {debug_e}")
        
        return pd.DataFrame()





def main():
    """Main function for the DuckDB Query page."""
    st.markdown('<h1 class="main-header">🦆 DuckDB Query Explorer</h1>', unsafe_allow_html=True)
    
    # Navigation
    back_button = st.button(
        label="← Back to Home",
        help="Navigate to the home page"
    )
    if back_button:
        st.markdown("Navigate to the Home page using the sidebar menu.")
    
    # Check if Parquet files exist
    stage1_path = get_stage1_path()
    parquet_files = glob.glob(os.path.join(stage1_path, "*.parquet"))
    
    if not parquet_files:
        st.warning("🦆 No Parquet files found in the stage1 directory.")
        st.info("Go back to the Find Collections page and process some collections first.")
        return
    
    # Show available tables info
    st.markdown("### 📋 Available Tables")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Files", len(parquet_files))
    
    with col2:
        total_size = sum(os.path.getsize(file) for file in parquet_files)
        st.metric("Total Size", f"{total_size / (1024*1024):.1f} MB")
    
    with col3:
        st.metric("Status", "Ready to Query")
    
    # Show available table names
    table_names = []
    for file in parquet_files:
        simple_name = os.path.basename(file).replace('.parquet', '')
        clean_name = simple_name.replace('-', '_').replace(' ', '_')
        table_names.append(clean_name)
    
    st.info(f"💡 **Available Tables:** {', '.join(table_names)}")
    
    # SQL Query Editor
    st.markdown("### 🔍 SQL Query Editor")
    
    # Initialize SQL in session state
    if 'sql_query' not in st.session_state:
        st.session_state.sql_query = ""
    
    # Set default query if none exists
    if not st.session_state.sql_query.strip() and table_names:
        default_table = table_names[0]
        st.session_state.sql_query = f"SELECT * FROM {default_table} LIMIT 10;"
    
    # SQL editor
    sql_query = st.text_area(
        label="Enter your SQL query:",
        value=st.session_state.sql_query,
        height=200,
        help=f"Write SQL to query your Parquet files. Available tables: {', '.join(table_names)}",
        placeholder="SELECT * FROM table_name LIMIT 10;"
    )
    
    # Query controls
    col1, col2, col3 = st.columns(3)
    
    with col1:
        execute_button = st.button(
            label="🚀 Execute Query", 
            type="primary",
            help="Execute the SQL query and display results"
        )
        if execute_button:
            if sql_query.strip():
                st.session_state.sql_query = sql_query
                with st.spinner("Executing query..."):
                    result = execute_query(sql_query)
                    st.session_state.query_result = result
                    if not result.empty:
                        st.success(f"Query executed successfully! Returned {len(result)} rows.")
            else:
                st.warning("Please enter a SQL query.")
    
    with col2:
        save_button = st.button(
            label="💾 Save Query",
            help="Save the current query to session state"
        )
        if save_button:
            if sql_query.strip():
                st.session_state.sql_query = sql_query
                st.success("Query saved!")
            else:
                st.warning("No query to save.")
    
    with col3:
        clear_button = st.button(
            label="🗑️ Clear Query",
            help="Clear the current query and results"
        )
        if clear_button:
            st.session_state.sql_query = ""
            st.session_state.query_result = None
            st.rerun()
    
    # Display results
    if 'query_result' in st.session_state and st.session_state.query_result is not None:
        st.markdown("### 📊 Query Results")
        
        result = st.session_state.query_result
        
        if not result.empty:
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
        else:
            st.info("Query returned no results.")
    
    # Sample queries
    st.markdown("### 💡 Sample Queries")
    
    with st.expander("Click to see sample queries"):
        if len(table_names) == 1:
            table_name = table_names[0]
            sample_queries = f"""
            **Basic queries you can try:**
            
            ```sql
            -- View all data from {table_name}
            SELECT * FROM {table_name} LIMIT 10;
            
            -- Count total records
            SELECT COUNT(*) as total_records FROM {table_name};
            
            -- View column names
            DESCRIBE {table_name};
            
            -- Sample aggregation
            SELECT column_name, COUNT(*) as count
            FROM {table_name} 
            GROUP BY column_name
            LIMIT 10;
            ```
            """
        else:
            # Multiple tables available
            union_queries = []
            for table in table_names:
                union_queries.append(f"SELECT '{table}' as file_name, COUNT(*) as record_count FROM {table}")
            
            union_sql = '\nUNION ALL\n'.join(union_queries)
            
            sample_queries = f"""
            **Basic queries you can try:**
            
            ```sql
            -- View data from first table
            SELECT * FROM {table_names[0]} LIMIT 10;
            
            -- Count records in each table
            {union_sql};
            
            -- Join tables (if they have common columns)
            SELECT a.*, b.* 
            FROM {table_names[0]} a 
            INNER JOIN {table_names[1]} b ON a.id = b.id;
            
            -- Sample aggregation
            SELECT column_name, COUNT(*) as count
            FROM {table_names[0]} 
            GROUP BY column_name
            LIMIT 10;
            ```
            """
        
        st.markdown(sample_queries)
        st.markdown("**Note**: Table names are automatically cleaned (spaces and hyphens replaced with underscores) for SQL compatibility.")


if __name__ == "__main__":
    main()
