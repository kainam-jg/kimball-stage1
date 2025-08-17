import streamlit as st
import pandas as pd
import os
import subprocess
import sys
from datetime import datetime
from config import MONGODB_URI, DATABASES
from pymongo import MongoClient


# Page configuration
st.set_page_config(
    page_title="Find Collections",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
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
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .collection-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
        margin: 0.5rem 0;
    }
    .stButton > button {
        width: 100%;
        margin: 0.5rem 0;
    }
    .workflow-step {
        background-color: #e8f4fd;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
        border-left: 4px solid #1f77b4;
    }
</style>
""", unsafe_allow_html=True)


def get_mongodb_collections():
    """Get all collection names from MongoDB."""
    try:
        client = MongoClient(MONGODB_URI)
        db_name = DATABASES['production']
        db = client[db_name]
        collections = db.list_collection_names()
        client.close()
        return collections
    except Exception as e:
        st.error(f"Error connecting to MongoDB: {e}")
        return []


def get_collection_info(collection_name):
    """Get basic information about a collection."""
    try:
        client = MongoClient(MONGODB_URI)
        db_name = DATABASES['production']
        db = client[db_name]
        collection = db[collection_name]
        
        # Get document count
        doc_count = collection.count_documents({})
        
        # Get sample document for structure analysis
        sample_doc = collection.find_one()
        
        # Analyze structure
        if sample_doc:
            fields = list(sample_doc.keys())
            has_nested = any(isinstance(v, (dict, list)) for v in sample_doc.values())
        else:
            fields = []
            has_nested = False
        
        client.close()
        
        return {
            'name': collection_name,
            'document_count': doc_count,
            'field_count': len(fields),
            'has_nested_data': has_nested,
            'fields': fields[:10]  # First 10 fields
        }
    except Exception as e:
        return {
            'name': collection_name,
            'document_count': 0,
            'field_count': 0,
            'has_nested_data': False,
            'fields': [],
            'error': str(e)
        }


def clear_log_file():
    """Clear the log file."""
    try:
        from config import STAGE1_LOG_FILE
        if os.path.exists(STAGE1_LOG_FILE):
            with open(STAGE1_LOG_FILE, 'w') as f:
                f.write("")  # Clear the file
    except Exception as e:
        st.error(f"Error clearing log file: {e}")


def run_stage1_parser(selected_collections):
    """Run the Stage1 parser for selected collections."""
    try:
        # Clear the log file before starting
        clear_log_file()
        
        # Create a temporary script to run the parser
        collections_str = ",".join(selected_collections)
        
        # Run the parser as a subprocess
        result = subprocess.run([
            sys.executable, "stage1_parser.py"
        ], input=f"2\n{collections_str}\n", text=True, capture_output=True)
        
        if result.returncode == 0:
            st.success("✅ Stage1 processing completed successfully!")
            return True
        else:
            st.error(f"❌ Stage1 processing failed: {result.stderr}")
            return False
            
    except Exception as e:
        st.error(f"Error running Stage1 parser: {e}")
        return False


def main():
    """Main function for the home page."""
            # Check if we should navigate to logs page
    if 'should_navigate_to_logs' in st.session_state and st.session_state.should_navigate_to_logs:
        st.session_state.should_navigate_to_logs = False
        st.markdown("### 🔄 Processing Started Successfully!")
        st.success("✅ Your collections are now being processed in the background.")
        
        st.markdown("### 📋 Next Steps:")
        st.markdown("1. **Navigate to Processing Logs** - Use the sidebar to go to the Processing Logs page")
        st.markdown("2. **Monitor Progress** - Watch real-time updates as your collections are processed")
        st.markdown("3. **Explore Results** - Once complete, visit the Parquet Explorer to view your data")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**📋 Processing Logs Page**")
            st.markdown("Monitor real-time processing progress")
        with col2:
            st.markdown("**📊 Parquet Explorer Page**")
            st.markdown("View and analyze processed data")
        
        st.info("💡 **Tip:** Use the sidebar menu to navigate between pages")
        st.stop()
    
    st.markdown('<h1 class="main-header">🗄️ Kimball Stage1 Pipeline</h1>', unsafe_allow_html=True)
    
    # Workflow overview
    st.markdown("""
    <div class="workflow-step">
        <h3>📋 Workflow Steps:</h3>
        <ol>
            <li><strong>Step 1 (Current):</strong> Select MongoDB collections to process</li>
            <li><strong>Step 2:</strong> Monitor processing logs</li>
            <li><strong>Step 3:</strong> Explore generated Parquet files</li>
        </ol>
    </div>
    """, unsafe_allow_html=True)
    
    # Clear log file on app refresh
    if 'app_initialized' not in st.session_state:
        clear_log_file()
        st.session_state.app_initialized = True
    
    # Initialize session state
    if 'collections_loaded' not in st.session_state:
        st.session_state.collections_loaded = False
    if 'collections' not in st.session_state:
        st.session_state.collections = []
    if 'collection_info' not in st.session_state:
        st.session_state.collection_info = []
    if 'selected_collections' not in st.session_state:
        st.session_state.selected_collections = []
    
    # Collection discovery section
    if not st.session_state.collections_loaded:
        st.markdown("### 🔍 Discover MongoDB Collections")
        st.info("Click the button below to discover and analyze collections in your MongoDB database.")
        
        if st.button("🔍 Discover Collections", type="primary"):
            with st.spinner("Connecting to MongoDB and analyzing collections..."):
                collections = get_mongodb_collections()
                
                if not collections:
                    st.error("No collections found or unable to connect to MongoDB.")
                    return
                
                st.session_state.collections = collections
                
                # Get collection info
                collection_info = []
                for collection in collections:
                    info = get_collection_info(collection)
                    collection_info.append(info)
                
                st.session_state.collection_info = collection_info
                st.session_state.collections_loaded = True
                st.success(f"✅ Discovered {len(collections)} collections!")
                st.experimental_rerun()
    else:
        # Collections are loaded, show the interface
        st.success(f"📊 {len(st.session_state.collections)} collections loaded")
        
        # Add a button to refresh collections
        if st.button("🔄 Refresh Collections"):
            st.session_state.collections_loaded = False
            st.session_state.collections = []
            st.session_state.collection_info = []
            st.session_state.selected_collections = []
            st.experimental_rerun()
    
        # Search and filter
        col1, col2 = st.columns(2)
        
        with col1:
            search_term = st.text_input("🔍 Search collections:", "")
        
        with col2:
            filter_option = st.selectbox(
                "📋 Filter by:",
                ["All", "Has Nested Data", "Simple Collections", "Large Collections (>10k docs)", "Small Collections (<1k docs)"]
            )
        
        # Filter collections based on search and filter
        filtered_collection_info = st.session_state.collection_info.copy()
        
        if search_term:
            filtered_collection_info = [c for c in filtered_collection_info if search_term.lower() in c['name'].lower()]
        
        # Apply additional filters
        if filter_option == "Has Nested Data":
            filtered_collection_info = [c for c in filtered_collection_info if c.get('has_nested_data', False)]
        elif filter_option == "Simple Collections":
            filtered_collection_info = [c for c in filtered_collection_info if not c.get('has_nested_data', False)]
        elif filter_option == "Large Collections (>10k docs)":
            filtered_collection_info = [c for c in filtered_collection_info if c.get('document_count', 0) > 10000]
        elif filter_option == "Small Collections (<1k docs)":
            filtered_collection_info = [c for c in filtered_collection_info if c.get('document_count', 0) < 1000]
        
        st.write(f"📋 Showing {len(filtered_collection_info)} collections")
    
        # Selection controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("✅ Select All"):
                st.session_state.selected_collections = [c['name'] for c in filtered_collection_info]
                st.experimental_rerun()
        
        with col2:
            if st.button("❌ Clear All"):
                st.session_state.selected_collections = []
                st.experimental_rerun()
        
        with col3:
            if st.button("🔄 Process", type="primary"):
                if st.session_state.selected_collections:
                    with st.spinner("Processing collections..."):
                        success = run_stage1_parser(st.session_state.selected_collections)
                        if success:
                            st.session_state.processing_complete = True
                            st.session_state.should_navigate_to_logs = True
                            st.success("✅ Collections processed successfully!")
                            st.info("🔄 Automatically navigating to Processing Logs page...")
                            st.experimental_rerun()
                else:
                    st.warning("Please select at least one collection to process.")
        
        # Display collections
        st.markdown("### 📁 Available Collections")
        
        for info in filtered_collection_info:
            collection_name = info['name']
            
            # Create a card for each collection
            with st.container():
                col1, col2, col3, col4, col5 = st.columns([1, 2, 1, 1, 1])
                
                with col1:
                    is_selected = collection_name in st.session_state.selected_collections
                    if st.checkbox("", value=is_selected, key=f"check_{collection_name}"):
                        if collection_name not in st.session_state.selected_collections:
                            st.session_state.selected_collections.append(collection_name)
                    else:
                        if collection_name in st.session_state.selected_collections:
                            st.session_state.selected_collections.remove(collection_name)
                
                with col2:
                    st.markdown(f"**{collection_name}**")
                    if info.get('error'):
                        st.error(f"Error: {info['error']}")
                    else:
                        st.caption(f"{info['field_count']} fields")
                
                with col3:
                    st.write(f"{info['document_count']:,} docs")
                
                with col4:
                    if info.get('has_nested_data'):
                        st.markdown("🔗 **Nested**")
                    else:
                        st.markdown("📄 **Simple**")
                
                with col5:
                    if st.button("ℹ️", key=f"info_{collection_name}"):
                        st.session_state.show_info = collection_name
    
        # Show collection details if requested
        if 'show_info' in st.session_state and st.session_state.show_info:
            collection_name = st.session_state.show_info
            info = next((c for c in st.session_state.collection_info if c['name'] == collection_name), None)
        
        if info:
            st.markdown(f"### 📋 Details: {collection_name}")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Documents", f"{info['document_count']:,}")
            
            with col2:
                st.metric("Fields", info['field_count'])
            
            with col3:
                st.metric("Has Nested Data", "Yes" if info.get('has_nested_data') else "No")
            
            if info.get('fields'):
                st.write("**Sample Fields:**")
                st.write(", ".join(info['fields']))
            
            if st.button("Close Details"):
                del st.session_state.show_info
                st.experimental_rerun()
    
        # Show selected collections info
        if st.session_state.selected_collections:
            st.markdown("---")
            st.markdown(f"### 📋 Selected Collections ({len(st.session_state.selected_collections)})")
            
            # Store selected collections in session state for other pages
            st.session_state.collections_to_process = st.session_state.selected_collections
            
            for i, collection in enumerate(st.session_state.selected_collections, 1):
                st.write(f"{i}. {collection}")
            
            st.info("Click the '🔄 Process' button above to start processing these collections.")
        
        # Navigation to other pages
        st.markdown("---")
        st.markdown("### 📄 Navigation")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📋 View Processing Logs"):
                st.markdown("Navigate to the Processing Logs page using the sidebar menu.")
        
        with col2:
            if st.button("📊 Parquet File Explorer"):
                st.markdown("Navigate to the Parquet Explorer page using the sidebar menu.")


if __name__ == "__main__":
    main()
