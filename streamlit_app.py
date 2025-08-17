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
    page_title="Kimball Stage1 - MongoDB to Parquet Pipeline",
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


def run_stage1_parser(selected_collections):
    """Run the Stage1 parser for selected collections."""
    try:
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
    
    # Get collections
    collections = get_mongodb_collections()
    
    if not collections:
        st.error("No collections found or unable to connect to MongoDB.")
        return
    
    st.info(f"📊 Found {len(collections)} collections in MongoDB")
    
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
    filtered_collections = collections
    
    if search_term:
        filtered_collections = [c for c in filtered_collections if search_term.lower() in c.lower()]
    
    # Get collection info for filtered collections
    with st.spinner("Analyzing collections..."):
        collection_info = []
        for collection in filtered_collections:
            info = get_collection_info(collection)
            collection_info.append(info)
    
    # Apply additional filters
    if filter_option == "Has Nested Data":
        collection_info = [c for c in collection_info if c.get('has_nested_data', False)]
    elif filter_option == "Simple Collections":
        collection_info = [c for c in collection_info if not c.get('has_nested_data', False)]
    elif filter_option == "Large Collections (>10k docs)":
        collection_info = [c for c in collection_info if c.get('document_count', 0) > 10000]
    elif filter_option == "Small Collections (<1k docs)":
        collection_info = [c for c in collection_info if c.get('document_count', 0) < 1000]
    
    st.write(f"📋 Showing {len(collection_info)} collections")
    
    # Selection controls
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("✅ Select All"):
            st.session_state.selected_collections = [c['name'] for c in collection_info]
            st.rerun()
    
    with col2:
        if st.button("❌ Clear All"):
            st.session_state.selected_collections = []
            st.rerun()
    
    with col3:
        if st.button("🔄 Refresh"):
            st.rerun()
    
    # Initialize selected collections in session state
    if 'selected_collections' not in st.session_state:
        st.session_state.selected_collections = []
    
    # Display collections
    st.markdown("### 📁 Available Collections")
    
    for info in collection_info:
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
        info = next((c for c in collection_info if c['name'] == collection_name), None)
        
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
                st.rerun()
    
    # Processing section
    if st.session_state.selected_collections:
        st.markdown("---")
        st.markdown(f"### 🚀 Processing {len(st.session_state.selected_collections)} Selected Collections")
        
        st.write("**Selected Collections:**")
        for i, collection in enumerate(st.session_state.selected_collections, 1):
            st.write(f"{i}. {collection}")
        
        # Store selected collections in session state for other pages
        st.session_state.collections_to_process = st.session_state.selected_collections
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔄 Parse Collections", type="primary"):
                with st.spinner("Processing collections..."):
                    success = run_stage1_parser(st.session_state.selected_collections)
                    if success:
                        st.session_state.processing_complete = True
                        st.success("✅ Collections processed successfully! Check the 'Processing Logs' page to monitor progress.")
                        st.rerun()
        
        with col2:
            if st.button("📊 View Existing Parquet Files"):
                st.switch_page("pages/03_Parquet_Explorer.py")
    
    # Navigation to other pages
    st.markdown("---")
    st.markdown("### 📄 Navigation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("📋 View Processing Logs"):
            st.switch_page("pages/02_Processing_Logs.py")
    
    with col2:
        if st.button("📊 Parquet File Explorer"):
            st.switch_page("pages/03_Parquet_Explorer.py")


if __name__ == "__main__":
    main()
