#!/usr/bin/env python3
"""
ERD Generator Page for Streamlit
Generates Entity Relationship Diagrams from Parquet files with persistent metadata
"""

import streamlit as st
import duckdb
import pandas as pd
import os
import re
import json
from typing import Dict, List, Tuple, Set
from pathlib import Path
import glob
import time

def get_stage1_path():
    """Get the Stage1 directory path."""
    return "parquet_exports/stage1"

class ERDGenerator:
    """Generates Entity Relationship Diagrams from Parquet files."""
    
    def __init__(self, parquet_dir: str = "parquet_exports/stage1"):
        self.parquet_dir = parquet_dir
        self.tables = {}
        self.relationships = []
        self.connection = None
        
    def connect_db(self):
        """Create DuckDB connection."""
        self.connection = duckdb.connect(':memory:')
        
    def close_db(self):
        """Close DuckDB connection."""
        if self.connection:
            self.connection.close()
            
    def get_parquet_files(self) -> List[str]:
        """Get all Parquet files in the directory."""
        pattern = os.path.join(self.parquet_dir, "*.parquet")
        return glob.glob(pattern)
    
    def analyze_table_structure(self, file_path: str) -> Dict:
        """Analyze the structure of a single table."""
        table_name = os.path.basename(file_path).replace('.parquet', '')
        
        # Create view for the table
        self.connection.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}')")
        
        # Get schema
        schema = self.connection.execute(f"DESCRIBE {table_name}").fetchdf()
        
        # Get sample data for analysis
        sample_data = self.connection.execute(f"SELECT * FROM {table_name} LIMIT 100").fetchdf()
        
        # Analyze columns
        columns = []
        for _, row in schema.iterrows():
            col_name = row['column_name']
            col_type = row['column_type']
            
            # Analyze column characteristics
            column_info = {
                'name': col_name,
                'type': col_type,
                'is_primary_key': col_name == '_id',
                'is_foreign_key': self._is_foreign_key_candidate(col_name, sample_data[col_name] if col_name in sample_data.columns else None),
                'is_reference_field': self._is_reference_field(col_name),
                'unique_values': len(sample_data[col_name].dropna().unique()) if col_name in sample_data.columns else 0,
                'null_count': sample_data[col_name].isna().sum() if col_name in sample_data.columns else 0
            }
            columns.append(column_info)
        
        return {
            'name': table_name,
            'file_path': file_path,
            'columns': columns,
            'row_count': len(sample_data),
            'schema': schema.to_dict('records')
        }
    
    def _is_foreign_key_candidate(self, column_name: str, column_data) -> bool:
        """Check if a column might be a foreign key."""
        if column_data is None:
            return False
            
        # Check for ObjectId pattern (24 character hex strings)
        if column_name != '_id':  # Don't treat _id as foreign key
            sample_values = column_data.dropna().astype(str)
            if len(sample_values) > 0:
                # Check if values look like ObjectIds
                objectid_pattern = re.compile(r'^[0-9a-f]{24}$', re.IGNORECASE)
                objectid_count = sum(1 for val in sample_values if objectid_pattern.match(str(val)))
                if objectid_count > 0 and objectid_count / len(sample_values) > 0.5:
                    return True
                    
        return False
    
    def _is_reference_field(self, column_name: str) -> bool:
        """Check if a column name suggests it's a reference field."""
        reference_patterns = [
            r'^.*_id$',  # Ends with _id
            r'^.*Id$',   # Ends with Id
            r'^.*Ref$',  # Ends with Ref
            r'^.*Reference$',  # Contains Reference
        ]
        
        for pattern in reference_patterns:
            if re.match(pattern, column_name):
                return True
        return False
    
    def detect_relationships(self) -> List[Dict]:
        """Detect relationships between tables."""
        relationships = []
        
        # Get all table names
        table_names = list(self.tables.keys())
        
        for i, table1 in enumerate(table_names):
            for table2 in table_names[i+1:]:
                # Check for direct foreign key relationships
                fk_relationships = self._find_foreign_key_relationships(table1, table2)
                relationships.extend(fk_relationships)
                
                # Check for common field relationships
                common_relationships = self._find_common_field_relationships(table1, table2)
                relationships.extend(common_relationships)
        
        return relationships
    
    def _find_foreign_key_relationships(self, table1: str, table2: str) -> List[Dict]:
        """Find foreign key relationships between two tables."""
        relationships = []
        
        table1_info = self.tables[table1]
        table2_info = self.tables[table2]
        
        # Get primary key of table2 (usually _id)
        table2_pk = None
        for col in table2_info['columns']:
            if col['is_primary_key']:
                table2_pk = col['name']
                break
        
        if not table2_pk:
            return relationships
        
        # Check if table1 has foreign keys pointing to table2
        for col in table1_info['columns']:
            if col['is_foreign_key'] or col['is_reference_field']:
                # Check if this column's values exist in table2's primary key
                try:
                    # Sample some values to check
                    sample_values = self.connection.execute(f"""
                        SELECT DISTINCT {col['name']} 
                        FROM {table1} 
                        WHERE {col['name']} IS NOT NULL 
                        LIMIT 10
                    """).fetchdf()
                    
                    if len(sample_values) > 0:
                        # Check if any of these values exist in table2
                        for _, row in sample_values.iterrows():
                            value = row[col['name']]
                            if value:
                                exists = self.connection.execute(f"""
                                    SELECT COUNT(*) as count 
                                    FROM {table2} 
                                    WHERE {table2_pk} = '{value}'
                                """).fetchdf()
                                
                                if exists.iloc[0]['count'] > 0:
                                    relationships.append({
                                        'from_table': table1,
                                        'from_column': col['name'],
                                        'to_table': table2,
                                        'to_column': table2_pk,
                                        'relationship_type': 'foreign_key',
                                        'confidence': 'high'
                                    })
                                    break
                except Exception as e:
                    # Skip if there's an error
                    continue
        
        return relationships
    
    def _find_common_field_relationships(self, table1: str, table2: str) -> List[Dict]:
        """Find relationships based on common field names and values."""
        relationships = []
        
        table1_info = self.tables[table1]
        table2_info = self.tables[table2]
        
        # Find columns with similar names
        table1_cols = {col['name'].lower(): col['name'] for col in table1_info['columns']}
        table2_cols = {col['name'].lower(): col['name'] for col in table2_info['columns']}
        
        common_cols = set(table1_cols.keys()) & set(table2_cols.keys())
        
        for col_lower in common_cols:
            col1 = table1_cols[col_lower]
            col2 = table2_cols[col_lower]
            
            # Skip if it's the same table or if it's _id
            if table1 == table2 or col_lower == '_id':
                continue
            
            # Check if values overlap
            try:
                overlap = self.connection.execute(f"""
                    SELECT COUNT(DISTINCT t1.{col1}) as overlap_count
                    FROM {table1} t1
                    INNER JOIN {table2} t2 ON t1.{col1} = t2.{col2}
                    WHERE t1.{col1} IS NOT NULL AND t2.{col2} IS NOT NULL
                """).fetchdf()
                
                if overlap.iloc[0]['overlap_count'] > 0:
                    relationships.append({
                        'from_table': table1,
                        'from_column': col1,
                        'to_table': table2,
                        'to_column': col2,
                        'relationship_type': 'common_field',
                        'confidence': 'medium'
                    })
            except Exception as e:
                # Skip if there's an error
                continue
        
        return relationships
    
    def generate_mermaid_erd(self) -> str:
        """Generate Mermaid ERD diagram."""
        mermaid = "erDiagram\n"
        
        # Add entities
        for table_name, table_info in self.tables.items():
            mermaid += f"    {table_name} {{\n"
            
            # Add primary key first
            for col in table_info['columns']:
                if col['is_primary_key']:
                    mermaid += f"        {col['type']} {col['name']} PK\n"
                    break
            
            # Add other columns
            for col in table_info['columns']:
                if not col['is_primary_key']:
                    fk_indicator = " FK" if col['is_foreign_key'] else ""
                    mermaid += f"        {col['type']} {col['name']}{fk_indicator}\n"
            
            mermaid += "    }\n\n"
        
        # Add relationships
        for rel in self.relationships:
            mermaid += f"    {rel['from_table']} ||--o{{ {rel['to_table']} : \"{rel['from_column']} -> {rel['to_column']}\"\n"
        
        return mermaid
    
    def generate_json_metadata(self) -> Dict:
        """Generate JSON metadata about the ERD."""
        return {
            'tables': self.tables,
            'relationships': self.relationships,
            'summary': {
                'total_tables': len(self.tables),
                'total_relationships': len(self.relationships),
                'foreign_key_relationships': len([r for r in self.relationships if r['relationship_type'] == 'foreign_key']),
                'common_field_relationships': len([r for r in self.relationships if r['relationship_type'] == 'common_field'])
            },
            'generated_at': time.strftime('%Y-%m-%d %H:%M:%S')
        }
    
    def analyze_all_tables(self, progress_bar, status_text):
        """Analyze all Parquet files and detect relationships."""
        parquet_files = self.get_parquet_files()
        total_files = len(parquet_files)
        
        if total_files == 0:
            st.error("No Parquet files found in the stage1 directory.")
            return False
        
        status_text.text(f"Found {total_files} Parquet files")
        
        for i, file_path in enumerate(parquet_files):
            table_name = os.path.basename(file_path).replace('.parquet', '')
            status_text.text(f"Analyzing {table_name}... ({i+1}/{total_files})")
            
            try:
                table_info = self.analyze_table_structure(file_path)
                self.tables[table_name] = table_info
            except Exception as e:
                st.warning(f"Error analyzing {table_name}: {e}")
            
            # Update progress bar
            progress = (i + 1) / total_files
            progress_bar.progress(progress)
        
        status_text.text("Detecting relationships...")
        self.relationships = self.detect_relationships()
        
        status_text.text(f"Analysis complete! Found {len(self.tables)} tables and {len(self.relationships)} relationships")
        return True

def load_erd_metadata():
    """Load ERD metadata from file if it exists."""
    metadata_file = "erd_metadata.json"
    if os.path.exists(metadata_file):
        try:
            with open(metadata_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            st.warning(f"Error loading ERD metadata: {e}")
    return None

def save_erd_metadata(metadata):
    """Save ERD metadata to file."""
    try:
        with open("erd_metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)
        return True
    except Exception as e:
        st.error(f"Error saving ERD metadata: {e}")
        return False

def main():
    """Main function for the ERD Generator page."""
    st.set_page_config(page_title="ERD Generator", page_icon="🔗")
    
    st.title("🔗 Entity Relationship Diagram Generator")
    st.markdown("Generate ERD diagrams from your Parquet files to understand table relationships.")
    
    # Navigation
    back_button = st.button(
        label="← Back to Home",
        help="Navigate to the home page"
    )
    if back_button:
        st.markdown("Navigate to the Home page using the sidebar menu.")
    
    # Check if Parquet files exist
    stage1_path = get_stage1_path()
    if not os.path.exists(stage1_path):
        st.error(f"Stage1 directory not found: {stage1_path}")
        st.info("Please run the Stage1 parser first to generate Parquet files.")
        return
    
    # Load existing metadata
    existing_metadata = load_erd_metadata()
    
    # Sidebar controls
    st.sidebar.header("ERD Controls")
    
    if existing_metadata:
        st.sidebar.success("✅ ERD metadata found")
        st.sidebar.info(f"Generated: {existing_metadata.get('generated_at', 'Unknown')}")
        st.sidebar.info(f"Tables: {existing_metadata['summary']['total_tables']}")
        st.sidebar.info(f"Relationships: {existing_metadata['summary']['total_relationships']}")
        
        regenerate = st.sidebar.button(
            label="🔄 Regenerate ERD",
            help="Re-run the ERD analysis"
        )
    else:
        st.sidebar.warning("⚠️ No ERD metadata found")
        regenerate = True
    
    # Main content area
    if regenerate or not existing_metadata:
        st.header("🔍 ERD Analysis")
        st.markdown("This will analyze all Parquet files to detect relationships between tables.")
        
        if st.button(
            label="🚀 Start ERD Analysis",
            type="primary",
            help="Begin the ERD analysis process"
        ):
            # Initialize generator
            generator = ERDGenerator()
            
            # Progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                # Connect to database
                generator.connect_db()
                
                # Analyze all tables
                success = generator.analyze_all_tables(progress_bar, status_text)
                
                if success:
                    # Generate metadata
                    metadata = generator.generate_json_metadata()
                    
                    # Save metadata
                    if save_erd_metadata(metadata):
                        st.success("✅ ERD analysis completed and saved!")
                        
                        # Store in session state for immediate viewing
                        st.session_state.erd_metadata = metadata
                        st.session_state.erd_generator = generator
                        
                        # Auto-refresh to show results
                        st.experimental_rerun()
                    else:
                        st.error("❌ Failed to save ERD metadata")
                else:
                    st.error("❌ ERD analysis failed")
                    
            except Exception as e:
                st.error(f"❌ Error during ERD analysis: {e}")
            finally:
                generator.close_db()
    
    # Display results
    metadata_to_display = st.session_state.get('erd_metadata', existing_metadata)
    
    if metadata_to_display:
        st.header("📊 ERD Analysis Results")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Tables", metadata_to_display['summary']['total_tables'])
        with col2:
            st.metric("Total Relationships", metadata_to_display['summary']['total_relationships'])
        with col3:
            st.metric("Foreign Key Relationships", metadata_to_display['summary']['foreign_key_relationships'])
        with col4:
            st.metric("Common Field Relationships", metadata_to_display['summary']['common_field_relationships'])
        
        # Mermaid diagram
        st.subheader("🔗 Entity Relationship Diagram")
        
        if 'erd_generator' in st.session_state:
            generator = st.session_state.erd_generator
        else:
            # Recreate generator for display
            generator = ERDGenerator()
            generator.tables = metadata_to_display['tables']
            generator.relationships = metadata_to_display['relationships']
        
        mermaid_diagram = generator.generate_mermaid_erd()
        
        # Display Mermaid diagram
        st.markdown("""
        <div style="text-align: center; margin: 20px 0;">
            <p><strong>Mermaid ERD Diagram</strong></p>
            <p>Copy the code below and paste it at <a href="https://mermaid.live/" target="_blank">mermaid.live</a></p>
        </div>
        """, unsafe_allow_html=True)
        
        st.code(mermaid_diagram, language="mermaid")
        
        # Download options
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                label="📥 Download Mermaid Code",
                data=mermaid_diagram,
                file_name="erd_diagram.mermaid",
                mime="text/plain",
                help="Download the Mermaid diagram code"
            )
        
        with col2:
            st.download_button(
                label="📥 Download JSON Metadata",
                data=json.dumps(metadata_to_display, indent=2),
                file_name="erd_metadata.json",
                mime="application/json",
                help="Download the complete ERD metadata"
            )
        
        # Table details
        st.subheader("📋 Table Details")
        
        # Table selector
        table_names = list(metadata_to_display['tables'].keys())
        selected_table = st.selectbox(
            label="Select a table to view details:",
            options=table_names,
            help="Choose a table to see its structure and characteristics"
        )
        
        if selected_table:
            table_info = metadata_to_display['tables'][selected_table]
            
            # Table summary
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Columns", len(table_info['columns']))
            with col2:
                st.metric("Primary Keys", sum(1 for col in table_info['columns'] if col['is_primary_key']))
            with col3:
                st.metric("Foreign Keys", sum(1 for col in table_info['columns'] if col['is_foreign_key']))
            
            # Column details
            st.markdown("#### Column Details")
            
            # Create DataFrame for display
            columns_data = []
            for col in table_info['columns']:
                characteristics = []
                if col['is_primary_key']:
                    characteristics.append("Primary Key")
                if col['is_foreign_key']:
                    characteristics.append("Foreign Key")
                if col['is_reference_field']:
                    characteristics.append("Reference Field")
                
                columns_data.append({
                    'Column': col['name'],
                    'Type': col['type'],
                    'Characteristics': ', '.join(characteristics) if characteristics else 'Regular Field',
                    'Unique Values': col['unique_values'],
                    'Null Count': col['null_count']
                })
            
            df = pd.DataFrame(columns_data)
            st.dataframe(df, use_container_width=True)
        
        # Relationships
        st.subheader("🔗 Detected Relationships")
        
        if metadata_to_display['relationships']:
            # Create DataFrame for relationships
            relationships_data = []
            for rel in metadata_to_display['relationships']:
                relationships_data.append({
                    'From Table': rel['from_table'],
                    'From Column': rel['from_column'],
                    'To Table': rel['to_table'],
                    'To Column': rel['to_column'],
                    'Type': rel['relationship_type'],
                    'Confidence': rel['confidence']
                })
            
            rel_df = pd.DataFrame(relationships_data)
            st.dataframe(rel_df, use_container_width=True)
            
            # Relationship statistics
            st.markdown("#### Relationship Statistics")
            rel_stats = rel_df['Type'].value_counts()
            st.bar_chart(rel_stats)
        else:
            st.info("No relationships detected between tables.")
    
    else:
        st.info("No ERD data available. Click 'Start ERD Analysis' to begin.")

if __name__ == "__main__":
    main()
