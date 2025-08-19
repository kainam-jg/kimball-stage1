# MongoDB to Parquet Pipeline

A comprehensive solution for extracting, flattening, and normalizing MongoDB collections into Parquet format for analytics and data processing.

## Overview

This pipeline provides an intelligent, unified approach to transform MongoDB documents into clean, analytics-ready Parquet files:

- **Smart Detection**: Automatically detects whether collections have nested data structures
- **Unified Processing**: Handles both simple and complex collections in a single stage
- **Optimized Output**: Creates normalized Parquet files with all data types as strings
- **Simple Naming**: Clean, collection-based file naming without timestamps
- **SQL Querying**: Advanced DuckDB integration for direct SQL queries on Parquet files

## Features

### 🔄 Intelligent MongoDB Parser
- **Smart Detection**: Automatically analyzes collections to detect nested data structures
- **Unified Processing**: Single parser handles both simple and complex collections
- **Recursive Flattening**: Completely flattens nested JSON structures of any depth
- **String Conversion**: Converts all data types to strings for consistency
- **Batch Processing**: Handles large collections efficiently with memory-optimized processing
- **Comprehensive Logging**: Detailed logging with file and console output
- **Simple File Naming**: Clean collection-based names without timestamps

### 📊 Unified Processing Pipeline

#### Stage1: Intelligent Processing
- **Input**: MongoDB collections (simple or nested)
- **Smart Analysis**: Detects nested data structures in each collection
- **Simple Collections**: Direct export with string conversion
- **Complex Collections**: Flatten → Denormalize → Export
- **Output**: String-only Parquet files ready for analytics
- **File Naming**: `{collection_name}.parquet` (simple and clean)
- **Example**: `parent_0_child` → `child` (denormalized)

### 🦆 DuckDB SQL Querying
- **Direct SQL Queries**: Write SQL directly on Parquet files
- **Simple Table Names**: Use collection names as table names (e.g., `SELECT * FROM collection_name`)
- **Visual Join Builder**: Build complex joins between multiple files
- **Schema Discovery**: Automatic schema detection and display
- **Query Results**: Download results as CSV or Parquet
- **Sample Queries**: Built-in examples for common operations

## Architecture

### Core Classes

#### NestedDataDetector
- Analyzes MongoDB collection structure to detect nested data
- Samples documents to determine complexity
- Provides intelligent processing decisions

#### UniversalFlattener
- Recursively flattens nested MongoDB documents
- Handles lists of dictionaries by creating numbered columns
- Converts all values to strings for consistency

#### Denormalizer
- Identifies numbered column patterns (e.g., `parent_0_child`)
- Groups related columns by base names
- Transposes numbered columns into normalized rows

#### UnifiedStage1Parser
- Orchestrates the entire processing pipeline
- Makes intelligent decisions based on collection analysis
- Handles both simple and complex collections seamlessly
- Uses simple file naming: `{collection_name}.parquet`

### Configuration Management
- **JSON Configuration**: Centralized `config.json` for all settings
- **Export Paths**: Configurable output directories
- **Logging**: Comprehensive logging for monitoring and debugging

## Installation

1. Clone the repository:
```bash
git clone <your-repository-url>
cd <project-directory>
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure the application:
   - Create a `config.json` file with your MongoDB connection details and authentication:
     - Replace `username:password` with your MongoDB credentials
     - Replace `cluster.mongodb.net` with your cluster URL
     - Replace `your_database_name` with your actual database name
     - Add authentication credentials for the web interface
   - Ensure your MongoDB instance is accessible

## Configuration

The pipeline uses `config.json` for configuration. Create this file with the following structure:

```json
{
    "mongodb": {
        "uri": "mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority",
        "databases": {
            "production": "your_database_name"
        }
    },
    "export": {
        "base_directory": "parquet_exports",
        "stage1_directory": "stage1"
    },
    "logging": {
        "level": "INFO",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "stage1_log_file": "logs/stage1.log"
    },
    "authentication": {
        "username": "your_web_username",
        "password": "your_web_password"
    }
}
```

**Note**: The `config.json` file is excluded from version control for security reasons.

## Usage

### Stage1: Unified Processing

```bash
python stage1_parser.py
```

The unified Stage1 parser will:
1. Connect to MongoDB and list all collections
2. Show processing options (all collections, specific collections, or test mode)
3. **Smart Analysis**: Analyze each collection for nested data structures
4. **Intelligent Processing**:
   - **Simple Collections**: Export directly with string conversion
   - **Complex Collections**: Flatten → Denormalize → Export
5. Export to `{collection_name}.parquet` (simple naming)

### Status and Verification

```bash
# Check processing status
python status.py

# Verify Stage1 files
python verify_stage1.py

# Explore data with Streamlit
streamlit run "01_Find_Collections.py"
```

## File Structure

```
project/
├── auth_utils.py            # Authentication utilities
├── 01_Find_Collections.py      # Main Streamlit app (Home page)
├── stage1_parser.py            # Unified Stage1: Smart MongoDB to Parquet
├── verify_stage1.py            # Verification tool for Stage1 files
├── status.py                   # Status checker
├── config.py                   # Configuration loader
├── logging_utils.py           # Logging utilities
├── requirements.txt           # Dependencies
├── pages/                     # Streamlit multipage app
│   ├── 02_Processing_Logs.py      # Processing logs page
│   ├── 03_Parquet_Explorer.py     # Parquet file explorer
│   ├── 04_ERD_Generator.py        # Entity Relationship Diagram generator
│   └── 05_DuckDB_Query.py         # DuckDB SQL query interface
├── parquet_exports/           # Output directory
│   └── stage1/               # Stage1 processed files
├── metadata/                 # ERD metadata and analysis files
├── logs/                     # Processing logs
│   └── stage1.log           # Stage1 processing logs
└── README.md                # This file
```

## Processing Results

### Stage1 Output
- **Format**: `{collection_name}.parquet` (simple naming)
- **Data Type**: All strings
- **Structure**: 
  - **Simple Collections**: Direct export with string conversion
  - **Complex Collections**: Flattened and denormalized
- **Example**: `parent_0_child` → `child` (denormalized)

### Smart Processing Examples

**Collections with Nested Data (Flattened & Denormalized):**
- Collections with nested objects or arrays are flattened and denormalized
- Numbered columns (e.g., `parent_0_child`) are transposed into separate rows
- Parent data is repeated for each child record

**Collections without Nested Data (Direct Export):**
- Collections with flat structures are exported directly
- No data expansion or transformation
- Maintains original record count

## Performance

- **Memory Efficient**: Batch processing for large collections
- **Scalable**: Handles collections with millions of documents
- **Fast**: Optimized for Parquet compression and storage
- **Reliable**: Comprehensive error handling and validation
- **Logging**: Detailed processing logs for monitoring and debugging
- **Smart**: Only processes complex collections when necessary
- **Simple**: Clean file naming without complex timestamps

## Use Cases

- **Data Warehousing**: Transform MongoDB data for analytics platforms
- **ETL Pipelines**: Prepare data for business intelligence tools
- **Machine Learning**: Create clean datasets for ML training
- **Data Migration**: Convert MongoDB data to other formats
- **Analytics**: Enable SQL-like queries on MongoDB data via DuckDB
- **Data Exploration**: Interactive exploration via Streamlit interface
- **SQL Querying**: Direct SQL queries on Parquet files without data loading

## Benefits

- **Complete Data Preservation**: No data loss during processing
- **Analytics Ready**: Clean, normalized structure for analysis
- **Scalable**: Handles collections of any size
- **Flexible**: Works with any MongoDB collection structure
- **Efficient**: Optimized Parquet format for storage and querying
- **Organized**: Clear separation between simple and complex processing
- **Monitored**: Comprehensive logging and status tracking
- **Smart**: Automatically detects and handles different collection types
- **Simple**: Clean file naming without complex timestamps
- **Queryable**: Direct SQL access via DuckDB integration

## Data Explorer

The included Streamlit application provides a comprehensive multipage interface:

### 🏠 Home Page - Collection Selection
- **MongoDB Connection**: Browse all available collections in your database
- **Smart Analysis**: View collection details including document count, field count, and nested data detection
- **Search & Filter**: Find collections by name or filter by characteristics (nested data, size, etc.)
- **Batch Selection**: Select individual collections, groups, or all collections for processing
- **Processing Control**: Initiate Stage1 processing with selected collections

### 📋 Processing Logs Page
- **Real-time Monitoring**: Watch processing progress with auto-refresh capabilities
- **Log Statistics**: View counts of info, success, warning, and error messages
- **Completion Detection**: Automatic notification when processing is complete
- **Navigation**: Easy access to move between processing and exploration

### 📊 Parquet Explorer Page
- **File Browser**: Browse and select specific collection files
- **Data Preview**: Interactive data exploration with filtering and search
- **Column Analysis**: Statistical analysis of data types and distributions
- **Search & Filter**: Advanced filtering and search capabilities
- **Data Summary**: Comprehensive data quality and statistics information

### 🔗 ERD Generator Page
- **Entity Relationship Diagrams**: Automatically generate ERD diagrams from Parquet files
- **Relationship Detection**: Detect foreign key relationships and common field relationships
- **Mermaid Diagrams**: Generate Mermaid-compatible ERD diagrams
- **Persistent Metadata**: Save analysis results for quick viewing without re-processing
- **Table Analysis**: Detailed analysis of table structures and column characteristics
- **Download Options**: Export diagrams and metadata for external use
- **Progress Tracking**: Real-time progress updates during analysis
- **File Selection**: Choose specific Parquet files for ERD analysis to improve performance

### 🦆 DuckDB Query Page
- **SQL Query Interface**: Write and execute SQL queries directly on Parquet files
- **Simple Table Names**: Use collection names as table names (e.g., `SELECT * FROM collection_name`)
- **Automatic File Detection**: Automatically registers all available Parquet files as tables
- **Schema Discovery**: Automatic schema detection and display for all files
- **Query Results**: View results in interactive tables with download options
- **Sample Queries**: Built-in examples for common operations

### 🚀 Launch the Application
```bash
streamlit run "01_Find_Collections.py"
```

**Note**: The application includes built-in authentication. When you first access any page, you'll be prompted to log in using the credentials configured in your `config.json` file.

## 🔐 Authentication

The application includes a simple authentication system to secure access:

### Features
- **Login Page**: Secure entry point requiring username and password
- **Session Management**: Maintains authentication state across pages
- **User Information**: Displays current user in sidebar
- **Logout Function**: Secure logout with session cleanup
- **Configuration-Based**: Credentials stored in `config.json`

### Security
- **No Password Storage**: Passwords are not stored in session state
- **Configuration File**: Credentials stored in excluded `config.json`
- **Session-Based**: Authentication persists during browser session
- **Automatic Redirect**: Unauthenticated users redirected to login

### Usage
1. **Start Application**: Run `streamlit run 01_Find_Collections.py`
2. **Enter Credentials**: Use username/password from `config.json` when prompted
3. **Access Features**: Navigate to any page after authentication
4. **Logout**: Use logout button in sidebar when finished

## SQL Querying with DuckDB

The DuckDB Query page enables direct SQL queries on your Parquet files:

### Basic Queries
```sql
-- View all data from a collection
SELECT * FROM collection_name LIMIT 10;

-- Count records in each collection
SELECT 'collection1' as file_name, COUNT(*) as record_count FROM collection1
UNION ALL
SELECT 'collection2' as file_name, COUNT(*) as record_count FROM collection2;

-- Join collections (if they have common columns)
SELECT c.*, p.* 
FROM collection1 c 
INNER JOIN collection2 p ON c.user_id = p.id;

-- Aggregation examples
SELECT column_name, COUNT(*) as count
FROM collection_name 
GROUP BY column_name;
```

### Features
- **Simple Table Names**: Use collection names directly (no complex filenames)
- **Visual Join Builder**: Build joins graphically without writing SQL
- **Schema Discovery**: Automatic column detection and display
- **Query Results**: Interactive tables with CSV/Parquet download
- **Error Handling**: Clear error messages for debugging

## Next Steps

The pipeline is designed to be extensible for future stages:
- **Stage2**: Data type inference and conversion
- **Stage3**: Schema validation and optimization
- **Stage4**: Integration with data warehouses
- **Stage5**: Automated data quality checks

## Support

For issues, questions, or contributions, please refer to the project documentation or create an issue in the repository.

---

**Pipeline Status**: ✅ **READY** - Generic MongoDB to Parquet pipeline with intelligent processing, simple naming, and DuckDB querying capabilities.
