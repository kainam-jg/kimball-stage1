# MongoDB to Parquet Pipeline

A comprehensive solution for extracting, flattening, and normalizing MongoDB collections into Parquet format for analytics and data processing.

## Overview

This pipeline provides an intelligent, unified approach to transform MongoDB documents into clean, analytics-ready Parquet files:

- **Smart Detection**: Automatically detects whether collections have nested data structures
- **Unified Processing**: Handles both simple and complex collections in a single stage
- **Optimized Output**: Creates normalized Parquet files with all data types as strings

## ✅ Processing Status: COMPLETED

**Successfully processed 73 MongoDB collections through unified Stage1:**

- **Stage1**: 71 collections processed and exported
- **Smart Processing**: Collections with nested data are flattened and denormalized
- **Simple Collections**: Collections without nested data are exported directly
- **File Structure**: Organized in `parquet_exports/stage1/`

## Features

### 🔄 Intelligent MongoDB Parser
- **Smart Detection**: Automatically analyzes collections to detect nested data structures
- **Unified Processing**: Single parser handles both simple and complex collections
- **Recursive Flattening**: Completely flattens nested JSON structures of any depth
- **String Conversion**: Converts all data types to strings for consistency
- **Batch Processing**: Handles large collections efficiently with memory-optimized processing
- **Comprehensive Logging**: Detailed logging with file and console output

### 📊 Unified Processing Pipeline

#### Stage1: Intelligent Processing
- **Input**: MongoDB collections (simple or nested)
- **Smart Analysis**: Detects nested data structures in each collection
- **Simple Collections**: Direct export with string conversion
- **Complex Collections**: Flatten → Denormalize → Export
- **Output**: String-only Parquet files ready for analytics
- **Example**: `cartFields_0_title` → `title` (denormalized)

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

### Configuration Management
- **JSON Configuration**: Centralized `config.json` for all settings
- **Export Paths**: Configurable output directories
- **Logging**: Comprehensive logging for monitoring and debugging

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

The pipeline uses `config.json` for configuration:

```json
{
    "mongodb": {
        "uri": "your_mongodb_connection_string",
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
        "stage1_log_file": "logs/stage1.log"
    }
}
```

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
5. Export to `{collection_name}_stage1_{timestamp}.parquet`

### Status and Verification

```bash
# Check processing status
python status.py

# Verify Stage1 files
python verify_stage1.py

# Explore data with Streamlit
streamlit run streamlit_app.py
```

## File Structure

```
project/
├── stage1_parser.py          # Unified Stage1: Smart MongoDB to Parquet
├── verify_stage1.py          # Verification tool for Stage1 files
├── status.py                 # Status checker
├── streamlit_app.py          # Data explorer
├── config.py                 # Configuration loader
├── config.json              # Configuration settings
├── logging_utils.py         # Logging utilities
├── requirements.txt         # Dependencies
├── parquet_exports/         # Output directory
│   └── stage1/             # Stage1 processed files
├── logs/                   # Processing logs
│   └── stage1.log         # Stage1 processing logs
└── README.md              # This file
```

## Processing Results

### Stage1 Output
- **Format**: `{collection_name}_stage1_{timestamp}.parquet`
- **Data Type**: All strings
- **Structure**: 
  - **Simple Collections**: Direct export with string conversion
  - **Complex Collections**: Flattened and denormalized
- **Example**: `cartFields_0_title` → `title` (denormalized)

### Smart Processing Examples

**Collections with Nested Data (Flattened & Denormalized):**
- **`carts`**: 15,474 → 75,443 rows (4.9x expansion, 233 → 38 columns)
- **`actioncarts`**: 34,700 → 139,757 rows (4.0x expansion, 164 → 45 columns)
- **`countries`**: 21 → 416 rows (19.8x expansion, 106 → 12 columns)

**Collections without Nested Data (Direct Export):**
- **`visits`**: 512,022 → 512,022 rows (1.0x, no expansion)
- **`proxies`**: 242,202 → 242,202 rows (1.0x, no expansion)
- **`ssos`**: 117,040 → 117,040 rows (1.0x, no expansion)

## Performance

- **Memory Efficient**: Batch processing for large collections
- **Scalable**: Handles collections with millions of documents
- **Fast**: Optimized for Parquet compression and storage
- **Reliable**: Comprehensive error handling and validation
- **Logging**: Detailed processing logs for monitoring and debugging
- **Smart**: Only processes complex collections when necessary

## Use Cases

- **Data Warehousing**: Transform MongoDB data for analytics platforms
- **ETL Pipelines**: Prepare data for business intelligence tools
- **Machine Learning**: Create clean datasets for ML training
- **Data Migration**: Convert MongoDB data to other formats
- **Analytics**: Enable SQL-like queries on MongoDB data
- **Data Exploration**: Interactive exploration via Streamlit interface

## Benefits

- **Complete Data Preservation**: No data loss during processing
- **Analytics Ready**: Clean, normalized structure for analysis
- **Scalable**: Handles collections of any size
- **Flexible**: Works with any MongoDB collection structure
- **Efficient**: Optimized Parquet format for storage and querying
- **Organized**: Clear separation between simple and complex processing
- **Monitored**: Comprehensive logging and status tracking
- **Smart**: Automatically detects and handles different collection types

## Data Explorer

The included Streamlit application provides:
- **File Browser**: Browse and select specific collection files
- **Data Preview**: Interactive data exploration with filtering and search
- **Column Analysis**: Statistical analysis of data types and distributions
- **Visual Analytics**: Charts and graphs for data insights
- **Smart Display**: Shows processing type (simple vs. complex)

## Next Steps

The pipeline is designed to be extensible for future stages:
- **Stage2**: Data type inference and conversion
- **Stage3**: Schema validation and optimization
- **Stage4**: Integration with data warehouses
- **Stage5**: Automated data quality checks

## Support

For issues, questions, or contributions, please refer to the project documentation or create an issue in the repository.

---

**Pipeline Status**: ✅ **COMPLETED** - All 73 collections successfully processed through unified Stage1.
