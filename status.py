import os
import glob
from datetime import datetime
from config import get_stage1_path, get_export_path
from pymongo import MongoClient
from config import MONGODB_URI, DATABASES


def get_parquet_files():
    """Get all Parquet files in the stage1 directory."""
    stage1_path = get_stage1_path()
    pattern = os.path.join(stage1_path, "*.parquet")
    return glob.glob(pattern)


def get_collection_names():
    """Get all collection names from MongoDB."""
    try:
        client = MongoClient(MONGODB_URI)
        db_name = DATABASES['production']
        db = client[db_name]
        collections = db.list_collection_names()
        client.close()
        return collections
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return []


def get_file_info(filepath):
    """Get information about a Parquet file."""
    try:
        import pandas as pd
        df = pd.read_parquet(filepath)
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        return {
            'rows': len(df),
            'columns': len(df.columns),
            'size_mb': file_size,
            'filename': os.path.basename(filepath)
        }
    except Exception as e:
        return {
            'rows': 0,
            'columns': 0,
            'size_mb': 0,
            'filename': os.path.basename(filepath),
            'error': str(e)
        }


def main():
    """Display the current status of the MongoDB to Parquet pipeline."""
    print("="*80)
    print("📊 MONGODB TO PARQUET PIPELINE STATUS")
    print("="*80)
    
    # Get collection information
    collections = get_collection_names()
    print(f"\n🗄️  MongoDB Collections: {len(collections)} total")
    
    # Get Parquet files
    parquet_files = get_parquet_files()
    print(f"📁 Stage1 Parquet Files: {len(parquet_files)} total")
    
    # Calculate totals
    total_rows = 0
    total_size_mb = 0
    
    if parquet_files:
        print(f"\n📋 Stage1 Files Details:")
        print("-" * 80)
        print(f"{'Collection':<25} {'Rows':<10} {'Columns':<10} {'Size (MB)':<12} {'Timestamp'}")
        print("-" * 80)
        
        for filepath in sorted(parquet_files):
            info = get_file_info(filepath)
            
            # Extract collection name from filename
            filename = info['filename']
            if '_stage1_' in filename:
                collection_name = filename.split('_stage1_')[0]
            else:
                collection_name = filename.replace('.parquet', '')
            
            # Extract timestamp
            if '_stage1_' in filename:
                timestamp_part = filename.split('_stage1_')[1].replace('.parquet', '')
                try:
                    dt = datetime.strptime(timestamp_part, "%Y%m%d_%H%M%S")
                    timestamp = dt.strftime("%Y-%m-%d %H:%M")
                except:
                    timestamp = timestamp_part
            else:
                timestamp = "Unknown"
            
            print(f"{collection_name:<25} {info['rows']:<10,} {info['columns']:<10} {info['size_mb']:<12.2f} {timestamp}")
            
            total_rows += info['rows']
            total_size_mb += info['size_mb']
        
        print("-" * 80)
        print(f"{'TOTAL':<25} {total_rows:<10,} {'':<10} {total_size_mb:<12.2f}")
    
    # Check for missing collections
    processed_collections = set()
    for filepath in parquet_files:
        filename = os.path.basename(filepath)
        if '_stage1_' in filename:
            collection_name = filename.split('_stage1_')[0]
            processed_collections.add(collection_name)
    
    missing_collections = set(collections) - processed_collections
    
    if missing_collections:
        print(f"\n❌ Missing Collections ({len(missing_collections)}):")
        for collection in sorted(missing_collections):
            print(f"  - {collection}")
    
    # Check for extra files (not in MongoDB)
    extra_files = processed_collections - set(collections)
    if extra_files:
        print(f"\n⚠️  Extra Files ({len(extra_files)}):")
        for collection in sorted(extra_files):
            print(f"  - {collection}")
    
    # Summary
    print(f"\n📈 SUMMARY:")
    print(f"  • MongoDB Collections: {len(collections)}")
    print(f"  • Processed Collections: {len(processed_collections)}")
    print(f"  • Missing Collections: {len(missing_collections)}")
    print(f"  • Total Rows: {total_rows:,}")
    print(f"  • Total Size: {total_size_mb:.2f} MB")
    
    if len(collections) > 0:
        completion_rate = (len(processed_collections) / len(collections)) * 100
        print(f"  • Completion Rate: {completion_rate:.1f}%")
    
    # Check for log files
    log_files = []
    if os.path.exists('logs'):
        log_files = [f for f in os.listdir('logs') if f.endswith('.log')]
    
    if log_files:
        print(f"\n📝 Log Files:")
        for log_file in sorted(log_files):
            log_path = os.path.join('logs', log_file)
            if os.path.exists(log_path):
                file_size = os.path.getsize(log_path) / 1024  # KB
                print(f"  • {log_file}: {file_size:.1f} KB")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
