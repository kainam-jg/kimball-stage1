import pandas as pd
import os
import glob
from datetime import datetime
from config import get_stage1_path


def get_stage1_files():
    """Get all Stage1 Parquet files."""
    stage1_path = get_stage1_path()
    pattern = os.path.join(stage1_path, "*.parquet")
    return glob.glob(pattern)


def verify_file(filepath):
    """Verify a single Stage1 Parquet file."""
    print(f"\n{'='*80}")
    print(f"🔍 VERIFYING: {os.path.basename(filepath)}")
    print(f"{'='*80}")
    
    try:
        # Load the file
        df = pd.read_parquet(filepath)
        
        # Basic information
        print(f"📊 Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        
        # Check data types
        print(f"\n📋 Data Types:")
        dtype_counts = df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"  • {dtype}: {count} columns")
        
        # Verify all columns are strings
        non_string_cols = df.select_dtypes(exclude=['object']).columns
        if len(non_string_cols) > 0:
            print(f"\n⚠️  WARNING: Found {len(non_string_cols)} non-string columns:")
            for col in non_string_cols:
                print(f"  • {col}: {df[col].dtype}")
        else:
            print(f"\n✅ All columns are strings (as expected)")
        
        # Check for numbered columns (indicating nested data was processed)
        numbered_cols = [col for col in df.columns if '_' in col and any(char.isdigit() for char in col)]
        if numbered_cols:
            print(f"\n🔢 Found {len(numbered_cols)} numbered columns (nested data was processed):")
            for col in numbered_cols[:10]:  # Show first 10
                print(f"  • {col}")
            if len(numbered_cols) > 10:
                print(f"  ... and {len(numbered_cols) - 10} more")
        else:
            print(f"\n📝 No numbered columns found (simple collection)")
        
        # Sample data
        print(f"\n📄 Sample Data (first 3 rows, first 5 columns):")
        sample_cols = df.columns[:5]
        sample_df = df[sample_cols].head(3)
        print(sample_df.to_string(index=False))
        
        # File size
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"\n💾 File Size: {file_size_mb:.2f} MB")
        
        # Memory usage
        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        print(f"🧠 Memory Usage: {memory_mb:.2f} MB")
        
        # Null values
        null_counts = df.isnull().sum()
        total_null = null_counts.sum()
        if total_null > 0:
            print(f"\n❌ Null Values: {total_null:,} total")
            null_cols = null_counts[null_counts > 0]
            for col, count in null_cols.head(5).items():
                print(f"  • {col}: {count:,}")
        else:
            print(f"\n✅ No null values found")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verifying file: {e}")
        return False


def main():
    """Main verification function."""
    print("🔍 STAGE1 PARQUET VERIFICATION")
    print("="*80)
    
    # Get all Stage1 files
    stage1_files = get_stage1_files()
    
    if not stage1_files:
        print("❌ No Stage1 Parquet files found!")
        print(f"Expected location: {get_stage1_path()}")
        return
    
    print(f"📁 Found {len(stage1_files)} Stage1 Parquet files")
    
    # Verify each file
    successful = 0
    failed = 0
    
    for filepath in sorted(stage1_files):
        if verify_file(filepath):
            successful += 1
        else:
            failed += 1
    
    # Summary
    print(f"\n{'='*80}")
    print("📊 VERIFICATION SUMMARY")
    print(f"{'='*80}")
    print(f"Total files: {len(stage1_files)}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"Success rate: {(successful/len(stage1_files))*100:.1f}%")
    
    if failed == 0:
        print(f"\n🎉 All Stage1 files verified successfully!")
    else:
        print(f"\n⚠️  {failed} files had issues during verification")
    
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
