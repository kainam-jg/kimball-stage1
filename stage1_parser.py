import pandas as pd
import json
import os
import re
from datetime import datetime
from typing import Dict, List, Any, Tuple
from pymongo import MongoClient
from config import MONGODB_URI, DATABASES, get_stage1_path
from logging_utils import get_stage1_logger, log_stage_start, log_stage_complete, log_error


class NestedDataDetector:
    """Detects if a collection has nested data structures."""
    
    def __init__(self):
        self.logger = get_stage1_logger()
    
    def has_nested_data(self, doc: Dict) -> bool:
        """Check if a document has nested data structures."""
        for value in doc.values():
            if isinstance(value, dict) and value:
                return True
            elif isinstance(value, list) and value:
                # Check if list contains dictionaries
                if any(isinstance(item, dict) for item in value):
                    return True
        return False
    
    def analyze_collection(self, collection) -> Tuple[bool, int, int]:
        """Analyze a collection to determine if it has nested data."""
        total_docs = collection.count_documents({})
        if total_docs == 0:
            return False, 0, 0
        
        # Sample documents to check for nested data
        sample_size = min(100, total_docs)
        sample_docs = list(collection.find().limit(sample_size))
        
        nested_count = sum(1 for doc in sample_docs if self.has_nested_data(doc))
        nested_percentage = (nested_count / sample_size) * 100
        
        has_nested = nested_percentage > 10  # If more than 10% have nested data
        
        self.logger.info(f"Collection analysis: {nested_count}/{sample_size} docs have nested data ({nested_percentage:.1f}%)")
        
        return has_nested, total_docs, nested_count


class UniversalFlattener:
    """Recursively flattens nested MongoDB documents."""
    
    def __init__(self):
        self.logger = get_stage1_logger()
    
    def flatten_document(self, doc: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Recursively flatten a nested document."""
        flattened = {}
        
        for key, value in doc.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            
            if isinstance(value, dict):
                # Recursively flatten nested dictionaries
                nested_flat = self.flatten_document(value, new_key, sep)
                flattened.update(nested_flat)
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    # Handle lists of dictionaries by numbering
                    for i, item in enumerate(value):
                        item_key = f"{new_key}{sep}{i}"
                        item_flat = self.flatten_document(item, item_key, sep)
                        flattened.update(item_flat)
                else:
                    # Handle lists of primitives by converting to JSON string
                    flattened[new_key] = json.dumps(value, default=str)
            else:
                # Handle primitive values
                if hasattr(value, '__str__') and 'ObjectId' in str(type(value)):
                    flattened[new_key] = str(value)
                elif hasattr(value, 'isoformat'):
                    flattened[new_key] = value.isoformat()
                elif hasattr(value, 'strftime'):
                    flattened[new_key] = str(value)
                else:
                    flattened[new_key] = str(value)
        
        return flattened


class Denormalizer:
    """Denormalizes flattened data with numbered columns."""
    
    def __init__(self):
        self.logger = get_stage1_logger()
        self.numbered_pattern = re.compile(r'(.+)_(\d+)_(.+)')
    
    def identify_numbered_columns(self, df: pd.DataFrame) -> Tuple[List[str], Dict[str, List[Tuple[str, int, str]]]]:
        """Identify numbered columns and group them."""
        numbered_columns = []
        numbered_groups = {}
        
        for col in df.columns:
            match = self.numbered_pattern.match(col)
            if match:
                base_name, number, suffix = match.groups()
                numbered_columns.append(col)
                
                if base_name not in numbered_groups:
                    numbered_groups[base_name] = []
                numbered_groups[base_name].append((col, int(number), suffix))
        
        return numbered_columns, numbered_groups
    
    def denormalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Denormalize a dataframe with numbered columns."""
        numbered_columns, numbered_groups = self.identify_numbered_columns(df)
        
        if not numbered_columns:
            self.logger.info("No numbered columns found, returning original dataframe")
            return df
        
        # Get base columns (non-numbered)
        base_columns = [col for col in df.columns if col not in numbered_columns]
        
        # Find maximum numbers for each group
        max_numbers = {}
        for base_name, group in numbered_groups.items():
            max_numbers[base_name] = max(number for _, number, _ in group)
        
        self.logger.info(f"Denormalizing {len(numbered_groups)} groups with {len(numbered_columns)} numbered columns")
        
        # Create denormalized rows
        denormalized_rows = []
        
        for idx, row in df.iterrows():
            base_data = {col: row[col] for col in base_columns}
            
            for base_name, group in numbered_groups.items():
                max_num = max_numbers[base_name]
                has_data_in_group = False
                
                for num in range(max_num + 1):
                    row_data = base_data.copy()
                    has_number_data = False
                    
                    for col, number, suffix in group:
                        if number == num:
                            value = row[col]
                            if pd.notna(value) and str(value).strip() != '':
                                row_data[suffix] = value
                                has_number_data = True
                    
                    if has_number_data:
                        denormalized_rows.append(row_data)
                        has_data_in_group = True
                
                # If no data for this group, add one row with empty values
                if not has_data_in_group:
                    row_data = base_data.copy()
                    for _, _, suffix in group:
                        row_data[suffix] = ''
                    denormalized_rows.append(row_data)
        
        denormalized_df = pd.DataFrame(denormalized_rows)
        
        self.logger.info(f"Denormalization complete: {len(df)} → {len(denormalized_df)} rows")
        return denormalized_df


class UnifiedStage1Parser:
    """Unified parser that handles both simple and nested collections."""
    
    def __init__(self):
        self.logger = get_stage1_logger()
        self.detector = NestedDataDetector()
        self.flattener = UniversalFlattener()
        self.denormalizer = Denormalizer()
        self.export_path = get_stage1_path()
    
    def process_collection(self, collection_name: str, db) -> bool:
        """Process a single collection through Stage1."""
        try:
            collection = db[collection_name]
            log_stage_start(self.logger, "Stage1", collection_name)
            
            # Step 1: Analyze collection for nested data
            has_nested, total_docs, nested_count = self.detector.analyze_collection(collection)
            
            if not has_nested:
                # Simple collection - export directly
                self.logger.info(f"Collection '{collection_name}' has no nested data, exporting directly")
                return self._export_simple_collection(collection, collection_name, total_docs)
            else:
                # Complex collection - flatten then denormalize
                self.logger.info(f"Collection '{collection_name}' has nested data, processing with flattening and denormalization")
                return self._process_nested_collection(collection, collection_name, total_docs)
                
        except Exception as e:
            log_error(self.logger, "Stage1", e, collection_name)
            return False
    
    def _export_simple_collection(self, collection, collection_name: str, total_docs: int) -> bool:
        """Export a simple collection without nested data."""
        try:
            # Convert all documents to strings
            documents = []
            for doc in collection.find():
                string_doc = {}
                for key, value in doc.items():
                    if hasattr(value, '__str__') and 'ObjectId' in str(type(value)):
                        string_doc[key] = str(value)
                    elif hasattr(value, 'isoformat'):
                        string_doc[key] = value.isoformat()
                    elif hasattr(value, 'strftime'):
                        string_doc[key] = str(value)
                    else:
                        string_doc[key] = str(value)
                documents.append(string_doc)
            
            # Create DataFrame and export
            df = pd.DataFrame(documents)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{collection_name}_stage1_{timestamp}.parquet"
            filepath = os.path.join(self.export_path, filename)
            
            df.to_parquet(filepath, index=False)
            
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            log_stage_complete(self.logger, "Stage1", collection_name, len(df), file_size_mb)
            
            return True
            
        except Exception as e:
            log_error(self.logger, "Stage1", e, collection_name)
            return False
    
    def _process_nested_collection(self, collection, collection_name: str, total_docs: int) -> bool:
        """Process a collection with nested data through flattening and denormalization."""
        try:
            # Step 1: Flatten the collection
            self.logger.info(f"Flattening collection '{collection_name}'")
            flattened_docs = []
            
            for doc in collection.find():
                flattened_doc = self.flattener.flatten_document(doc)
                flattened_docs.append(flattened_doc)
            
            # Create flattened DataFrame
            flattened_df = pd.DataFrame(flattened_docs)
            
            # Step 2: Denormalize the flattened data
            self.logger.info(f"Denormalizing collection '{collection_name}'")
            denormalized_df = self.denormalizer.denormalize_dataframe(flattened_df)
            
            # Step 3: Export final denormalized data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{collection_name}_stage1_{timestamp}.parquet"
            filepath = os.path.join(self.export_path, filename)
            
            denormalized_df.to_parquet(filepath, index=False)
            
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            log_stage_complete(self.logger, "Stage1", collection_name, len(denormalized_df), file_size_mb)
            
            return True
            
        except Exception as e:
            log_error(self.logger, "Stage1", e, collection_name)
            return False
    
    def process_all_collections(self, db) -> Dict[str, bool]:
        """Process all collections in the database."""
        collections = db.list_collection_names()
        self.logger.info(f"Found {len(collections)} collections to process")
        
        results = {}
        successful = 0
        
        for collection_name in collections:
            self.logger.info(f"Processing collection: {collection_name}")
            success = self.process_collection(collection_name, db)
            results[collection_name] = success
            if success:
                successful += 1
        
        self.logger.info(f"Stage1 processing complete: {successful}/{len(collections)} collections successful")
        return results
    
    def process_specific_collections(self, collection_names: List[str], db) -> Dict[str, bool]:
        """Process specific collections."""
        results = {}
        successful = 0
        
        for collection_name in collection_names:
            if collection_name in db.list_collection_names():
                self.logger.info(f"Processing collection: {collection_name}")
                success = self.process_collection(collection_name, db)
                results[collection_name] = success
                if success:
                    successful += 1
            else:
                self.logger.warning(f"Collection '{collection_name}' not found in database")
                results[collection_name] = False
        
        self.logger.info(f"Stage1 processing complete: {successful}/{len(collection_names)} collections successful")
        return results


def main():
    """Main function to run the Stage1 parser."""
    logger = get_stage1_logger()
    
    try:
        # Connect to MongoDB
        client = MongoClient(MONGODB_URI)
        db_name = DATABASES['production']
        db = client[db_name]
        
        logger.info(f"Connected to MongoDB database: {db_name}")
        
        # Create parser
        parser = UnifiedStage1Parser()
        
        # Get user choice
        print("\n" + "="*60)
        print("UNIFIED STAGE1 PARSER")
        print("="*60)
        print("1. Process all collections")
        print("2. Process specific collections")
        print("3. Test mode (process first 3 collections)")
        print("="*60)
        
        choice = input("Enter your choice (1-3): ").strip()
        
        if choice == "1":
            logger.info("Starting processing of all collections")
            results = parser.process_all_collections(db)
            
        elif choice == "2":
            collections = input("Enter collection names (comma-separated): ").strip()
            collection_names = [name.strip() for name in collections.split(",")]
            logger.info(f"Starting processing of specific collections: {collection_names}")
            results = parser.process_specific_collections(collection_names, db)
            
        elif choice == "3":
            all_collections = db.list_collection_names()
            test_collections = all_collections[:3]
            logger.info(f"Starting test mode with collections: {test_collections}")
            results = parser.process_specific_collections(test_collections, db)
            
        else:
            logger.error("Invalid choice")
            return
        
        # Print summary
        print("\n" + "="*60)
        print("PROCESSING SUMMARY")
        print("="*60)
        
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        print(f"Total collections: {total}")
        print(f"Successful: {successful}")
        print(f"Failed: {total - successful}")
        print(f"Success rate: {(successful/total)*100:.1f}%")
        
        if total - successful > 0:
            print("\nFailed collections:")
            for collection, success in results.items():
                if not success:
                    print(f"  - {collection}")
        
        print("="*60)
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        print(f"Error: {str(e)}")
    
    finally:
        client.close()


if __name__ == "__main__":
    main()
