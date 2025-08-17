import logging
import os
from pathlib import Path
from config import LOG_LEVEL, LOG_FORMAT, STAGE1_LOG_FILE

def setup_logger(name: str, log_file: str, level: str = None) -> logging.Logger:
    """Set up a logger with file and console handlers."""
    # Create logs directory if it doesn't exist
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level or LOG_LEVEL))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create file handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(getattr(logging, level or LOG_LEVEL))
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, level or LOG_LEVEL))
    
    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def get_stage1_logger() -> logging.Logger:
    """Get the Stage1 logger."""
    return setup_logger("stage1", STAGE1_LOG_FILE)

def log_stage_start(logger: logging.Logger, stage: str, collection_name: str = None):
    """Log the start of a processing stage."""
    if collection_name:
        logger.info(f"🚀 Starting {stage} for collection: {collection_name}")
    else:
        logger.info(f"🚀 Starting {stage}")

def log_stage_complete(logger: logging.Logger, stage: str, collection_name: str = None,
                      records_processed: int = None, file_size_mb: float = None):
    """Log the completion of a processing stage."""
    message = f"✅ Completed {stage}"
    if collection_name:
        message += f" for collection: {collection_name}"
    if records_processed is not None:
        message += f" - {records_processed:,} records processed"
    if file_size_mb is not None:
        message += f" - {file_size_mb:.2f} MB"
    logger.info(message)

def log_error(logger: logging.Logger, stage: str, error: Exception, collection_name: str = None):
    """Log an error during processing."""
    message = f"❌ Error in {stage}"
    if collection_name:
        message += f" for collection: {collection_name}"
    message += f": {str(error)}"
    logger.error(message)
