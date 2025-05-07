"""
Utility functions for the DB Analyzer.
"""

import pandas as pd
from typing import Optional, Dict, Any, List
import json
import os
from datetime import datetime

def load_config_from_env() -> Dict[str, Any]:
    """
    Load configuration from environment variables.
    
    Returns:
        Dictionary with config values
    """
    config = {
        "db": {
            "host": os.environ.get("DB_HOST", "localhost"),
            "port": int(os.environ.get("DB_PORT", "5432")),
            "dbname": os.environ.get("DB_NAME", ""),
            "user": os.environ.get("DB_USER", ""),
            "password": os.environ.get("DB_PASSWORD", "")
        },
        "openai": {
            "api_key": os.environ.get("OPENAI_API_KEY", ""),
            "model": os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
        },
        "mcp": {
            "api_key": os.environ.get("MCP_API_KEY", "")
        }
    }
    return config

def format_markdown_table(df: pd.DataFrame) -> str:
    """
    Format DataFrame as markdown table.
    
    Args:
        df: DataFrame to format
        
    Returns:
        Markdown table string
    """
    return df.to_markdown(index=False)

def sanitize_filename(name: str) -> str:
    """
    Sanitize a string for use as a filename.
    
    Args:
        name: String to sanitize
        
    Returns:
        Sanitized string
    """
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name

def get_timestamp() -> str:
    """
    Get current timestamp as string.
    
    Returns:
        Timestamp string in format YYYYMMDD_HHMMSS
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def save_analysis_to_file(analysis: Dict[str, Any], request: str) -> str:
    """
    Save analysis results to a JSON file.
    
    Args:
        analysis: Analysis results
        request: Original request
        
    Returns:
        Path to saved file
    """
    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)
    
    # Create filename from request and timestamp
    safe_request = sanitize_filename(request)[:30]  # Limit length
    timestamp = get_timestamp()
    filename = f"output/analysis_{safe_request}_{timestamp}.json"
    
    # Save to file
    with open(filename, 'w') as f:
        json.dump(analysis, f, indent=2)
        
    return filename

def truncate_long_text(text: str, max_length: int = 1000) -> str:
    """
    Truncate long text for display.
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        
    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length] + "... [truncated]"