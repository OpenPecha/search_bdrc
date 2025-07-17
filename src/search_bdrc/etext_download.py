from pathlib import Path
from typing import Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_text_from_instance_id(instance_id: str) -> Optional[str]:
    """Get text content from a given instance ID.
    
    Args:
        instance_id: The ID of the instance (e.g. MW23703)
        
    Returns:
        Text content if successful, None if instance not found or can't be read
    """
    pass

def read_text_file(file_path: str | Path) -> Optional[str]:
    """Read text content from a file.
    
    Args:
        file_path: Path to the text file to read
        
    Returns:
        Text content if successful, None if file doesn't exist or can't be read
    """
    try:
        path = Path(file_path)
        if not path.exists():
            return None
            
        return path.read_text(encoding='utf-8')
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def main():
    # Example usage
    file_path = Path("outputs/etexts/W22084.txt")
    text = read_text_file(file_path)
    if text:
        print(f"Successfully read {len(text)} characters from {file_path}")
    else:
        print(f"Failed to read {file_path}")

if __name__ == "__main__":
    main()