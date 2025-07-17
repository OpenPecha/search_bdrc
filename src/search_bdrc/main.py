from search_bdrc import BdrcScraper
from pathlib import Path
import json
from search_bdrc.outline_formatter import TextPartProcessor
import logging

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Initialize scraper and processor
    scraper = BdrcScraper()
    processor = TextPartProcessor(scraper)
    
    # Get outline ID for an instance
    instance_id = "MW21752"
    outline_id = scraper.get_outline_of_instance(instance_id)[0]
    print(f"Outline ID: {outline_id}")
    
    # Process outline
    try:
        output_dir = Path("outputs")  # or any directory you want
        output = processor.process_outline(outline_id, output_dir=output_dir)
        logger.info(f"Successfully processed outline {outline_id}")
    except Exception as e:
        logger.error(f"Failed to process outline {outline_id}: {e}")
    
   