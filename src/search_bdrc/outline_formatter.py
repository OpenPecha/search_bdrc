import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Any

from search_bdrc import BdrcScraper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TextPartProcessor:
    def __init__(self, scraper: BdrcScraper):
        self.scraper = scraper
        self.cache_dir = Path('cache')
        self.cache_dir.mkdir(exist_ok=True)
    


    def _build_annotation_tree(self, annotations: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """Build a tree structure mapping each annotation to its direct children and siblings.
        
        Args:
            annotations: List of annotations with tree_index information
            
        Returns:
            Dictionary mapping tree_index to dict with children and siblings info
        """
        tree = {}
        for i, anno in enumerate(annotations):
            tree_idx = anno['meta']['part_tree_index']
            tree[tree_idx] = []
            # Find all descendants (direct and indirect children)
            for j, other in enumerate(annotations):
                if other['meta']['part_tree_index'].startswith(tree_idx + '.'):
                    tree[tree_idx].append(j)
        return tree

    def _convert_to_annotation_format(self, text_parts: List[Dict[str, Any]], 
                                   title: str = "BDRC Text Parts",
                                   language: str = "bo",
                                   content: str = "") -> Dict[str, Any]:
        """Convert text parts to annotation format with only headers
        
        Args:
            text_parts: List of text parts with location and label information
            title: Title of the text
            language: Language code
            content: Optional content string. If provided, annotations will be adjusted to match content length.
                    If not provided, content will be filled with underscores to match max position.
        """
        # Sort text parts by tree index to maintain hierarchy
        text_parts.sort(key=lambda x: [int(i) for i in x['meta']['part_tree_index'].split('.')])
        
        annotations = []
        
        # Calculate base spacing between annotations
        total_parts = len(text_parts)
        content_length = len(content) if content else total_parts * 10
        base_spacing = content_length // total_parts
        current_pos = 1
        
        # First pass: create annotations with initial positions
        for part in text_parts:
            annotation = {
                "annotation_type": "header",
                "start_position": current_pos,
                "end_position": current_pos + base_spacing,  # Will be updated for parents
                "label": "section_header",
                "name": part['label'],
                "meta": {
                    "id": part.get('id'),
                    "location": part.get('location'),
                    "titles": part.get('titles', []),
                    "colophon": part.get('colophon'),
                    "part_index": part.get('part_index'),
                    "part_tree_index": part['part_tree_index'],
                    "instance_of": part.get('instance_of'),
                    "part_of": part.get('part_of'),
                    "root_instance": part.get('root_instance'),
                    "level": len(part['part_tree_index'].split('.')),
                    "parent": '.'.join(part['part_tree_index'].split('.')[:-1]) if '.' in part['part_tree_index'] else None
                }
            }
            annotations.append(annotation)
            current_pos += base_spacing
        
        # Build tree structure
        tree = self._build_annotation_tree(annotations)
        
        # Second pass: establish relationships and adjust positions bottom-up
        # Process deepest nodes first
        sorted_indices = sorted(
            range(len(annotations)),
            key=lambda i: (-len(annotations[i]['meta']['part_tree_index'].split('.')), annotations[i]['meta']['part_tree_index'])
        )
        
        for idx in sorted_indices:
            anno = annotations[idx]
            tree_idx = anno['meta']['part_tree_index']
            
            # Find direct parent
            if anno['meta']['parent']:
                for j, other in enumerate(annotations):
                    if other['meta']['part_tree_index'] == anno['meta']['parent']:
                        anno['meta']['parent_id'] = j
                        anno['meta']['relationship'] = 'child'
                        other['meta']['relationship'] = 'parent'
                        break
            
            # Adjust positions for all ancestors
            if anno['meta']['parent']:
                current = anno['meta']['parent']
                while current:
                    # Find the parent annotation
                    parent_idx = next(
                        (i for i, a in enumerate(annotations) if a['meta']['part_tree_index'] == current),
                        None
                    )
                    if parent_idx is not None:
                        parent = annotations[parent_idx]
                        # Update parent's end position to cover this descendant
                        parent['end_position'] = max(
                            parent['end_position'],
                            anno['end_position']
                        )
                        # Move up to grandparent
                        current = parent['meta']['parent']
                    else:
                        break
        
        # Create content string
        max_pos = max(a['end_position'] for a in annotations)
        content = "_" * max_pos if not content else content + "_" * (max_pos - len(content))
        
        # Sort annotations by tree index
        annotations.sort(key=lambda x: [int(i) for i in x['meta']['part_tree_index'].split('.')])
        
        return {
            "text": {
                "title": title,
                "content": content,
                "language": language,
                "source": "Buddhist Digital Resource Center"
            },
            "annotations": annotations
        }
    # Convert to annotation format
    def _filter_annotations(self, output_data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter annotations to keep only specific fields and meta.
        
        Args:
            output_data: Original output dictionary with text and annotations
            
        Returns:
            Dictionary with filtered annotations containing required fields and meta
        """
        filtered_output = {
            "text": output_data["text"],
            "annotations": []
        }
        
        required_fields = {
            "annotation_type",
            "start_position",
            "end_position",
            "label",
            "name",
            "meta"  # Keep meta field to preserve additional data
        }
        
        for annotation in output_data["annotations"]:
            filtered_annotation = {k: v for k, v in annotation.items() if k in required_fields}
            filtered_output["annotations"].append(filtered_annotation)
        
        return filtered_output

    def process_outline(self, outline_id: str, output_dir: Optional[Path] = None) -> Dict[str, Any]:
        """
        Process an outline ID to extract text parts and convert to annotation format
        
        Args:
            outline_id: BDRC outline ID
            output_dir: Optional directory to save output JSON
            
        Returns:
            Dictionary containing text and annotations
        """
        try:

            # Get outline graph
            logger.info(f"Fetching graph for outline {outline_id}...")
            graph = self.scraper.get_outline_graph(outline_id)
            if not graph:
                raise ValueError(f"Could not fetch graph for outline {outline_id}")

            logger.info(f"Got graph with {len(list(graph))} triples")

            # Extract text parts
            logger.info("Extracting text parts...")
            text_parts = self.scraper.get_ordered_text_parts(graph)
            logger.info(f"Found {len(text_parts)} text parts")

            title = self.scraper.get_page_title(graph)

            output = self._convert_to_annotation_format(text_parts, title)
            output_format_annotations = self._filter_annotations(output)

            # Save full version to cache directory
            cache_file = self.cache_dir / f"{outline_id}_annotations_full.json"
            logger.info(f"Saving full version to cache: {cache_file}...")
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(output, f, ensure_ascii=False, indent=4)

            # Save filtered version to cache
            filtered_cache_file = self.cache_dir / f"{outline_id}_annotations.json"
            logger.info(f"Saving filtered version to cache: {filtered_cache_file}...")
            with open(filtered_cache_file, 'w', encoding='utf-8') as f:
                json.dump(output_format_annotations, f, ensure_ascii=False, indent=4)

            # Save to output directory if specified
            if output_dir:
                output_dir = Path(output_dir)
                output_dir.mkdir(exist_ok=True)
                
                # Save filtered version
                filtered_output_file = output_dir / f"{outline_id}.json"
                logger.info(f"Saving filtered version to {filtered_output_file}...")
                with open(filtered_output_file, "w", encoding="utf-8") as f:
                    json.dump(output_format_annotations, f, ensure_ascii=False, indent=4)

            return output

        except Exception as e:
            logger.exception(f"Error processing outline {outline_id}: {e}")
            raise

def main():
    # Initialize scraper and processor
    scraper = BdrcScraper()
    processor = TextPartProcessor(scraper)

    # Process outline
    outline_id = "O2DB80610"
    try:
        output_dir = Path("outputs")  # or any directory you want
        output = processor.process_outline(outline_id, output_dir=output_dir)
        logger.info(f"Successfully processed outline {outline_id}")
        return output
    except Exception as e:
        logger.error(f"Failed to process outline {outline_id}: {e}")
        return None

if __name__ == "__main__":
    main()