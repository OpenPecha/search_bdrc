"""
This module provides a BdrcScraper class for extracting instance IDs from the BDRC library search results.
It uses Playwright for web scraping and multiprocessing for parallel page retrieval.
"""
import re
import json
import requests
from typing import List, Optional
from rdflib import ConjunctiveGraph, Graph, Namespace, URIRef, RDF, RDFS
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright
from tqdm import tqdm
from search_bdrc.config import get_logger
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
logger = get_logger(__name__)


class BdrcScraper:
    def __init__(self):
        """
        Initialize the BdrcScraper with a regex pattern for extracting instance IDs from HTML content.
        """
        self.instance_id_regex = r"<a\shref=\"/show/bdr:([A-Z0-9_]+)\?"

    @staticmethod
    def scrape(args):
        """
        Scrape a single page of BDRC search results.
        """
        input, page_no = args
        url = f"https://library.bdrc.io/osearch/search?q={input}&uilang=bo&page={page_no}"  # noqa
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, wait_until="networkidle")  # waits for JS to load
                content = page.content()
                browser.close()
            return page_no, content
        except Exception as e:
            logger.error(f"Error scraping page {page_no}: {e}")
            return page_no, ""

    def run_scrape(self, input: str, no_of_page: int, processes: int = 4):
        """
        Scrape multiple pages of BDRC search results in parallel.
        """
        logger.info(
            f"Starting parallel scrape for '{input}' across {no_of_page} pages with {processes} processes."
        )
        page_args = [(input, page_no) for page_no in range(1, no_of_page + 1)]
        res = {}
        with Pool(processes=processes) as pool:
            for page_no, content in tqdm(
                pool.imap_unordered(BdrcScraper.scrape, page_args),
                total=no_of_page,
                desc="Scraping pages from bdrc",
            ):
                res[page_no] = content
        logger.info(f"Completed scraping {no_of_page} pages.")
        return res

    def extract_instance_ids(self, text: str) -> list[str]:
        """
        Extract unique instance IDs from the provided HTML content.
        """
        logger.debug("Extracting instance IDs from HTML content.")
        ids = re.findall(self.instance_id_regex, text)
        # Remove duplicate
        ids = list(set(ids))
        logger.info(f"Extracted {len(ids)} unique instance IDs.")
        return ids

    def get_related_instance_ids(
        self, input: str, no_of_page: int, processes: int = 4
    ) -> list[str]:
        """
        Scrape multiple pages and extract all unique instance IDs from the results.
        """
        logger.info(
            f"Getting related instance IDs for query '{input}' across {no_of_page} pages."
        )
        scraped = self.run_scrape(input, no_of_page, processes)

        ids = []
        for _, content in scraped.items():
            ids_in_page = self.extract_instance_ids(content)
            ids.extend(ids_in_page)

        # remove duplicates
        ids = list(set(ids))
        logger.info(f"Total unique instance IDs found: {len(ids)}")
        return ids

    def get_related_instance_ids_from_work(self, work_id: str) -> list[str]:
        metadata = self.get_instance_metadata(work_id)

        if not metadata:
            return []

        instance_ids = []
        for subj, pred, obj in metadata:
            if str(pred) == "http://purl.bdrc.io/ontology/core/workHasInstance":
                instance_link = str(obj)
                instance_id = instance_link.split("/")[-1]
                instance_ids.append(instance_id)

        # remove duplicates
        instance_ids = list(set(instance_ids))
        return instance_ids

    @staticmethod
    def get_instance_metadata(instance_id: str, json_format: bool = False):
        if json_format:
            url = f"https://purl.bdrc.io/resource/{instance_id}.jsonld"  # noqa
            headers = {"Accept": "application/ld+json"}
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                try:
                    return response.json()
                except Exception as e:
                    logger.error(
                        f"Failed to parse JSON for instance {instance_id}: {e}"
                    )
                    return None
            else:
                logger.error(
                    f"Failed to retrieve JSON metadata from instance {instance_id}: {response.status_code}"
                )
                return None
        else:
            url = f"https://ldspdi-dev.bdrc.io/resource/{instance_id}.ttl"  # noqa
            headers = {"Accept": "text/turtle"}  # Requesting Turtle format
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.text
                g = Graph()
                g.parse(data=data, format="turtle")
                return g
            else:
                logger.error(
                    f"Failed to retrieve metadata from instance {instance_id}: {response.status_code}"
                )
                return None

    def get_work_of_instance(self, instance_id: str):
        metadata = self.get_instance_metadata(instance_id)
        if not metadata:
            return []

        works = []
        for subj, pred, obj in metadata:
            if str(pred) == "http://purl.bdrc.io/ontology/core/instanceOf":
                work_link = str(obj)
                work_id = work_link.split("/")[-1]
                works.append(work_id)

        # remove duplicates
        works = list(set(works))
        return works


    def get_outline_of_instance(self, instance_id: str) -> list[str]:
        """Get outline IDs for a given instance.
        
        Args:
            instance_id: The ID of the instance (e.g. MW19999)
            
        Returns:
            List of outline IDs associated with the instance
        """
        metadata = self.get_instance_metadata(instance_id)
        if not metadata:
            print(f"No metadata found for instance {instance_id}")
            return []

        # Save metadata to file
        output_dir = Path("outputs")
        output_dir.mkdir(exist_ok=True)
        meta_file = output_dir / f"{instance_id}_metadata.ttl"
        meta_file.write_text(metadata.serialize(format='turtle'))
        print(f"Metadata saved to {meta_file}")

        # Get outlines using BDO namespace
        BDO = Namespace("http://purl.bdrc.io/ontology/core/")
        outlines = []

        # Look for hasOutline predicate
        for _, _, obj in metadata.triples((None, BDO.hasOutline, None)):
            outline_id = str(obj).split("/")[-1]
            outlines.append(outline_id)
            print(f"Found outline: {outline_id}")

        if not outlines:
            print(f"No outlines found for instance {instance_id}")
            return []

        # Remove duplicates while preserving order
        return list(dict.fromkeys(outlines))

    def get_outline_metadata(self, outline_id: str):
        metadata = self.get_instance_metadata(outline_id)
        if not metadata:
            return None
        
        return metadata
    
    def get_outline_graph(self, outline_id: str):
        url = f"https://purl.bdrc.io/graph/{outline_id}.trig"  # noqa
        response = requests.get(url, headers={"Accept": "text/trig"})
        if response.status_code != 200:
            logger.error(f"Error fetching {url}: {response.status_code}")
            return None
        
        # Create and configure graph
        g = ConjunctiveGraph()
        g.bind('bdr', 'http://purl.bdrc.io/resource/')
        g.bind('bdo', 'http://purl.bdrc.io/ontology/core/')
        g.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
        
        try:
            g.parse(data=response.text, format="trig")
            return g
        except Exception as e:
            logger.warning(f"Failed to parse as trig: {e}, trying turtle format")
            g = Graph()
            g.parse(data=response.text, format="turtle")
            return g

    def get_page_title(self, graph: Graph) -> Optional[str]:
        BDO = Namespace("http://purl.bdrc.io/ontology/core/")
        RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
        
        # Find subjects that are of type TitlePageTitle
        for subject in graph.subjects(RDF.type, BDO.TitlePageTitle):
            # Get their rdfs:label
            for label in graph.objects(subject, RDFS.label):
                return str(label)
        return None

    def get_ordered_text_parts(self, graph: Graph) -> list[dict]:
        """
        Get all text parts from the graph ordered by their tree index.
        
        Args:
            graph: RDF graph containing the outline structure
            
        Returns:
            List of dictionaries containing text part information including:
            - id: text part ID
            - label: skos:prefLabel
            - titles: list of title objects with type and label
            - colophon: text colophon if available
            - location: detailed content location info
            - part_index: numerical index
            - part_tree_index: hierarchical index
            - instance_of: work ID this is an instance of
            - part_of: parent section ID
        """
        BDO = Namespace("http://purl.bdrc.io/ontology/core/")
        BDR = Namespace("http://purl.bdrc.io/resource/")
        SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
        RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
        RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        
        
        
        def process_part(subject):
            """Process a single part and extract its information"""
            part_info = {
                'id': str(subject).split('/')[-1],
                'label': None,
                'location': None,
                'titles': [],
                'colophon': None,
                'part_index': None,
                'part_tree_index': None,
                'instance_of': None,
                'part_of': None,
                'root_instance': None
            }
            
            # Get skos:prefLabel
            for label in graph.objects(subject, SKOS.prefLabel):
                part_info['label'] = str(label)
            
            # Get content location
            for _, _, loc_node in graph.triples((subject, BDO.contentLocation, None)):
                location_info = {'id': str(loc_node).split('/')[-1]}
                for pred, obj in graph.predicate_objects(loc_node):
                    pred_name = str(pred).split('/')[-1]
                    if 'contentLocation' in pred_name and pred_name != 'contentLocation':
                        key = pred_name.replace('contentLocation', '').lower()
                        try:
                            location_info[key] = int(obj)
                        except ValueError:
                            location_info[key] = str(obj).split('/')[-1] if '/' in str(obj) else str(obj).split('#')[-1]
                part_info['location'] = location_info
            
            # Get titles
            for title_node in graph.objects(subject, BDO.hasTitle):
                title_info = {
                    'id': str(title_node).split('/')[-1],
                    'type': None,
                    'label': None
                }
                # Get title type
                for title_type in graph.objects(title_node, RDF.type):
                    title_info['type'] = str(title_type).split('/')[-1]
                # Get title label
                for label in graph.objects(title_node, SKOS.prefLabel):
                    title_info['label'] = str(label)
                if not title_info['label']:
                    for label in graph.objects(title_node, RDFS.label):
                        title_info['label'] = str(label)
                part_info['titles'].append(title_info)
            
            # Get all other properties
            part_info['colophon'] = next((str(col) for col in graph.objects(subject, BDO.colophon)), None)
            part_info['part_index'] = next((int(idx) for idx in graph.objects(subject, BDO.partIndex)), None)
            part_info['part_tree_index'] = next((str(idx) for idx in graph.objects(subject, BDO.partTreeIndex)), None)
            part_info['instance_of'] = next((str(work).split('/')[-1] for work in graph.objects(subject, BDO.instanceOf)), None)
            part_info['part_of'] = next((str(parent).split('/')[-1] for parent in graph.objects(subject, BDO.partOf)), None)
            part_info['root_instance'] = next((str(root).split('/')[-1] for root in graph.objects(subject, BDO.inRootInstance)), None)
            
            return part_info

        # Collect all parts
        text_parts = []
        
        # Get both text parts and table of contents parts
        for part_type in [BDR.PartTypeText, BDR.PartTypeTableOfContent, BDR.PartTypeVolume, BDR.PartTypeSection, BDR.PartTypeChapter]:
            for s, _, _ in graph.triples((None, BDO.partType, part_type)):
                text_parts.append(process_part(s))

        # Sort by part_tree_index
        return sorted(text_parts, key=lambda x: (x['part_tree_index'] or ''))


