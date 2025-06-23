"""
This module provides a BdrcScraper class for extracting instance IDs from the BDRC library search results.
It uses Playwright for web scraping and multiprocessing for parallel page retrieval.
"""
import re
from multiprocessing import Pool

import requests
from playwright.sync_api import sync_playwright
from rdflib import Graph
from tqdm import tqdm

from search_bdrc.config import get_logger

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

        pass

    @staticmethod
    def get_instance_metadata(instance_id: str):
        url = f"https://purl.bdrc.io/resource/{instance_id}.ttl"  # noqa
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


if __name__ == "__main__":
    from search_bdrc.utils import read_json, write_json

    scraper = BdrcScraper()

    work_ids = read_json("works.json")

    instance_ids = []
    for work_id in tqdm(work_ids, desc="Getting instance ids from work id"):
        work_instance_ids = scraper.get_related_instance_ids_from_work(work_id)
        instance_ids.extend(work_instance_ids)

    write_json(instance_ids, "instance_ids.json")
