"""
This module provides a Scraper class for extracting instance IDs from the BDRC library search results.
It uses Playwright for web scraping and multiprocessing for parallel page retrieval.
"""
import re
from multiprocessing import Pool
from pathlib import Path

from playwright.sync_api import sync_playwright
from tqdm import tqdm

from search_bdrc.config import get_logger

logger = get_logger(__name__)


class Scraper:
    def __init__(self):
        """
        Initialize the Scraper with a regex pattern for extracting instance IDs from HTML content.
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
                pool.imap_unordered(Scraper.scrape, page_args),
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


if __name__ == "__main__":
    scraper = Scraper()

    input = "ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པའི་སྙིང་པོ།"
    no_of_page = 44
    logger.info(f"Running scraper for input: {input}, pages: {no_of_page}")
    ids = scraper.get_related_instance_ids(input, no_of_page)
    logger.info(f"Scraping finished. Found {len(ids)} unique instance IDs.")
