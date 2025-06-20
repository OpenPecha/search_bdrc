"""
This module provides a Scraper class for extracting instance IDs from the BDRC library search results.
It uses Playwright for web scraping and multiprocessing for parallel page retrieval.
"""
import re
from multiprocessing import Pool
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from tqdm import tqdm


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
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle")  # waits for JS to load
            content = page.content()
            browser.close()
        return page_no, content

    def run_scrape(self, input: str, no_of_page: int, processes: int = 4):
        """
        Scrape multiple pages of BDRC search results in parallel.
        """

        page_args = [(input, page_no) for page_no in range(1, no_of_page + 1)]
        res = {}
        with Pool(processes=processes) as pool:
            for page_no, content in tqdm(
                pool.imap_unordered(Scraper.scrape, page_args),
                total=no_of_page,
                desc="Scraping pages from bdrc",
            ):
                res[page_no] = content
        return res

    def extract_instance_ids(self, text: str) -> list[str]:
        """
        Extract unique instance IDs from the provided HTML content.
        """
        ids = re.findall(self.instance_id_regex, text)
        # Remove duplicate
        ids = list(set(ids))
        return ids

    def get_related_instance_ids(
        self, input: str, no_of_page: int, processes: int = 4
    ) -> list[str]:
        """
        Scrape multiple pages and extract all unique instance IDs from the results.
        """
        scraped = self.run_scrape(input, no_of_page, processes)

        ids = []
        for _, content in scraped.items():
            ids_in_page = self.extract_instance_ids(content)
            ids.extend(ids_in_page)

        # remove duplicates
        ids = list(set(ids))
        return ids


if __name__ == "__main__":
    scraper = Scraper()

    input = "ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པའི་སྙིང་པོ།"
    no_of_page = 44
    scraper.get_related_instance_ids(input, no_of_page)
