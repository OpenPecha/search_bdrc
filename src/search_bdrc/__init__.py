import re
from multiprocessing import Pool
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from tqdm import tqdm

text = "ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པའི་སྙིང་པོ།"


class Scraper:
    def __init__(self):
        self.instance_id_regex = r"<a\shref=\"/show/bdr:([A-Z0-9_]+)\?"

    @staticmethod
    def scrape(args):
        input, page_no = args
        url = f"https://library.bdrc.io/osearch/search?q={input}&uilang=bo&page={page_no}"  # noqa
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle")  # waits for JS to load
            content = page.content()
            browser.close()
        return page_no, content

    def run(self, input: str, no_of_page: int, processes: int = 4):
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
        ids = re.findall(self.instance_id_regex, text)
        # Remove duplicate
        ids = list(set(ids))
        return ids
