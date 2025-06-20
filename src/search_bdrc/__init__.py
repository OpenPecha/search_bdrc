import json
import time
from multiprocessing import Pool
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from tqdm import tqdm

text = "ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པའི་སྙིང་པོ།"


class Scraper:
    def scrape(self, input: str, page_no: int):
        url = f"https://library.bdrc.io/osearch/search?q={text}&uilang=bo&page={page_no}"  # noqa
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle")  # waits for JS to load
            content = page.content()
            browser.close()
        return content

    def run(self, input: str, no_of_page: int):
        res = {}
        for page_no in range(1, no_of_page + 1):
            res[page_no] = self.scrape(input, page_no)

        return res
