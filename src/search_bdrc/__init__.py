import json
import time
from multiprocessing import Pool
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from tqdm import tqdm

text = "ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པའི་སྙིང་པོ།"


def scrape_and_save(page_no: int):
    # Check if the page no is already scraped
    if Path(f"{text}/{page_no}.txt").exists():
        return None

    url = f"https://library.bdrc.io/osearch/search?q={text}&uilang=bo&page={page_no}"  # noqa
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")  # waits for JS to load
        content = page.content()
        browser.close()
    Path(f"{text}").mkdir(exist_ok=True)
    Path(f"{text}/{page_no}.txt").write_text(content, encoding="utf-8")


if __name__ == "__main__":
    page_numbers = list(range(1, 45))
    with Pool(processes=1) as pool:
        list(
            tqdm(
                pool.imap_unordered(scrape_and_save, page_numbers),
                total=len(page_numbers),
                desc="Scraping pages from bdrc",
            )
        )
