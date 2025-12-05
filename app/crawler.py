from collections import deque
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from loguru import logger

from .config import (
    SEED_URLS,
    REQUEST_TIMEOUT,
    USER_AGENT,
    CRAWL_MAX_PAGES,
    CRAWL_SAME_DOMAIN_ONLY,
)


class Crawler:
    def __init__(self):
        self.visited = set()

    def same_domain(self, base_url: str, new_url: str) -> bool:
        if not CRAWL_SAME_DOMAIN_ONLY:
            return True
        return urlparse(base_url).netloc == urlparse(new_url).netloc

    def normalize_url(self, base: str, link: str | None) -> str | None:
        if not link:
            return None
        link = link.strip()
        # ignore mailto, tel, javascript
        if link.startswith(("mailto:", "tel:", "javascript:")):
            return None
        full = urljoin(base, link)
        parsed = urlparse(full)
        # remove fragments
        return parsed._replace(fragment="").geturl()

    def fetch(self, url: str) -> str:
        logger.info(f"Fetching: {url}")
        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text

    def extract_links(self, html: str, base_url: str):
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            new_url = self.normalize_url(base_url, a["href"])
            if new_url and self.same_domain(base_url, new_url):
                yield new_url

    def crawl(self):
        queue = deque(SEED_URLS)
        pages = 0

        while queue and pages < CRAWL_MAX_PAGES:
            url = queue.popleft()
            if url in self.visited:
                continue

            try:
                html = self.fetch(url)
            except Exception as ex:
                logger.error(f"Error fetching {url}: {ex}")
                continue

            self.visited.add(url)
            pages += 1

            # send page HTML to parser/indexer
            yield url, html

            try:
                for link in self.extract_links(html, url):
                    if link not in self.visited:
                        queue.append(link)
            except Exception as ex:
                logger.error(f"Error extracting links from {url}: {ex}")
