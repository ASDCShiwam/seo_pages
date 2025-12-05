import asyncio
import time
from urllib.parse import urljoin, urlparse

import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
from loguru import logger

from .config import (
    SEED_URLS,
    REQUEST_TIMEOUT,
    USER_AGENT,
    CRAWL_MAX_PAGES,
    CRAWL_SAME_DOMAIN_ONLY,
    CRAWL_CONCURRENCY,
    CRAWL_MAX_RETRIES,
    CRAWL_RETRY_BACKOFF,
)


class Crawler:
    def __init__(self):
        self.visited: set[str] = set()
        self.enqueued: set[str] = set()
        self.pages_crawled = 0
        self.url_lock = asyncio.Lock()
        self.pages_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        self.start_time = time.monotonic()

    def same_domain(self, base_url: str, new_url: str) -> bool:
        if not CRAWL_SAME_DOMAIN_ONLY:
            return True
        return urlparse(base_url).netloc == urlparse(new_url).netloc

    def normalize_url(self, base: str, link: str | None) -> str | None:
        if not link:
            return None
        link = link.strip()
        if link.startswith(("mailto:", "tel:", "javascript:")):
            return None
        full = urljoin(base, link)
        parsed = urlparse(full)
        return parsed._replace(fragment="").geturl()

    def extract_links(self, html: str, base_url: str):
        soup = BeautifulSoup(html, "lxml")
        for a in soup.find_all("a", href=True):
            new_url = self.normalize_url(base_url, a["href"])
            if new_url and self.same_domain(base_url, new_url):
                yield new_url

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        headers = {"User-Agent": USER_AGENT}
        last_error: Exception | None = None

        for attempt in range(1, CRAWL_MAX_RETRIES + 1):
            try:
                logger.info(f"Fetching: {url} (attempt {attempt}/{CRAWL_MAX_RETRIES})")
                async with session.get(url, headers=headers, allow_redirects=True) as resp:
                    resp.raise_for_status()
                    return await resp.text()
            except Exception as ex:
                last_error = ex
                logger.warning(f"Error fetching {url} (attempt {attempt}): {ex}")
                if attempt < CRAWL_MAX_RETRIES:
                    await asyncio.sleep(CRAWL_RETRY_BACKOFF * attempt)

        raise last_error or RuntimeError(f"Failed to fetch {url}")

    async def _mark_enqueued(self, url: str) -> bool:
        async with self.url_lock:
            if url in self.visited or url in self.enqueued or self.stop_event.is_set():
                return False
            self.enqueued.add(url)
            return True

    async def _mark_visited(self, url: str) -> None:
        async with self.url_lock:
            self.visited.add(url)

    async def _increment_pages(self) -> int | None:
        async with self.pages_lock:
            if self.pages_crawled >= CRAWL_MAX_PAGES:
                self.stop_event.set()
                return None

            self.pages_crawled += 1
            if self.pages_crawled >= CRAWL_MAX_PAGES:
                self.stop_event.set()
            return self.pages_crawled

    def _log_speed(self) -> None:
        elapsed = time.monotonic() - self.start_time
        if elapsed <= 0:
            return
        speed = self.pages_crawled / elapsed
        logger.info(
            f"Crawl speed: {speed:.2f} pages/sec "
            f"({self.pages_crawled} pages in {elapsed:.2f}s)"
        )

    async def worker(
        self,
        session: aiohttp.ClientSession,
        queue: asyncio.Queue[str],
        results: asyncio.Queue[tuple[str, str]],
    ) -> None:
        while True:
            try:
                url = await queue.get()
            except asyncio.CancelledError:
                break

            try:
                if self.stop_event.is_set():
                    continue

                html = await self.fetch(session, url)
                page_number = await self._increment_pages()
                if page_number is None:
                    continue

                await self._mark_visited(url)
                await results.put((url, html))
                self._log_speed()

                if not self.stop_event.is_set():
                    for link in self.extract_links(html, url):
                        if await self._mark_enqueued(link):
                            await queue.put(link)
            except asyncio.CancelledError:
                break
            except Exception as ex:
                logger.error(f"Error processing {url}: {ex}")
            finally:
                queue.task_done()

    async def crawl(self):
        queue: asyncio.Queue[str] = asyncio.Queue()
        results: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

        for seed in SEED_URLS:
            if await self._mark_enqueued(seed):
                await queue.put(seed)

        timeout = ClientTimeout(total=REQUEST_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            workers = [
                asyncio.create_task(self.worker(session, queue, results))
                for _ in range(CRAWL_CONCURRENCY)
            ]

            queue_join: asyncio.Task[None] | None = None

            try:
                queue_join = asyncio.create_task(queue.join())

                while True:
                    if queue_join.done() and results.empty():
                        break

                    try:
                        item = await asyncio.wait_for(results.get(), timeout=0.2)
                    except asyncio.TimeoutError:
                        continue

                    yield item
            finally:
                for worker in workers:
                    worker.cancel()
                await asyncio.gather(*workers, return_exceptions=True)

                if queue_join and not queue_join.done():
                    queue_join.cancel()
