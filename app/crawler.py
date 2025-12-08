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
from .robots_manager import RobotsManager


class Crawler:
    def __init__(
        self,
        *,
        seed_urls: list[str] | None = None,
        request_timeout: float = REQUEST_TIMEOUT,
        concurrency: int = CRAWL_CONCURRENCY,
        max_pages: int = CRAWL_MAX_PAGES,
        same_domain_only: bool = CRAWL_SAME_DOMAIN_ONLY,
        max_retries: int = CRAWL_MAX_RETRIES,
        retry_backoff: float = CRAWL_RETRY_BACKOFF,
    ):
        self.seed_urls = seed_urls or SEED_URLS
        self.request_timeout = request_timeout
        self.concurrency = max(1, concurrency)
        self.max_pages = max_pages
        self.same_domain_only = same_domain_only
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff

        self.visited: set[str] = set()
        self.enqueued: set[str] = set()
        self.pages_crawled = 0
        self.url_lock = asyncio.Lock()
        self.pages_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        self.start_time = time.monotonic()
        self.fetch_semaphore = asyncio.Semaphore(self.concurrency)
        self.robots_manager = RobotsManager(USER_AGENT)

    def same_domain(self, base_url: str, new_url: str) -> bool:
        if not self.same_domain_only:
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

        for attempt in range(1, self.max_retries + 1):
            try:
                async with self.fetch_semaphore:
                    logger.info(
                        f"Fetching: {url} (attempt {attempt}/{self.max_retries})"
                    )
                    async with session.get(
                        url, headers=headers, allow_redirects=True
                    ) as resp:
                        resp.raise_for_status()
                        return await resp.text()
            except Exception as ex:
                last_error = ex
                logger.warning(f"Error fetching {url} (attempt {attempt}): {ex}")
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_backoff * attempt)

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
            if self.pages_crawled >= self.max_pages:
                self.stop_event.set()
                return None

            self.pages_crawled += 1
            if self.pages_crawled >= self.max_pages:
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

    async def _log_speed_periodically(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(1)
            self._log_speed()

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

                await self.robots_manager.ensure_rules(session, url)
                if not self.robots_manager.is_allowed(url):
                    logger.info(f"Blocked by robots.txt: {url}")
                    await self._mark_visited(url)
                    continue

                await self.robots_manager.wait_for_crawl_delay(url)

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
        self.start_time = time.monotonic()
        log_task: asyncio.Task[None] | None = None
        queue: asyncio.Queue[str] = asyncio.Queue()
        results: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

        for seed in self.seed_urls:
            if await self._mark_enqueued(seed):
                await queue.put(seed)

        timeout = ClientTimeout(total=self.request_timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            workers = [
                asyncio.create_task(self.worker(session, queue, results))
                for _ in range(self.concurrency)
            ]

            queue_join: asyncio.Task[None] | None = None

            try:
                queue_join = asyncio.create_task(queue.join())
                log_task = asyncio.create_task(self._log_speed_periodically())

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

                if log_task:
                    log_task.cancel()
                    await asyncio.gather(log_task, return_exceptions=True)

                self._log_speed()
