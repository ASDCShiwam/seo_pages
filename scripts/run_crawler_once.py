import asyncio
import os
import sys

# Allow "python scripts/run_crawler_once.py" to find app.*
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from app.crawler import Crawler
from app.indexer import Indexer
from app.parser_cleaner import parse_html
from loguru import logger


async def main():
    crawler = Crawler()
    indexer = Indexer()

    async for url, html in crawler.crawl():
        try:
            doc = parse_html(url, html)
            if doc["content_length"] < 50:
                logger.info(f"Skipping {url} – content too short")
                continue
            indexer.index_document(doc)
        except Exception as ex:
            logger.error(f"Error processing {url}: {ex}")


if __name__ == "__main__":
    asyncio.run(main())


#python .\scripts\run_crawler_once.py