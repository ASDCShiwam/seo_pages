from elasticsearch import Elasticsearch, helpers
from loguru import logger

from .config import ELASTICSEARCH_URL, ELASTICSEARCH_INDEX


class Indexer:
    def __init__(self) -> None:
        self.es = Elasticsearch(ELASTICSEARCH_URL)

    def index_document(self, doc: dict) -> None:
        logger.info(f"Indexing {doc.get('url')}")
        self.es.index(index=ELASTICSEARCH_INDEX, document=doc)

    def bulk_index(self, docs: list[dict]) -> None:
        actions = [
            {
                "_index": ELASTICSEARCH_INDEX,
                "_id": d["url"],
                "_source": d,
            }
            for d in docs
        ]
        helpers.bulk(self.es, actions)
