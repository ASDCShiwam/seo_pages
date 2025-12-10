from elasticsearch import Elasticsearch, helpers
from loguru import logger

from .config import ELASTICSEARCH_URL, ELASTICSEARCH_INDEX
from .index_schemas import ensure_indices
from .ranking import compute_ranking_score, current_time_ms


class Indexer:
    def __init__(self) -> None:
        self.es = Elasticsearch(ELASTICSEARCH_URL)
        ensure_indices(self.es)

    def _with_click_defaults(self, doc: dict) -> dict:
        doc.setdefault("clicks_total", 0)
        doc.setdefault("recent_clicks", 0.0)
        doc.setdefault("ranking_score", 0.0)
        doc.setdefault("last_clicked_at", None)
        doc.setdefault("last_clicked_at_ms", None)

        if doc.get("ranking_score") in (None, 0.0):
            now_ms = current_time_ms()
            doc["ranking_score"] = compute_ranking_score(
                clicks_total=doc.get("clicks_total", 0),
                recent_clicks=doc.get("recent_clicks", 0.0),
                last_clicked_at_ms=doc.get("last_clicked_at_ms"),
                now_ms=now_ms,
            )

        return doc

    def index_document(self, doc: dict) -> None:
        logger.info(f"Indexing {doc.get('url')}")
        prepared = self._with_click_defaults(doc)
        self.es.index(index=ELASTICSEARCH_INDEX, id=prepared.get("url"), document=prepared)

    def bulk_index(self, docs: list[dict]) -> None:
        actions = [
            {
                "_index": ELASTICSEARCH_INDEX,
                "_id": d.get("url"),
                "_source": self._with_click_defaults(d),
            }
            for d in docs
        ]
        helpers.bulk(self.es, actions)
