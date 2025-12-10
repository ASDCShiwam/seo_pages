from elasticsearch import Elasticsearch

from .config import CLICK_EVENTS_INDEX, ELASTICSEARCH_INDEX

SEO_INDEX_BODY = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "url": {"type": "keyword"},
            "canonical_url": {"type": "keyword"},
            "title": {
                "type": "text",
                "fields": {"raw": {"type": "keyword", "ignore_above": 256}},
            },
            "content": {"type": "text"},
            "summary": {"type": "text"},
            "h1": {"type": "text"},
            "headings_h1": {"type": "text"},
            "headings_h2": {"type": "text"},
            "headings_h3": {"type": "text"},
            "meta_description": {"type": "text"},
            "meta_keywords": {"type": "text"},
            "lang": {"type": "keyword"},
            "crawled_at": {"type": "date"},
            "content_length": {"type": "integer"},
            "clicks_total": {"type": "long"},
            "recent_clicks": {"type": "double"},
            "last_clicked_at": {"type": "date"},
            "last_clicked_at_ms": {"type": "long"},
            "ranking_score": {"type": "double"},
        }
    },
}

CLICK_LOG_INDEX_BODY = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "url": {"type": "keyword"},
            "user_id": {"type": "keyword"},
            "clicked_at": {"type": "date"},
            "metadata": {"type": "object"},
        }
    },
}


def ensure_indices(es: Elasticsearch) -> None:
    """Create indices with mappings if they do not exist."""
    if not es.indices.exists(index=ELASTICSEARCH_INDEX):
        es.indices.create(index=ELASTICSEARCH_INDEX, **SEO_INDEX_BODY)

    if not es.indices.exists(index=CLICK_EVENTS_INDEX):
        es.indices.create(index=CLICK_EVENTS_INDEX, **CLICK_LOG_INDEX_BODY)


__all__ = [
    "CLICK_LOG_INDEX_BODY",
    "SEO_INDEX_BODY",
    "ensure_indices",
]
