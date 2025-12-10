from fastapi import FastAPI, Query
from pydantic import BaseModel
from elasticsearch import Elasticsearch
from fastapi.middleware.cors import CORSMiddleware

from .config import ELASTICSEARCH_URL, ELASTICSEARCH_INDEX

app = FastAPI(title="Offline SEO Search API")

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for testing / internal use
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

es = Elasticsearch(ELASTICSEARCH_URL)


class SearchResult(BaseModel):
    url: str
    title: str
    snippet: str
    score: float
    h1: str | None = None
    meta_description: str | None = None
    crawled_at: str | None = None
    content_length: int | None = None


@app.get("/search", response_model=list[SearchResult])
def search(q: str = Query(..., min_length=1), size: int = 10):
    body = {
        "query": {
            "multi_match": {
                "query": q,
                "fields": [
                    "title^3",
                    "h1^2",
                    "meta_description^1.5",
                    "content",
                ],
            }
        },
        "highlight": {
            "fields": {
                "content": {},
            }
        },
    }

    resp = es.search(index=ELASTICSEARCH_INDEX, body=body, size=size)
    results: list[SearchResult] = []

    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        highlight = hit.get("highlight", {}).get("content", [])
        snippet = highlight[0] if highlight else src.get("summary") or src.get("content", "")[:200]

        results.append(
            SearchResult(
                url=src.get("url", ""),
                title=src.get("title", "") or src.get("url", ""),
                snippet=snippet,
                score=float(hit["_score"]),
                h1=src.get("h1"),
                meta_description=src.get("meta_description"),
                crawled_at=src.get("crawled_at"),
                content_length=src.get("content_length"),
            )
        )

    return results

# Run:
# uvicorn app.search_api:app --host 0.0.0.0 --port 8000
