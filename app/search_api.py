import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

from elasticsearch import Elasticsearch
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import BaseModel

from .config import (
    CLICK_EVENTS_INDEX,
    DECAY_JOB_INTERVAL_SECONDS,
    ELASTICSEARCH_INDEX,
    ELASTICSEARCH_URL,
    RANKING_DECAY_PER_HOUR,
    RECENT_CLICK_DECAY_MULTIPLIER,
)
from .index_schemas import ensure_indices
from .ranking import compute_ranking_score, current_time_ms

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
decay_task: asyncio.Task | None = None


class SearchResult(BaseModel):
    url: str
    title: str
    snippet: str
    score: float
    ranking_score: Optional[float] = None
    h1: str | None = None
    meta_description: str | None = None
    crawled_at: str | None = None
    content_length: int | None = None


class ClickEvent(BaseModel):
    url: str
    user_id: str | None = None
    metadata: dict[str, Any] | None = None


CLICK_UPDATE_SCRIPT = """
if (ctx._source.clicks_total == null) { ctx._source.clicks_total = 0; }
if (ctx._source.recent_clicks == null) { ctx._source.recent_clicks = 0.0; }
long prevLast = ctx._source.containsKey('last_clicked_at_ms') && ctx._source.last_clicked_at_ms != null ? ctx._source.last_clicked_at_ms : params.now_ms;
ctx._source.clicks_total += 1;
ctx._source.recent_clicks += 1;
ctx._source.last_clicked_at_ms = params.now_ms;
ctx._source.last_clicked_at = params.now_iso;
double decayHours = (params.now_ms - prevLast) / 3_600_000.0;
double decay = decayHours * params.decay_per_hour;
ctx._source.ranking_score = Math.log(ctx._source.clicks_total + 1.0) + (ctx._source.recent_clicks * 0.7) - decay;
"""

DECAY_SCRIPT = """
if (ctx._source.recent_clicks == null) { ctx._source.recent_clicks = 0.0; }
if (ctx._source.clicks_total == null) { ctx._source.clicks_total = 0; }
ctx._source.recent_clicks = ctx._source.recent_clicks * params.recent_click_multiplier;
if (ctx._source.recent_clicks < 0.01) { ctx._source.recent_clicks = 0.0; }
long last = ctx._source.containsKey('last_clicked_at_ms') && ctx._source.last_clicked_at_ms != null ? ctx._source.last_clicked_at_ms : params.now_ms;
double decayHours = (params.now_ms - last) / 3_600_000.0;
double decay = decayHours * params.decay_per_hour;
ctx._source.ranking_score = Math.log(ctx._source.clicks_total + 1.0) + (ctx._source.recent_clicks * 0.7) - decay;
"""


def build_search_body(q: str) -> dict:
    return {
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
        "sort": [
            {"ranking_score": {"order": "desc", "missing": "_last"}},
            {"_score": {"order": "desc"}},
        ],
    }


async def decay_loop() -> None:
    while True:
        await asyncio.sleep(DECAY_JOB_INTERVAL_SECONDS)
        try:
            await asyncio.to_thread(apply_decay)
        except Exception as exc:  # pragma: no cover - background resilience
            logger.error(f"Decay loop failed: {exc}")


def apply_decay() -> None:
    now_ms = current_time_ms()
    logger.info("Applying ranking decay to all documents")
    es.update_by_query(
        index=ELASTICSEARCH_INDEX,
        body={
            "query": {"match_all": {}},
            "script": {
                "source": DECAY_SCRIPT,
                "lang": "painless",
                "params": {
                    "recent_click_multiplier": RECENT_CLICK_DECAY_MULTIPLIER,
                    "now_ms": now_ms,
                    "decay_per_hour": RANKING_DECAY_PER_HOUR,
                },
            },
        },
        conflicts="proceed",
        refresh=True,
    )


@app.on_event("startup")
async def startup_event() -> None:
    ensure_indices(es)
    global decay_task
    if decay_task is None:
        decay_task = asyncio.create_task(decay_loop())


@app.get("/search", response_model=list[SearchResult])
def search(q: str = Query(..., min_length=1), size: int = 10):
    body = build_search_body(q)

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
                ranking_score=src.get("ranking_score"),
                h1=src.get("h1"),
                meta_description=src.get("meta_description"),
                crawled_at=src.get("crawled_at"),
                content_length=src.get("content_length"),
            )
        )

    return results


@app.post("/track_click")
async def track_click(event: ClickEvent) -> dict:
    ensure_indices(es)
    now_iso = datetime.now(timezone.utc).isoformat()
    now_ms = current_time_ms()

    await asyncio.to_thread(
        es.index,
        CLICK_EVENTS_INDEX,
        {
            "url": event.url,
            "user_id": event.user_id,
            "clicked_at": now_iso,
            "metadata": event.metadata or {},
        },
    )

    script = {
        "source": CLICK_UPDATE_SCRIPT,
        "lang": "painless",
        "params": {
            "now_ms": now_ms,
            "now_iso": now_iso,
            "decay_per_hour": RANKING_DECAY_PER_HOUR,
        },
    }

    upsert_doc = {
        "url": event.url,
        "title": event.url,
        "summary": "",
        "content": "",
        "clicks_total": 1,
        "recent_clicks": 1.0,
        "last_clicked_at": now_iso,
        "last_clicked_at_ms": now_ms,
        "ranking_score": compute_ranking_score(
            clicks_total=1,
            recent_clicks=1.0,
            last_clicked_at_ms=now_ms,
            now_ms=now_ms,
        ),
    }

    await asyncio.to_thread(
        es.update,
        ELASTICSEARCH_INDEX,
        event.url,
        script=script,
        upsert=upsert_doc,
        refresh="wait_for",
    )

    return {"status": "tracked", "url": event.url}


# Run:
# uvicorn app.search_api:app --host 0.0.0.0 --port 8000
