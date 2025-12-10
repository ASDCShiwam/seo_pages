ELASTICSEARCH_URL = "http://localhost:9200"
ELASTICSEARCH_INDEX = "seo_pages"
CLICK_EVENTS_INDEX = "seo_page_clicks"

# For now you can use some simple site to test.
# Later change to http://localhost/your_iis_site/, etc.
SEED_URLS = [
    "https://www.adda247.com/",   # change this when you want intranet only
]

CRAWL_MAX_PAGES =500         # keep small for testing
CRAWL_SAME_DOMAIN_ONLY = False
REQUEST_TIMEOUT = 10
USER_AGENT = "OfflineSEOEngine/1.0"
CRAWL_CONCURRENCY = 5           # number of concurrent fetches
CRAWL_MAX_RETRIES = 3           # retry attempts per URL
CRAWL_RETRY_BACKOFF = 1.5       # seconds to wait between retries

RANKING_DECAY_PER_HOUR = 0.05
RECENT_CLICK_DECAY_MULTIPLIER = 0.85
DECAY_JOB_INTERVAL_SECONDS = 1800
