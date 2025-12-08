ELASTICSEARCH_URL = "http://localhost:9200"
ELASTICSEARCH_INDEX = "seo_pages"

# For now you can use some simple site to test.
# Later change to http://localhost/your_iis_site/, etc.
SEED_URLS = [
    "https://docs.djangoproject.com/en/5.2/",   # change this when you want intranet only
]

CRAWL_MAX_PAGES = 1000           # keep small for testing
CRAWL_SAME_DOMAIN_ONLY = False
REQUEST_TIMEOUT = 10
USER_AGENT = "OfflineSEOEngine/1.0"
CRAWL_CONCURRENCY = 5           # number of concurrent fetches
CRAWL_MAX_RETRIES = 3           # retry attempts per URL
CRAWL_RETRY_BACKOFF = 1.5       # seconds to wait between retries
