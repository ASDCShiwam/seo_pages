ELASTICSEARCH_URL = "http://localhost:9200"
ELASTICSEARCH_INDEX = "seo_pages"

# For now you can use some simple site to test.
# Later change to http://localhost/your_iis_site/, etc.
SEED_URLS = [
    "https://delhi.gov.in/page/police",   # change this when you want intranet only
]

CRAWL_MAX_PAGES = 50            # keep small for testing
CRAWL_SAME_DOMAIN_ONLY = False
REQUEST_TIMEOUT = 20
USER_AGENT = "OfflineSEOEngine/1.0"
