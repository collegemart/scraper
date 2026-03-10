BOT_NAME = "bharatfare"

SPIDER_MODULES = ["bharatfare.spiders"]
NEWSPIDER_MODULE = "bharatfare.spiders"

# --- TLS Fingerprint Impersonation (scrapy-impersonate) ---
DOWNLOAD_HANDLERS = {
    "http": "scrapy_impersonate.ImpersonateDownloadHandler",
    "https": "scrapy_impersonate.ImpersonateDownloadHandler",
}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# --- Anti-detection ---
ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS = 32
CONCURRENT_REQUESTS_PER_DOMAIN = 8
DOWNLOAD_DELAY = 0.25

# Fake User-Agent rotation providers
FAKEUSERAGENT_PROVIDERS = [
    'scrapy_fake_useragent.providers.FakeUserAgentProvider',
    'scrapy_fake_useragent.providers.FakerProvider',
]

COOKIES_ENABLED = True

# --- Downloader middlewares ---
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
    'bharatfare.middlewares.BrowserHeadersMiddleware': 401,
    'bharatfare.middlewares.ProxyRotationMiddleware': 350,
    'bharatfare.middlewares.SmartRetryMiddleware': 550,
}

# --- Proxy config (add your proxies here or via environment variable) ---
PROXY_LIST = []  # e.g. ['http://user:pass@host:port', ...]
PROXY_LIST_ENV = 'SCRAPER_PROXIES'  # or set this env var with comma-separated proxies

# --- AutoThrottle ---
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.5
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 8.0

# --- Retry ---
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# --- Pipelines ---
ITEM_PIPELINES = {
    'bharatfare.pipelines.CleaningPipeline': 100,
    'bharatfare.pipelines.DropIncompletePipeline': 200,
    'bharatfare.pipelines.DeduplicationPipeline': 300,
    'bharatfare.pipelines.TravelLeadScoringPipeline': 400,
}

# --- HTTP Cache (enable during dev/test to avoid re-downloading) ---
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 86400  # 24 hours
# HTTPCACHE_DIR = 'httpcache'

# --- Safety limits ---
CLOSESPIDER_ERRORCOUNT = 100

# --- Feed export ---
FEED_EXPORT_ENCODING = 'utf-8'

# CSV columns: 16 outreach-ready columns for email + LinkedIn + WhatsApp automation
FEED_EXPORT_FIELDS = [
    'company_name',               # Company Name
    'company_website',            # Website
    'sector',                     # Industry (IT, Consulting, Pharma, etc.)
    'company_size',               # Employee Size
    'hq_city',                    # HQ
    'office_locations',           # Office Locations
    'contact_email',              # Official Email
    'contact_person',             # Contact Person Name
    'designation',                # Role (HR/Admin/Procurement/Travel)
    'contact_linkedin',           # LinkedIn Profile
    'phone',                      # Phone
    'linkedin_company_url',       # LinkedIn Company URL
    'company_revenue',            # Revenue Range
    'has_international_hiring',   # Hiring Internationally (Y/N)
    'estimated_travel_frequency', # Estimated Travel Frequency
    'source_url',                 # Source URL
]

# --- SerpAPI key for Google Maps spider (optional) ---
SERPAPI_KEY = ''  # Set via environment variable SERPAPI_KEY or directly here

# --- Misc ---
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
