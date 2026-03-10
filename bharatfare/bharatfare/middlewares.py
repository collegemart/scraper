import random
import logging
from twisted.internet import reactor, defer

logger = logging.getLogger(__name__)


class RandomDelayMiddleware:
    """Adds random jitter to download delay to look more human.
    Uses Twisted's async deferLater instead of blocking time.sleep."""

    def __init__(self, delay_min, delay_max):
        self.delay_min = delay_min
        self.delay_max = delay_max

    @classmethod
    def from_crawler(cls, crawler):
        delay_min = crawler.settings.getfloat('RANDOM_DELAY_MIN', 2)
        delay_max = crawler.settings.getfloat('RANDOM_DELAY_MAX', 7)
        return cls(delay_min, delay_max)

    def process_request(self, request, spider):
        delay = random.uniform(self.delay_min, self.delay_max)
        d = defer.Deferred()
        reactor.callLater(delay, d.callback, None)
        return d


class BrowserHeadersMiddleware:
    """Rotates realistic browser headers per request."""

    HEADER_SETS = [
        {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,hi;q=0.6',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Ch-Ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        },
        {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Ch-Ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
        },
        {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-IN,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
        },
    ]

    REFERERS = [
        'https://www.google.com/',
        'https://www.google.co.in/',
        'https://www.bing.com/',
        '',
    ]

    def process_request(self, request, spider):
        headers = random.choice(self.HEADER_SETS)
        for key, value in headers.items():
            request.headers[key] = value
        if not request.headers.get('Referer'):
            referer = random.choice(self.REFERERS)
            if referer:
                request.headers['Referer'] = referer
        return None


class ProxyRotationMiddleware:
    """Rotates through a list of proxy servers.

    Configure in settings.py:
        PROXY_LIST = [
            'http://user:pass@proxy1.example.com:8080',
            'http://user:pass@proxy2.example.com:8080',
        ]

    Or load from environment variable:
        PROXY_LIST_ENV = 'SCRAPER_PROXIES'  # comma-separated proxy URLs
    """

    def __init__(self, proxies):
        self.proxies = proxies

    @classmethod
    def from_crawler(cls, crawler):
        import os
        proxies = crawler.settings.getlist('PROXY_LIST', [])
        env_var = crawler.settings.get('PROXY_LIST_ENV', '')
        if not proxies and env_var:
            env_proxies = os.environ.get(env_var, '')
            if env_proxies:
                proxies = [p.strip() for p in env_proxies.split(',') if p.strip()]
        if not proxies:
            logger.info("ProxyRotationMiddleware enabled but no proxies configured — passing through")
        return cls(proxies)

    def process_request(self, request, spider):
        if self.proxies and 'proxy' not in request.meta:
            proxy = random.choice(self.proxies)
            request.meta['proxy'] = proxy


class SmartRetryMiddleware:
    """Enhanced retry with exponential backoff for 429s and Cloudflare detection."""

    CLOUDFLARE_MARKERS = [
        b'Checking your browser',
        b'cf-browser-verification',
        b'challenges.cloudflare.com',
        b'Just a moment...',
        b'Attention Required',
    ]

    def __init__(self, max_retries, backoff_base):
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    @classmethod
    def from_crawler(cls, crawler):
        max_retries = crawler.settings.getint('SMART_RETRY_TIMES', 5)
        backoff_base = crawler.settings.getfloat('SMART_RETRY_BACKOFF_BASE', 2.0)
        return cls(max_retries, backoff_base)

    def process_response(self, request, response, spider):
        # Handle 429 Too Many Requests with exponential backoff
        if response.status == 429:
            return self._retry_with_backoff(request, spider, reason="429 Too Many Requests")

        # Detect Cloudflare challenge pages (often served as 403 or 503)
        if response.status in (403, 503):
            body = response.body[:2000]
            for marker in self.CLOUDFLARE_MARKERS:
                if marker in body:
                    logger.warning(
                        f"Cloudflare challenge detected for {request.url}"
                    )
                    return self._retry_with_backoff(
                        request, spider, reason="Cloudflare challenge"
                    )

        return response

    def _retry_with_backoff(self, request, spider, reason):
        retry_count = request.meta.get('smart_retry_count', 0)
        if retry_count >= self.max_retries:
            logger.error(
                f"Giving up on {request.url} after {retry_count} retries ({reason})"
            )
            from scrapy.http import HtmlResponse
            return HtmlResponse(url=request.url, body=b'', request=request)

        retry_count += 1
        delay = self.backoff_base ** retry_count + random.uniform(0, 1)
        logger.info(
            f"Smart retry #{retry_count} for {request.url} ({reason}), "
            f"backing off {delay:.1f}s"
        )

        new_request = request.copy()
        new_request.meta['smart_retry_count'] = retry_count
        new_request.dont_filter = True

        d = defer.Deferred()
        reactor.callLater(delay, d.callback, new_request)
        return d
