import json
import os
import re
from datetime import datetime
from urllib.parse import quote_plus

import scrapy
from scrapy_playwright.page import PageMethod

from bharatfare.items import LeadItem
from bharatfare.constants import GOOGLE_MAPS_QUERIES, keyword_to_sector


class GoogleMapsSpider(scrapy.Spider):
    """Dual-mode spider for Google Maps business listings.

    Mode 1 (preferred): Uses SerpAPI if SERPAPI_KEY env var is set.
    Mode 2 (fallback):  Uses Playwright to scrape Google Maps directly.
    """

    name = "googlemaps"
    allowed_domains = [
        "serpapi.com",
        "www.google.com",
        "google.com",
        "maps.google.com",
    ]

    custom_settings = {
        'DOWNLOAD_DELAY': 3.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DOWNLOAD_TIMEOUT': 45,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': False,
            'args': [
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--start-maximized',
            ],
        },
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 45000,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.serpapi_key = (
            kwargs.get('serpapi_key') or
            os.environ.get('SERPAPI_KEY', '') or
            self.settings.get('SERPAPI_KEY', '') if hasattr(self, 'settings') else ''
        )
        self.use_serpapi = bool(self.serpapi_key)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        if not spider.serpapi_key:
            spider.serpapi_key = crawler.settings.get('SERPAPI_KEY', '')
            spider.use_serpapi = bool(spider.serpapi_key)
        return spider

    def start_requests(self):
        if self.use_serpapi:
            self.logger.info("Using SerpAPI for Google Maps data")
            for query in GOOGLE_MAPS_QUERIES:
                yield from self._serpapi_request(query, start=0)
        else:
            self.logger.info(
                "No SERPAPI_KEY found. Using Playwright for Google Maps. "
                "Set SERPAPI_KEY env var for better results."
            )
            for query in GOOGLE_MAPS_QUERIES:
                yield from self._playwright_request(query)

    # ── SerpAPI Mode ──────────────────────────────────────────────────

    def _serpapi_request(self, query, start=0):
        url = (
            f"https://serpapi.com/search.json"
            f"?engine=google_maps"
            f"&q={quote_plus(query)}"
            f"&type=search"
            f"&start={start}"
            f"&api_key={self.serpapi_key}"
        )
        yield scrapy.Request(
            url=url,
            callback=self.parse_serpapi,
            cb_kwargs={'query': query, 'start': start},
            dont_filter=True,
        )

    def parse_serpapi(self, response, query, start):
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"SerpAPI returned invalid JSON for: {query}")
            return

        results = data.get('local_results', [])
        self.logger.info(
            f"[SerpAPI][{query}] start={start}: {len(results)} results"
        )

        for result in results:
            item = LeadItem()

            item['company_name'] = str(result.get('title', '')).strip()
            if not item['company_name']:
                continue

            item['source'] = 'googlemaps'
            item['source_url'] = (
                result.get('link') or result.get('place_id_search') or ''
            )
            item['profile_id'] = result.get('place_id', '')
            item['company_website'] = str(result.get('website', '')).strip()
            item['phone'] = str(result.get('phone', '')).strip()

            # Address
            address = result.get('address', '')
            item['address'] = str(address).strip()
            if address:
                parts = [p.strip() for p in str(address).split(',')]
                item['hq_city'] = parts[0] if parts else ''
                item['city'] = item['hq_city']

            item['supplier_rating'] = str(result.get('rating', '')).strip()
            item['business_type'] = str(result.get('type', '')).strip()

            item['sector'] = keyword_to_sector(query)
            item['industry'] = query
            item['search_keyword'] = query

            # Infer city from query
            loc_match = re.search(r'\bin\s+(.+)$', query, re.I)
            if loc_match:
                inferred_city = loc_match.group(1).strip()
                if not item.get('hq_city'):
                    item['hq_city'] = inferred_city
                    item['city'] = inferred_city

            # Infer country from city
            city_lower = (item.get('hq_city') or '').lower()
            if city_lower in ('london', 'manchester', 'birmingham'):
                item['hq_country'] = 'United Kingdom'
            elif city_lower in ('dubai', 'abu dhabi'):
                item['hq_country'] = 'UAE'
            else:
                item['hq_country'] = 'India'

            item['scraped_date'] = datetime.utcnow().isoformat()
            yield item

        # Paginate once (2 pages max)
        if len(results) >= 20 and start == 0:
            yield from self._serpapi_request(query, start=20)

    # ── Playwright Fallback Mode ──────────────────────────────────────

    def _playwright_request(self, query):
        url = f"https://www.google.com/maps/search/{quote_plus(query)}"
        yield scrapy.Request(
            url=url,
            callback=self.parse_google_maps,
            cb_kwargs={'query': query},
            meta={
                'playwright': True,
                'playwright_include_page': True,
                'playwright_page_methods': [
                    PageMethod("wait_for_load_state", "networkidle", timeout=20000),
                ],
            },
            errback=self.errback_close_page,
        )

    async def errback_close_page(self, failure):
        page = failure.request.meta.get('playwright_page')
        if page:
            try:
                await page.close()
            except Exception:
                pass
        self.logger.error(f"Request failed: {failure.request.url}")

    async def parse_google_maps(self, response, query):
        pw_page = response.meta.get('playwright_page')

        if pw_page:
            # Scroll the results sidebar to load more
            try:
                for _ in range(5):
                    await pw_page.evaluate('''
                        const el = document.querySelector('div[role="feed"]');
                        if (el) el.scrollTop = el.scrollHeight;
                    ''')
                    await pw_page.wait_for_timeout(2000)

                content = await pw_page.content()
                response = response.replace(body=content.encode('utf-8'))
            except Exception as e:
                self.logger.debug(f"Scroll failed: {e}")

            try:
                await pw_page.close()
            except Exception:
                pass

        # Google Maps result cards
        results = response.css(
            'div[role="feed"] > div > div[jsaction], '
            'div.Nv2PK, '
            'a[class*=hfpxzc]'
        )

        self.logger.info(
            f"[GoogleMaps][{query}] Found {len(results)} result cards"
        )

        for result in results:
            item = LeadItem()

            name = (
                result.css('[class*=fontHeadlineSmall]::text').get('') or
                result.css('.qBF1Pd::text').get('') or
                result.css('a::attr(aria-label)').get('')
            ).strip()

            if not name:
                continue

            item['company_name'] = name
            item['source'] = 'googlemaps'
            item['source_url'] = response.url

            item['supplier_rating'] = (
                result.css('[class*=MW4etd]::text').get('') or
                result.css('span[role=img]::attr(aria-label)').get('')
            ).strip()

            item['business_type'] = (
                result.css(
                    '[class*=fontBodyMedium] > div:nth-child(4)::text'
                ).get('') or ''
            ).strip()

            addr_parts = result.css(
                '[class*=fontBodyMedium] span::text'
            ).getall()
            item['address'] = ', '.join(
                p.strip() for p in addr_parts if p.strip() and len(p.strip()) > 3
            )

            item['sector'] = keyword_to_sector(query)
            item['industry'] = query
            item['search_keyword'] = query

            loc_match = re.search(r'\bin\s+(.+)$', query, re.I)
            if loc_match:
                item['hq_city'] = loc_match.group(1).strip()
                item['city'] = item['hq_city']

            # Infer country
            city_lower = (item.get('hq_city') or '').lower()
            if city_lower in ('london', 'manchester', 'birmingham'):
                item['hq_country'] = 'United Kingdom'
            elif city_lower in ('dubai', 'abu dhabi'):
                item['hq_country'] = 'UAE'
            else:
                item['hq_country'] = 'India'

            item['scraped_date'] = datetime.utcnow().isoformat()
            yield item
