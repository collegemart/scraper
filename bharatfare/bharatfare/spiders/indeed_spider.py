from datetime import datetime
from urllib.parse import quote_plus

import scrapy
from scrapy_playwright.page import PageMethod

from bharatfare.items import LeadItem
from bharatfare.constants import INDEED_JOB_QUERIES, INDEED_DOMAINS


class IndeedSpider(scrapy.Spider):
    """Signal enrichment spider: identifies companies hiring for travel-related
    roles. Companies actively hiring corporate travel managers, travel
    coordinators, or procurement managers are strong leads for corporate travel
    services."""

    name = "indeed"
    allowed_domains = [
        "in.indeed.com", "www.indeed.co.uk",
        "www.indeed.com", "ae.indeed.com", "www.indeed.com.sg",
    ]

    custom_settings = {
        'DOWNLOAD_DELAY': 3.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DOWNLOAD_TIMEOUT': 45,
        # Playwright for JS-rendered Indeed pages
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
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 30000,
    }

    MAX_PAGES = 5  # Conservative: signal spider, not bulk scrape

    def start_requests(self):
        for domain, locations in INDEED_DOMAINS:
            for query in INDEED_JOB_QUERIES:
                for location in locations:
                    url = (
                        f"https://{domain}/jobs"
                        f"?q={quote_plus(query)}"
                        f"&l={quote_plus(location)}"
                        f"&start=0"
                    )
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_jobs,
                        cb_kwargs={
                            'domain': domain,
                            'query': query,
                            'location': location,
                            'page': 1,
                        },
                        meta={
                            'playwright': True,
                            'playwright_include_page': True,
                            'playwright_page_methods': [
                                PageMethod("wait_for_load_state", "networkidle", timeout=15000),
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

    async def parse_jobs(self, response, domain, query, location, page):
        pw_page = response.meta.get('playwright_page')
        if pw_page:
            try:
                await pw_page.close()
            except Exception:
                pass

        items_found = 0
        is_uk = 'co.uk' in domain
        country = 'United Kingdom' if is_uk else 'India'

        # Indeed job cards - multiple selector patterns for resilience
        job_cards = response.css(
            'div.job_seen_beacon, '
            'div.jobsearch-ResultsList > div, '
            'div[class*=result], '
            'td.resultContent, '
            'div.slider_container'
        )

        seen_companies = set()

        for card in job_cards:
            # Extract company name
            company_name = (
                card.css('[data-testid=company-name]::text').get('') or
                card.css('.companyName::text').get('') or
                card.css('span.company::text').get('') or
                card.css('[class*=company] a::text').get('') or
                card.css('[class*=company] span::text').get('')
            ).strip()

            if not company_name:
                continue

            # Deduplicate within this response
            company_key = company_name.lower().strip()
            if company_key in seen_companies:
                continue
            seen_companies.add(company_key)

            # Extract job location
            job_location = (
                card.css('[data-testid=text-location]::text').get('') or
                card.css('.companyLocation::text').get('') or
                card.css('div.location::text').get('') or
                card.css('[class*=location]::text').get('')
            ).strip()

            # Extract job title
            job_title = (
                card.css('h2 a span::text').get('') or
                card.css('.jobTitle span::text').get('') or
                card.css('a[data-jk] span::text').get('') or
                card.css('[class*=title] a::text').get('')
            ).strip()

            item = LeadItem()
            item['company_name'] = company_name
            item['source'] = 'indeed'
            item['source_url'] = response.url

            if job_location:
                city_parts = [p.strip() for p in job_location.split(',')]
                item['city'] = city_parts[0] if city_parts else location
                item['hq_city'] = item['city']
            else:
                item['city'] = location
                item['hq_city'] = location

            item['hq_country'] = country

            # Signal: companies hiring travel roles = high travel frequency
            item['has_international_hiring'] = 'Y'
            item['estimated_travel_frequency'] = 'High'

            item['sector'] = self._query_to_sector(query)
            item['industry'] = query
            item['search_keyword'] = query
            item['designation'] = self._infer_role(query, job_title)

            item['scraped_date'] = datetime.utcnow().isoformat()

            yield item
            items_found += 1

        self.logger.info(
            f"[{query}][{location}] Page {page}: "
            f"{items_found} unique companies found"
        )

        # Pagination
        if items_found > 0 and page < self.MAX_PAGES:
            next_start = page * 10
            next_url = (
                f"https://{domain}/jobs"
                f"?q={quote_plus(query)}"
                f"&l={quote_plus(location)}"
                f"&start={next_start}"
            )
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_jobs,
                cb_kwargs={
                    'domain': domain,
                    'query': query,
                    'location': location,
                    'page': page + 1,
                },
                meta={
                    'playwright': True,
                    'playwright_include_page': True,
                    'playwright_page_methods': [
                        PageMethod("wait_for_load_state", "networkidle", timeout=15000),
                    ],
                },
                errback=self.errback_close_page,
            )

    @staticmethod
    def _query_to_sector(query):
        q = query.lower()
        if 'procurement' in q:
            return 'Consulting'
        if 'travel' in q:
            return 'IT'
        return 'Other'

    @staticmethod
    def _infer_role(query, job_title):
        q = (query + ' ' + job_title).lower()
        if 'hr' in q or 'human resource' in q:
            return 'HR'
        if 'procurement' in q:
            return 'Procurement'
        if 'admin' in q:
            return 'Admin'
        return 'Travel'
