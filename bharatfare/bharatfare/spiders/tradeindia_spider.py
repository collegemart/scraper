import json
import re
from datetime import datetime

import scrapy

from bharatfare.items import LeadItem
from bharatfare.utils import extract_emails_from_response
from bharatfare.constants import (
    CORPORATE_TRAVEL_KEYWORDS,
    CITIES_INDIA,
    keyword_to_sector,
)


class TradeIndiaSpider(scrapy.Spider):
    name = "tradeindia"
    allowed_domains = ["www.tradeindia.com"]

    custom_settings = {
        'DOWNLOAD_DELAY': 1.5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 3,
    }

    MAX_PAGES = 15  # Increased from 5

    def start_requests(self):
        for keyword in CORPORATE_TRAVEL_KEYWORDS:
            for city in CITIES_INDIA:
                url = (
                    f"https://www.tradeindia.com/search.html"
                    f"?keyword={keyword.replace(' ', '+')}&city={city}&page=1"
                )
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_search,
                    cb_kwargs={'keyword': keyword, 'city': city, 'page': 1},
                    meta={'impersonate': 'chrome124'},
                )

    def _extract_next_data(self, response):
        script = response.css('script#__NEXT_DATA__::text').get()
        if not script:
            match = re.search(
                r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                response.text, re.DOTALL,
            )
            if match:
                script = match.group(1)
        if script:
            try:
                return json.loads(script)
            except json.JSONDecodeError:
                self.logger.error(f"Failed to parse __NEXT_DATA__ at {response.url}")
        return None

    def parse_search(self, response, keyword, city, page):
        data = self._extract_next_data(response)
        if not data:
            self.logger.warning(f"No __NEXT_DATA__ found at {response.url}")
            return

        try:
            server_data = data['props']['pageProps']['serverData']
            listing_data = server_data['searchListingData']['listing_data']
            pagination = server_data['searchListingData']['pagination']
        except (KeyError, TypeError) as e:
            self.logger.error(f"Unexpected JSON structure at {response.url}: {e}")
            return

        self.logger.info(
            f"[{keyword}][{city}] Page {page}: "
            f"{len(listing_data)} listings, "
            f"{pagination.get('total_pages', '?')} total pages"
        )

        for listing in listing_data:
            partial = {
                'company_name': (listing.get('co_name') or '').strip(),
                'city': (listing.get('city') or city).strip(),
                'hq_city': (listing.get('city') or city).strip(),
                'hq_country': 'India',
                'state': (listing.get('state') or '').strip(),
                'business_type': listing.get('business_type', ''),
                'source': 'tradeindia',
                'search_keyword': keyword,
                'industry': keyword,
                'sector': keyword_to_sector(keyword),
                'scraped_date': datetime.utcnow().isoformat(),
            }

            profile_id = listing.get('profile_id')
            profile_url = listing.get('profile_url', '')

            if profile_url:
                if not profile_url.startswith('http'):
                    profile_url = f"https://www.tradeindia.com{profile_url}"

                partial['source_url'] = profile_url
                partial['profile_id'] = str(profile_id)

                # Yield search result immediately (profile will enrich later)
                item = LeadItem()
                for key, val in partial.items():
                    item[key] = val
                yield item

                # Also follow profile for richer data
                yield scrapy.Request(
                    url=profile_url,
                    callback=self.parse_profile,
                    cb_kwargs={'partial': partial},
                    priority=0,
                    meta={'impersonate': 'chrome124'},
                )
            else:
                item = LeadItem()
                for key, val in partial.items():
                    item[key] = val
                yield item

        if pagination.get('has_next') and page < self.MAX_PAGES:
            next_page = page + 1
            next_url = (
                f"https://www.tradeindia.com/search.html"
                f"?keyword={keyword.replace(' ', '+')}&city={city}&page={next_page}"
            )
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_search,
                cb_kwargs={'keyword': keyword, 'city': city, 'page': next_page},
                priority=1,
                meta={'impersonate': 'chrome124'},
            )

    def parse_profile(self, response, partial):
        data = self._extract_next_data(response)

        item = LeadItem()
        for key, val in partial.items():
            item[key] = val

        if not data:
            self.logger.warning(f"No __NEXT_DATA__ on profile: {response.url}")
            yield item
            return

        company = None
        try:
            company = (
                data['props']['pageProps']['initialState']
                ['sellerProfile']['seller_profile']
                ['seller_profile_res']['company_details_data']
            )
        except (KeyError, TypeError):
            try:
                company = (
                    data['props']['pageProps']['initialState']
                    ['product']['PDP_page']['PDP_page_res']
                    ['company_details']
                )
            except (KeyError, TypeError):
                self.logger.warning(f"Cannot find company details at {response.url}")
                yield item
                return

        if not company:
            yield item
            return

        biz = company.get('business_details', company)

        item['company_name'] = (
            biz.get('co_name') or company.get('co_name')
            or item.get('company_name', '')
        )
        item['contact_person'] = company.get('owner_name', '')
        item['designation'] = company.get('desg', '')
        item['gst_number'] = company.get('gst_no', '')
        item['address'] = biz.get('address', '')
        item['city'] = biz.get('city') or item.get('city', '')
        item['hq_city'] = item['city']
        item['state'] = biz.get('state') or item.get('state', '')
        item['year_established'] = biz.get('establishment', '')
        item['company_size'] = biz.get('employees_count', '')
        item['annual_turnover'] = biz.get('annual_turnover', '')
        item['company_revenue'] = biz.get('annual_turnover', '')
        item['supplier_rating'] = company.get('rating', '')

        bt = biz.get('business_type', [])
        if isinstance(bt, list):
            item['business_type'] = ', '.join(bt)
        elif bt:
            item['business_type'] = str(bt)

        item['source_url'] = response.url

        # Extract emails from profile page
        best_email, _ = extract_emails_from_response(response)
        if best_email:
            item['contact_email'] = best_email
            item['email'] = best_email

        yield item
