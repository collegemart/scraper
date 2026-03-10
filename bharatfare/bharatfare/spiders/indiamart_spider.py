import json
import re
from datetime import datetime

import scrapy

from bharatfare.items import LeadItem
from bharatfare.constants import (
    CORPORATE_TRAVEL_KEYWORDS,
    CITIES_INDIA,
    keyword_to_sector,
)


class IndiamartSpider(scrapy.Spider):
    name = "indiamart"
    allowed_domains = ["dir.indiamart.com"]

    custom_settings = {
        'DOWNLOAD_DELAY': 1.5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 3,
    }

    MAX_PAGES = 20  # Increased from 10

    def start_requests(self):
        for keyword in CORPORATE_TRAVEL_KEYWORDS:
            for city in CITIES_INDIA:
                url = (
                    f"https://dir.indiamart.com/search.mp"
                    f"?ss={keyword.replace(' ', '+')}&prdsrc=1"
                    f"&city_dir={city}"
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
            search_resp = data['props']['pageProps']['searchResponse']
            results = search_resp.get('results', [])
            total_results = search_resp.get('total_results', 0)
            has_next = search_resp.get('nextPage', False)
        except (KeyError, TypeError) as e:
            self.logger.error(f"Unexpected JSON structure at {response.url}: {e}")
            return

        self.logger.info(
            f"[{keyword}][{city}] Page {page}: "
            f"{len(results)} results (total: {total_results})"
        )

        for result in results:
            fields = result.get('fields', result)
            item = LeadItem()

            item['company_name'] = str(fields.get('companyname') or '').strip()
            item['profile_id'] = str(fields.get('glusrid') or fields.get('displayid') or '')
            item['source'] = 'indiamart'
            item['source_url'] = str(fields.get('catalog_url') or fields.get('title_url') or '')

            item['phone'] = str(fields.get('pns') or '').strip()
            item['contact_person'] = ''
            item['email'] = ''
            item['company_website'] = str(fields.get('paidurl') or '')
            item['website'] = item['company_website']

            item['address'] = str(fields.get('address') or '').strip()
            item['city'] = str(fields.get('city') or fields.get('district') or city).strip()
            item['hq_city'] = item['city']
            item['hq_country'] = 'India'
            item['state'] = str(fields.get('state') or '').strip()
            item['pincode'] = str(fields.get('zipcode') or '').strip()

            item['industry'] = keyword
            item['sector'] = keyword_to_sector(keyword)
            item['search_keyword'] = keyword
            item['gst_number'] = str(fields.get('gstNumber') or '').strip()
            item['supplier_rating'] = fields.get('supplier_rating', '')
            item['member_since'] = str(fields.get('memberSince') or '')

            item['scraped_date'] = datetime.utcnow().isoformat()

            if item['company_name']:
                yield item

        if has_next and page < self.MAX_PAGES:
            next_page = page + 1
            next_url = (
                f"https://dir.indiamart.com/search.mp"
                f"?ss={keyword.replace(' ', '+')}&prdsrc=1"
                f"&city_dir={city}&cq_pg={next_page}"
            )
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_search,
                cb_kwargs={'keyword': keyword, 'city': city, 'page': next_page},
                meta={'impersonate': 'chrome124'},
            )
