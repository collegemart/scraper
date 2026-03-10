import json
import re
from datetime import datetime

import scrapy

from bharatfare.items import LeadItem
from bharatfare.constants import (
    CLUTCH_CATEGORIES,
    CLUTCH_LOCATIONS,
    clutch_category_to_sector,
)

# Map location slug to (city, country) for fallback
LOCATION_CITY_MAP = {
    'india': ('', 'India'),
    'india/delhi': ('Delhi', 'India'),
    'india/mumbai': ('Mumbai', 'India'),
    'india/bangalore': ('Bangalore', 'India'),
    'india/hyderabad': ('Hyderabad', 'India'),
    'india/chennai': ('Chennai', 'India'),
    'india/pune': ('Pune', 'India'),
    'united-kingdom/london': ('London', 'United Kingdom'),
    'uae/dubai': ('Dubai', 'UAE'),
    'singapore': ('Singapore', 'Singapore'),
    'united-states': ('', 'United States'),
}


class ClutchSpider(scrapy.Spider):
    name = "clutch"
    allowed_domains = ["clutch.co"]

    custom_settings = {
        'DOWNLOAD_DELAY': 2.5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_TIMEOUT': 30,
    }

    MAX_PAGES = 10

    def start_requests(self):
        for category in CLUTCH_CATEGORIES:
            for location in CLUTCH_LOCATIONS:
                url = f"https://clutch.co/{category}/{location}?page=1"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_listing,
                    cb_kwargs={
                        'category': category,
                        'location': location,
                        'page': 1,
                    },
                    meta={'impersonate': 'chrome124'},
                )

    def parse_listing(self, response, category, location, page):
        sector = clutch_category_to_sector(category)
        default_city, default_country = LOCATION_CITY_MAP.get(location, ('', ''))
        items_found = 0

        # Strategy A: __NEXT_DATA__ JSON
        next_data = self._extract_next_data(response)
        if next_data:
            for item in self._parse_next_data(
                next_data, response, category, location, sector,
                default_city, default_country,
            ):
                yield item
                items_found += 1

        # Strategy B: CSS selector fallback
        if items_found == 0:
            for item in self._parse_html(
                response, category, location, sector,
                default_city, default_country,
            ):
                yield item
                items_found += 1

        self.logger.info(
            f"[{category}][{location}] Page {page}: {items_found} companies"
        )

        if items_found > 0 and page < self.MAX_PAGES:
            next_page = page + 1
            next_url = f"https://clutch.co/{category}/{location}?page={next_page}"
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_listing,
                cb_kwargs={
                    'category': category,
                    'location': location,
                    'page': next_page,
                },
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
                self.logger.error("Failed to parse __NEXT_DATA__")
        return None

    def _parse_next_data(self, data, response, category, location, sector,
                         default_city, default_country):
        try:
            page_props = data.get('props', {}).get('pageProps', {})
            companies = (
                page_props.get('providers') or
                page_props.get('companies') or
                page_props.get('results') or
                page_props.get('listings') or
                []
            )
            if not companies:
                companies = self._find_company_array(page_props)
        except (KeyError, TypeError):
            return

        for company in companies:
            if not isinstance(company, dict):
                continue

            item = LeadItem()
            item['company_name'] = str(
                company.get('name') or
                company.get('company_name') or
                company.get('title') or ''
            ).strip()

            if not item['company_name']:
                continue

            item['source'] = 'clutch'
            item['source_url'] = response.url

            profile_slug = company.get('slug') or company.get('profile_url') or ''
            if profile_slug:
                if not profile_slug.startswith('http'):
                    item['source_url'] = f"https://clutch.co/profile/{profile_slug}"
                else:
                    item['source_url'] = profile_slug

            item['profile_id'] = str(
                company.get('id') or
                company.get('provider_id') or
                profile_slug or ''
            )

            # Website: skip clutch redirect URLs
            raw_website = str(
                company.get('website') or company.get('website_url') or ''
            ).strip()
            if raw_website and 'clutch.co' not in raw_website:
                item['company_website'] = raw_website

            # Location extraction
            loc_data = company.get('location') or company.get('locations') or {}
            if isinstance(loc_data, dict):
                item['hq_city'] = str(loc_data.get('city') or '').strip()
                item['hq_country'] = str(loc_data.get('country') or '').strip()
            elif isinstance(loc_data, str):
                item['hq_city'] = loc_data.strip()
            else:
                item['hq_city'] = str(
                    company.get('city') or company.get('location_city') or ''
                ).strip()
                item['hq_country'] = str(company.get('country') or '').strip()

            # Fallback city/country from URL location slug
            if not item.get('hq_city'):
                item['hq_city'] = default_city
            if not item.get('hq_country'):
                item['hq_country'] = default_country
            item['city'] = item.get('hq_city', '')

            item['company_size'] = str(
                company.get('employees') or
                company.get('employee_range') or
                company.get('size') or
                company.get('min_employees', '')
            ).strip()

            item['year_established'] = str(
                company.get('founded') or
                company.get('year_founded') or ''
            ).strip()

            item['supplier_rating'] = str(
                company.get('rating') or
                company.get('clutch_rating') or ''
            ).strip()

            item['company_revenue'] = str(
                company.get('revenue') or
                company.get('project_size') or ''
            ).strip()

            item['sector'] = sector
            item['industry'] = category.replace('-', ' ')
            item['search_keyword'] = f"{category} {location}"

            services = company.get('service_focus') or company.get('services') or []
            if isinstance(services, list):
                item['business_type'] = ', '.join(str(s) for s in services[:5])

            item['scraped_date'] = datetime.utcnow().isoformat()
            yield item

    def _parse_html(self, response, category, location, sector,
                    default_city, default_country):
        """Fallback: parse company cards from HTML via CSS selectors."""
        cards = response.css(
            'li.provider-row, '
            'div[data-provider-id], '
            'div.provider-info, '
            'ul.providers-list > li, '
            'div[class*=provider], '
            'div[class*=company-card]'
        )

        for card in cards:
            item = LeadItem()

            name = (
                card.css('h3.company_info a::text').get('') or
                card.css('a.company_name::text').get('') or
                card.css('h3 a::text').get('') or
                card.css('[class*=company-name]::text').get('') or
                card.css('a[class*=provider-name]::text').get('')
            ).strip()

            if not name:
                continue

            item['company_name'] = name
            item['source'] = 'clutch'

            profile_href = (
                card.css('h3.company_info a::attr(href)').get('') or
                card.css('a.company_name::attr(href)').get('') or
                card.css('h3 a::attr(href)').get('')
            )
            if profile_href:
                if not profile_href.startswith('http'):
                    profile_href = f"https://clutch.co{profile_href}"
                item['source_url'] = profile_href
            else:
                item['source_url'] = response.url

            raw_website = card.css(
                'a[class*=website]::attr(href), '
                'a[data-link-type=website]::attr(href)'
            ).get('')
            if raw_website and 'clutch.co' not in raw_website:
                item['company_website'] = raw_website

            loc_text = (
                card.css('.locality::text').get('') or
                card.css('[class*=location]::text').get('') or
                card.css('.provider-info--location::text').get('')
            ).strip()
            if loc_text:
                parts = [p.strip() for p in loc_text.split(',')]
                item['hq_city'] = parts[0] if parts else default_city
                item['hq_country'] = parts[-1] if len(parts) > 1 else default_country
            else:
                item['hq_city'] = default_city
                item['hq_country'] = default_country

            item['city'] = item.get('hq_city', '')

            item['company_size'] = (
                card.css('[class*=employees]::text').get('') or
                card.css('[class*=size]::text').get('')
            ).strip()

            item['supplier_rating'] = (
                card.css('[class*=rating] span::text').get('') or
                card.css('.rating::text').get('')
            ).strip()

            item['sector'] = sector
            item['industry'] = category.replace('-', ' ')
            item['search_keyword'] = f"{category} {location}"
            item['scraped_date'] = datetime.utcnow().isoformat()

            yield item

    def _find_company_array(self, obj, depth=0):
        if depth > 8:
            return []
        if isinstance(obj, list):
            dicts = [x for x in obj if isinstance(x, dict)]
            if len(dicts) >= 3:
                sample = dicts[0]
                if any(k in sample for k in (
                    'name', 'company_name', 'title', 'website',
                    'slug', 'provider_id', 'employees',
                )):
                    return dicts
            for arr_item in obj:
                if isinstance(arr_item, (dict, list)):
                    result = self._find_company_array(arr_item, depth + 1)
                    if result:
                        return result
        elif isinstance(obj, dict):
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    result = self._find_company_array(v, depth + 1)
                    if result:
                        return result
        return []
