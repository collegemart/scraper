import json
import re
from datetime import datetime

import scrapy

from bharatfare.items import LeadItem
from bharatfare.constants import (
    GOODFIRMS_CATEGORIES,
    GOODFIRMS_LOCATIONS,
    goodfirms_category_to_sector,
)


class GoodFirmsSpider(scrapy.Spider):
    name = "goodfirms"
    allowed_domains = ["www.goodfirms.co"]

    custom_settings = {
        'DOWNLOAD_DELAY': 2.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_TIMEOUT': 30,
    }

    MAX_PAGES = 10

    def start_requests(self):
        for category in GOODFIRMS_CATEGORIES:
            for location in GOODFIRMS_LOCATIONS:
                url = f"https://www.goodfirms.co/{category}/{location}"
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
        sector = goodfirms_category_to_sector(category)
        loc_parts = location.split('/')
        default_country = loc_parts[0].replace('-', ' ').title() if loc_parts else ''
        default_city = loc_parts[-1].replace('-', ' ').title() if len(loc_parts) > 1 else ''
        items_found = 0

        # Try __NEXT_DATA__ first
        next_data = self._extract_next_data(response)
        if next_data:
            for item in self._parse_next_data(next_data, response, category, location, sector):
                yield item
                items_found += 1

        # CSS selector fallback
        if items_found == 0:
            for item in self._parse_html(response, category, location, sector,
                                         default_city, default_country):
                yield item
                items_found += 1

        # Last resort: extract all links with company-like text
        if items_found == 0:
            for item in self._parse_links(response, category, location, sector,
                                          default_city, default_country):
                yield item
                items_found += 1

        self.logger.info(
            f"[{category}][{location}] Page {page}: {items_found} companies"
        )

        # Pagination
        if items_found > 0 and page < self.MAX_PAGES:
            next_link = response.css(
                'a[rel=next]::attr(href), '
                'a.next-page::attr(href), '
                'li.next a::attr(href), '
                '[class*=pagination] a[class*=next]::attr(href)'
            ).get('')

            if next_link:
                if not next_link.startswith('http'):
                    next_link = f"https://www.goodfirms.co{next_link}"
            else:
                next_link = (
                    f"https://www.goodfirms.co/{category}/{location}"
                    f"?page={page + 1}"
                )

            yield scrapy.Request(
                url=next_link,
                callback=self.parse_listing,
                cb_kwargs={
                    'category': category,
                    'location': location,
                    'page': page + 1,
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
                pass
        return None

    def _parse_next_data(self, data, response, category, location, sector):
        """Parse company array from __NEXT_DATA__ if available."""
        try:
            page_props = data.get('props', {}).get('pageProps', {})
            companies = self._find_company_array(page_props)
        except (KeyError, TypeError):
            return

        for company in companies:
            if not isinstance(company, dict):
                continue
            name = str(
                company.get('name') or company.get('company_name') or
                company.get('title') or ''
            ).strip()
            if not name:
                continue

            item = LeadItem()
            item['company_name'] = name
            item['source'] = 'goodfirms'
            item['source_url'] = response.url

            slug = company.get('slug') or company.get('url') or ''
            if slug and not slug.startswith('http'):
                item['source_url'] = f"https://www.goodfirms.co/company/{slug}"

            item['profile_id'] = str(company.get('id') or slug or '')
            item['company_website'] = str(company.get('website') or '').strip()

            loc = company.get('location') or ''
            if isinstance(loc, dict):
                item['hq_city'] = str(loc.get('city') or '').strip()
                item['hq_country'] = str(loc.get('country') or '').strip()
            elif isinstance(loc, str) and loc:
                parts = [p.strip() for p in loc.split(',')]
                item['hq_city'] = parts[0]
                item['hq_country'] = parts[-1] if len(parts) > 1 else ''

            item['city'] = item.get('hq_city', '')
            item['company_size'] = str(company.get('employees') or company.get('size') or '').strip()
            item['year_established'] = str(company.get('founded') or '').strip()
            item['supplier_rating'] = str(company.get('rating') or '').strip()
            item['sector'] = sector
            item['industry'] = category.replace('-', ' ')
            item['search_keyword'] = f"{category} {location}"
            item['scraped_date'] = datetime.utcnow().isoformat()
            yield item

    def _parse_html(self, response, category, location, sector,
                    default_city='', default_country=''):
        """Parse company cards from rendered HTML using broad CSS selectors."""
        cards = response.css(
            'div.firm-card-block, '
            'div.company-card, '
            'div[class*=listing-card], '
            'div.profile-card, '
            'div.firm-info, '
            'div[class*=directory-list] > div, '
            'div[class*=firm], '
            'div[class*=agency], '
            'div[class*=provider]'
        )

        for card in cards:
            name = (
                card.css('h3 a::text').get('') or
                card.css('.firm-name a::text').get('') or
                card.css('[class*=company-name] a::text').get('') or
                card.css('a.firm-title::text').get('') or
                card.css('h2 a::text').get('') or
                card.css('a[class*=name]::text').get('')
            ).strip()

            if not name or len(name) < 2:
                continue

            item = LeadItem()
            item['company_name'] = name
            item['source'] = 'goodfirms'

            profile_href = (
                card.css('h3 a::attr(href)').get('') or
                card.css('.firm-name a::attr(href)').get('') or
                card.css('a.firm-title::attr(href)').get('') or
                card.css('h2 a::attr(href)').get('')
            )
            if profile_href:
                if not profile_href.startswith('http'):
                    profile_href = f"https://www.goodfirms.co{profile_href}"
                item['source_url'] = profile_href
                item['profile_id'] = profile_href.rstrip('/').split('/')[-1]
            else:
                item['source_url'] = response.url

            loc_text = (
                card.css('.location::text').get('') or
                card.css('[class*=location]::text').get('') or
                card.css('.firm-location::text').get('')
            ).strip()
            if loc_text:
                parts = [p.strip() for p in loc_text.split(',')]
                item['hq_city'] = parts[0] if parts else default_city
                item['hq_country'] = parts[-1] if len(parts) > 1 else default_country
                item['city'] = item['hq_city']
            else:
                item['hq_city'] = default_city
                item['hq_country'] = default_country
                item['city'] = default_city

            item['company_size'] = (
                card.css('[class*=employee]::text').get('') or
                card.css('.emp-count::text').get('') or
                card.css('[class*=size]::text').get('')
            ).strip()

            founded = (
                card.css('[class*=founded]::text').get('') or
                card.css('[class*=year]::text').get('')
            ).strip()
            year_match = re.search(r'(\d{4})', founded)
            if year_match:
                item['year_established'] = year_match.group(1)

            item['company_website'] = (
                card.css('a[class*=website]::attr(href)').get('') or
                card.css('a[rel=nofollow]::attr(href)').get('')
            ).strip()

            item['supplier_rating'] = (
                card.css('[class*=rating]::text').get('') or
                card.css('.score::text').get('')
            ).strip()

            services = card.css(
                '.services-list li::text, '
                '[class*=service] span::text, '
                '.focus-area span::text'
            ).getall()
            if services:
                item['business_type'] = ', '.join(
                    s.strip() for s in services if s.strip()
                )

            item['sector'] = sector
            item['industry'] = category.replace('-', ' ')
            item['search_keyword'] = f"{category} {location}"
            item['scraped_date'] = datetime.utcnow().isoformat()

            yield item

    def _find_company_array(self, obj, depth=0):
        """Recursively search JSON for an array of company-like dicts."""
        if depth > 8:
            return []
        if isinstance(obj, list):
            dicts = [x for x in obj if isinstance(x, dict)]
            if len(dicts) >= 3:
                sample = dicts[0]
                if any(k in sample for k in (
                    'name', 'company_name', 'title', 'website',
                    'slug', 'employees', 'rating',
                )):
                    return dicts
            for item_val in obj:
                if isinstance(item_val, (dict, list)):
                    result = self._find_company_array(item_val, depth + 1)
                    if result:
                        return result
        elif isinstance(obj, dict):
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    result = self._find_company_array(v, depth + 1)
                    if result:
                        return result
        return []

    def _parse_links(self, response, category, location, sector,
                     default_city='', default_country=''):
        """Last resort: extract company names from all links on the page
        that point to /company/ profile pages."""
        seen = set()
        for link in response.css('a[href*="/company/"]'):
            href = link.attrib.get('href', '')
            text = link.css('::text').get('').strip()
            if not text or len(text) < 3 or href in seen:
                continue
            if any(skip in text.lower() for skip in (
                'view all', 'see more', 'read more', 'login', 'sign',
                'home', 'about', 'contact', 'privacy', 'terms',
            )):
                continue
            seen.add(href)

            item = LeadItem()
            item['company_name'] = text
            item['source'] = 'goodfirms'
            if not href.startswith('http'):
                href = f"https://www.goodfirms.co{href}"
            item['source_url'] = href
            item['profile_id'] = href.rstrip('/').split('/')[-1]
            item['hq_city'] = default_city
            item['hq_country'] = default_country
            item['city'] = default_city
            item['sector'] = sector
            item['industry'] = category.replace('-', ' ')
            item['search_keyword'] = f"{category} {location}"
            item['scraped_date'] = datetime.utcnow().isoformat()
            yield item
