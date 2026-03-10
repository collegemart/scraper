"""JustDial spider: India's largest local search / business directory.

Extracts company name, phone, address, rating, website from search results
and follows company detail pages for richer data.
"""

import re
from datetime import datetime
from urllib.parse import quote_plus

import scrapy

from bharatfare.items import LeadItem
from bharatfare.utils import extract_contact_from_response
from bharatfare.constants import (
    CORPORATE_TRAVEL_KEYWORDS,
    CITIES_INDIA,
    keyword_to_sector,
)


class JustDialSpider(scrapy.Spider):
    name = "justdial"
    allowed_domains = ["www.justdial.com"]

    custom_settings = {
        'DOWNLOAD_DELAY': 2.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_TIMEOUT': 30,
    }

    MAX_PAGES = 15

    def start_requests(self):
        for keyword in CORPORATE_TRAVEL_KEYWORDS:
            for city in CITIES_INDIA:
                slug = keyword.replace(' ', '-')
                url = f"https://www.justdial.com/{city.title()}/{slug}/nct-10101010"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_search,
                    cb_kwargs={'keyword': keyword, 'city': city, 'page': 1},
                    meta={'impersonate': 'chrome124'},
                )

    def parse_search(self, response, keyword, city, page):
        items_found = 0

        # JustDial uses multiple listing formats
        listings = response.css(
            'li.cntanr, '
            'div.resultbox_info, '
            'div[class*=resultbox], '
            'div[class*=store-details], '
            'div[class*=jsx-], '  # React components
            'section[class*=result]'
        )

        self.logger.info(
            f"[{keyword}][{city}] Page {page}: {len(listings)} listings"
        )

        for listing in listings:
            item = LeadItem()

            # Company name
            name = (
                listing.css('span.lng_cont_name::text').get('') or
                listing.css('.resultbox_title_anchor span::text').get('') or
                listing.css('h2 a span::text').get('') or
                listing.css('[class*=store-name] span::text').get('') or
                listing.css('a[class*=title]::text').get('') or
                listing.css('h2::text, h3::text').get('')
            ).strip()

            if not name or len(name) < 3:
                continue

            item['company_name'] = name

            # Rating
            rating = (
                listing.css('span.green-box::text').get('') or
                listing.css('[class*=rating]::text').get('')
            ).strip()
            if rating:
                item['supplier_rating'] = rating

            # Address
            address = (
                listing.css('span.cont_fl_addr::text').get('') or
                listing.css('[class*=address]::text').get('') or
                listing.css('.resultbox_address::text').get('')
            ).strip()
            if address:
                item['address'] = address

            # Category / business type
            category = (
                listing.css('span.lng_cont_catg::text').get('') or
                listing.css('[class*=category]::text').get('')
            ).strip()
            if category:
                item['business_type'] = category

            # Profile URL
            profile_href = (
                listing.css('a.resultbox_title_anchor::attr(href)').get('') or
                listing.css('h2 a::attr(href)').get('') or
                listing.css('a[class*=title]::attr(href)').get('')
            )

            item['city'] = city.title()
            item['hq_city'] = city.title()
            item['hq_country'] = 'India'
            item['source'] = 'justdial'
            item['industry'] = keyword
            item['sector'] = keyword_to_sector(keyword)
            item['search_keyword'] = keyword
            item['scraped_date'] = datetime.utcnow().isoformat()

            if profile_href:
                if not profile_href.startswith('http'):
                    profile_href = f"https://www.justdial.com{profile_href}"
                item['source_url'] = profile_href

                # Follow profile for phone/email/website
                yield scrapy.Request(
                    url=profile_href,
                    callback=self.parse_profile,
                    cb_kwargs={'item': item},
                    meta={'impersonate': 'chrome124'},
                    priority=0,
                )
            else:
                item['source_url'] = response.url
                yield item

            items_found += 1

        # Pagination
        if items_found > 0 and page < self.MAX_PAGES:
            next_page = page + 1
            # JustDial pagination pattern
            next_link = response.css(
                'a[rel=next]::attr(href), '
                'a.pagination-next::attr(href), '
                'a[class*=next]::attr(href)'
            ).get('')

            if not next_link:
                slug = keyword.replace(' ', '-')
                next_link = (
                    f"https://www.justdial.com/{city.title()}/{slug}"
                    f"/nct-10101010/page-{next_page}"
                )

            if next_link:
                if not next_link.startswith('http'):
                    next_link = f"https://www.justdial.com{next_link}"
                yield scrapy.Request(
                    url=next_link,
                    callback=self.parse_search,
                    cb_kwargs={'keyword': keyword, 'city': city, 'page': next_page},
                    meta={'impersonate': 'chrome124'},
                )

    def parse_profile(self, response, item):
        """Extract detailed info from company profile page."""
        contact = extract_contact_from_response(response)

        if contact['best_email']:
            item['contact_email'] = contact['best_email']
            item['email'] = contact['best_email']
        if contact['best_phone']:
            item['phone'] = contact['best_phone']

        # Try structured data on profile page
        for text_el in response.css('section[class*=contact] *::text, div[class*=contact] *::text').getall():
            text_el = text_el.strip()
            if '@' in text_el and not item.get('contact_email'):
                if re.match(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text_el):
                    item['contact_email'] = text_el
                    item['email'] = text_el

        # Website
        website = (
            response.css('a[class*=website]::attr(href)').get('') or
            response.css('a[rel=nofollow][target=_blank]::attr(href)').get('')
        )
        if website and 'justdial' not in website:
            item['company_website'] = website

        # Contact person / owner
        owner = response.css(
            '[class*=owner-name]::text, '
            '[class*=contact-person]::text'
        ).get('')
        if owner and owner.strip():
            item['contact_person'] = owner.strip()

        # Employee count / company size
        for detail in response.css('.comp-info li, [class*=detail] li'):
            label = detail.css('::text').getall()
            text = ' '.join(t.strip() for t in label)
            if 'employee' in text.lower():
                emp_match = re.search(r'(\d[\d,\-\s+]*)', text)
                if emp_match:
                    item['company_size'] = emp_match.group(1).strip()
            elif 'established' in text.lower() or 'year' in text.lower():
                year_match = re.search(r'(\d{4})', text)
                if year_match:
                    item['year_established'] = year_match.group(1)

        item['source_url'] = response.url
        yield item
