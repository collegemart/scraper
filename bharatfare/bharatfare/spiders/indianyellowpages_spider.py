import re
from datetime import datetime

import scrapy

from bharatfare.items import LeadItem
from bharatfare.utils import extract_contact_from_response
from bharatfare.constants import (
    CORPORATE_TRAVEL_KEYWORDS,
    CITIES_INDIA,
    keyword_to_sector,
    keyword_to_hyphenated,
)


class IndianYellowPagesSpider(scrapy.Spider):
    name = "indianyellowpages"
    allowed_domains = ["www.indianyellowpages.com"]

    custom_settings = {
        'DOWNLOAD_DELAY': 1.5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 3,
    }

    def start_requests(self):
        for keyword in CORPORATE_TRAVEL_KEYWORDS:
            slug = keyword_to_hyphenated(keyword)
            for city in CITIES_INDIA:
                url = f"https://www.indianyellowpages.com/{city}/{slug}.htm"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_search,
                    cb_kwargs={
                        'keyword': keyword,
                        'city': city,
                    },
                    meta={'impersonate': 'chrome124'},
                )

    def parse_search(self, response, keyword, city):
        listings = response.css('#append_results_div > li')
        self.logger.info(f"[{keyword}][{city}] Found {len(listings)} listings")

        for listing in listings:
            box = listing.css('div._service_box')
            if not box:
                continue

            item = LeadItem()

            # Profile URL from data-url attribute
            profile_url = box.attrib.get('data-url', '')
            item['source_url'] = profile_url

            # Extract company name and ID from button title attribute
            # Format: "IT Services#5464882#Prime Search#0"
            btn = listing.css('button._send_inq_bt, button._call_bt')
            title_attr = ''
            for b in btn:
                t = b.attrib.get('title', '')
                if '#' in t:
                    title_attr = t
                    break

            company_name = ''
            company_id = ''
            if title_attr and '#' in title_attr:
                parts = title_attr.split('#')
                if len(parts) >= 3:
                    company_id = parts[1].strip()
                    company_name = parts[2].strip()

            if not company_name:
                company_name = listing.css('h3.pdp_name::text').get('').strip()

            if not company_name:
                onclick = listing.css('[onclick]').attrib.get('onclick', '')
                name_match = re.search(r"'([^']{3,})'", onclick)
                if name_match:
                    company_name = name_match.group(1)

            item['company_name'] = company_name
            item['profile_id'] = company_id

            # Services/industry description
            services = listing.css('div.pdp_service_info::text').get('').strip()
            item['business_type'] = services

            item['city'] = city.title()
            item['hq_city'] = city.title()
            item['hq_country'] = 'India'
            item['source'] = 'indianyellowpages'
            item['industry'] = keyword
            item['sector'] = keyword_to_sector(keyword)
            item['search_keyword'] = keyword
            item['scraped_date'] = datetime.utcnow().isoformat()

            if not item['company_name']:
                continue

            # Follow profile URL to extract contact info (email, phone)
            if profile_url and profile_url.startswith('http'):
                yield scrapy.Request(
                    url=profile_url,
                    callback=self.parse_profile,
                    cb_kwargs={'item': item},
                    meta={'impersonate': 'chrome124'},
                    priority=0,
                )
            else:
                yield item

    def parse_profile(self, response, item):
        """Extract contact info from the company profile page."""
        contact = extract_contact_from_response(response)

        if contact['best_email']:
            item['contact_email'] = contact['best_email']
            item['email'] = contact['best_email']
        if contact['best_phone']:
            item['phone'] = contact['best_phone']

        # Also try to extract from structured page elements
        for text_block in response.css('.company_contact_info *::text').getall():
            text_block = text_block.strip()
            if '@' in text_block and not item.get('contact_email'):
                item['contact_email'] = text_block
                item['email'] = text_block

        # Extract website if available
        website = response.css('a[href*="website"]::attr(href)').get('')
        if not website:
            for a in response.css('a[rel=nofollow]'):
                href = a.attrib.get('href', '')
                if href.startswith('http') and 'indianyellowpages' not in href:
                    website = href
                    break
        if website and not item.get('company_website'):
            item['company_website'] = website

        item['source_url'] = response.url
        yield item
