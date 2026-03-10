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


class ExportersIndiaSpider(scrapy.Spider):
    name = "exportersindia"
    allowed_domains = ["www.exportersindia.com"]

    custom_settings = {
        'DOWNLOAD_DELAY': 1.5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 3,
    }

    def start_requests(self):
        for keyword in CORPORATE_TRAVEL_KEYWORDS:
            for city in CITIES_INDIA:
                url = (
                    f"https://www.exportersindia.com/search.php"
                    f"?term={keyword.replace(' ', '+')}&city={city}"
                )
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_search,
                    cb_kwargs={'keyword': keyword, 'city': city},
                    meta={'impersonate': 'chrome124'},
                )

    # Non-company URL segments to skip
    SKIP_SLUGS = {
        'web-stories', 'blog', 'news', 'about', 'contact', 'privacy',
        'terms', 'sitemap', 'faq', 'help', 'careers', 'advertise',
        'cn', 'us', 'uk', 'ae',  # country subfolders
    }

    def parse_search(self, response, keyword, city):
        seen_urls = set()

        # Extract company profile links from search results
        for link in response.css('a[href*="exportersindia.com/"]'):
            href = link.attrib.get('href', '')
            text = link.css('::text').get('').strip()

            # Company profiles: end with /, not a special page
            if not (href.endswith('/')
                    and '/product-detail/' not in href
                    and '/search' not in href
                    and '/category/' not in href
                    and '/blog/' not in href
                    and '/web-stories/' not in href
                    and href not in seen_urls
                    and text):
                continue

            # Extract slug and skip known non-company paths
            slug = href.rstrip('/').split('/')[-1]
            if slug in self.SKIP_SLUGS or len(slug) < 3:
                continue

            seen_urls.add(href)

            if not href.startswith('http'):
                href = f"https://www.exportersindia.com{href}"

            yield scrapy.Request(
                url=href,
                callback=self.parse_company,
                cb_kwargs={
                    'keyword': keyword,
                    'city': city,
                    'link_text': text,
                },
                meta={'impersonate': 'chrome124'},
            )

        self.logger.info(
            f"[{keyword}][{city}] Found {len(seen_urls)} company links"
        )

    def parse_company(self, response, keyword, city, link_text):
        item = LeadItem()

        # Company name from h1
        h1_text = response.css('h1::text').get('')
        name = h1_text.strip() if h1_text.strip() else link_text
        item['company_name'] = name

        # Address from company_info section
        address_parts = []
        for text in response.css('.company_info *::text').getall():
            text = text.strip()
            if text and text != name and len(text) > 3:
                address_parts.append(text)
        item['address'] = ', '.join(address_parts[:3]) if address_parts else ''

        # Extract from structured data if available
        for dt in response.css('table tr, .profile-detail tr, dl dt'):
            label = dt.css('td:first-child::text, dt::text').get('')
            value_el = dt.css('td:last-child::text, dd::text')
            value = value_el.get('').strip() if value_el else ''

            label_lower = label.strip().lower().rstrip(':')
            if not value:
                continue

            if 'website' in label_lower or 'url' in label_lower:
                item['company_website'] = value
                item['website'] = value
            elif 'email' in label_lower:
                item['contact_email'] = value
                item['email'] = value
            elif 'phone' in label_lower or 'mobile' in label_lower:
                item['phone'] = value
            elif 'address' in label_lower:
                item['address'] = value
            elif 'city' in label_lower:
                item['city'] = value
            elif 'state' in label_lower:
                item['state'] = value
            elif 'year' in label_lower or 'established' in label_lower:
                item['year_established'] = value
            elif 'employee' in label_lower:
                item['company_size'] = value
            elif 'turnover' in label_lower or 'revenue' in label_lower:
                item['annual_turnover'] = value
                item['company_revenue'] = value
            elif 'gst' in label_lower:
                item['gst_number'] = value
            elif 'nature' in label_lower or 'type' in label_lower:
                item['business_type'] = value
            elif 'owner' in label_lower or 'director' in label_lower or 'ceo' in label_lower:
                item['contact_person'] = value
                item['designation'] = label.strip().rstrip(':')

        item['city'] = item.get('city') or city
        item['hq_city'] = item.get('city', city)
        item['hq_country'] = 'India'
        item['source'] = 'exportersindia'
        item['source_url'] = response.url
        item['industry'] = keyword
        item['sector'] = keyword_to_sector(keyword)
        item['search_keyword'] = keyword
        item['scraped_date'] = datetime.utcnow().isoformat()

        # Extract profile ID from URL
        slug = response.url.rstrip('/').split('/')[-1]
        item['profile_id'] = slug

        # Extract emails from the full page if not already found
        if not item.get('contact_email'):
            best_email, _ = extract_emails_from_response(response)
            if best_email:
                item['contact_email'] = best_email
                item['email'] = best_email

        if item['company_name']:
            yield item
