"""Fundoodata spider: Indian corporate database with company details,
revenue, employee count, and key decision-maker contacts.
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


class FundoodataSpider(scrapy.Spider):
    name = "fundoodata"
    allowed_domains = ["www.fundoodata.com"]

    custom_settings = {
        'DOWNLOAD_DELAY': 2.5,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_TIMEOUT': 30,
    }

    MAX_PAGES = 10

    # Fundoodata uses industry categories in URLs
    INDUSTRY_SLUGS = [
        'information-technology',
        'consulting',
        'financial-services',
        'pharmaceutical',
        'logistics',
        'oil-and-gas',
        'manufacturing',
        'construction',
        'staffing-and-recruitment',
        'marketing-and-advertising',
        'real-estate',
        'healthcare',
        'education',
        'hospitality',
        'media-and-entertainment',
        'telecom',
        'retail',
        'bpo-and-outsourcing',
    ]

    def start_requests(self):
        # Category-based browsing
        for slug in self.INDUSTRY_SLUGS:
            url = f"https://www.fundoodata.com/companies/industry/{slug}"
            yield scrapy.Request(
                url=url,
                callback=self.parse_listing,
                cb_kwargs={'industry': slug, 'page': 1},
                meta={'impersonate': 'chrome124'},
            )

        # Also search by keyword + city for targeted results
        top_keywords = CORPORATE_TRAVEL_KEYWORDS[:20]  # Top 20 for Fundoodata
        top_cities = CITIES_INDIA[:14]  # Major cities
        for keyword in top_keywords:
            for city in top_cities:
                url = (
                    f"https://www.fundoodata.com/search"
                    f"?q={quote_plus(keyword)}&city={quote_plus(city)}"
                )
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_search,
                    cb_kwargs={'keyword': keyword, 'city': city},
                    meta={'impersonate': 'chrome124'},
                )

    def parse_listing(self, response, industry, page):
        """Parse company listing page by industry category."""
        items_found = 0

        cards = response.css(
            'div.company-card, '
            'div[class*=company-list], '
            'div[class*=company-item], '
            'tr.company-row, '
            'div[class*=result-item], '
            'div[class*=listing]'
        )

        for card in cards:
            item = self._parse_card(card, response, industry.replace('-', ' '))
            if item:
                yield item
                items_found += 1

        self.logger.info(
            f"[{industry}] Page {page}: {items_found} companies"
        )

        # Pagination
        if items_found > 0 and page < self.MAX_PAGES:
            next_link = response.css(
                'a[rel=next]::attr(href), '
                'a.next::attr(href), '
                'li.next a::attr(href), '
                '[class*=pagination] a[class*=next]::attr(href)'
            ).get('')

            if next_link:
                if not next_link.startswith('http'):
                    next_link = f"https://www.fundoodata.com{next_link}"
                yield scrapy.Request(
                    url=next_link,
                    callback=self.parse_listing,
                    cb_kwargs={'industry': industry, 'page': page + 1},
                    meta={'impersonate': 'chrome124'},
                )

    def parse_search(self, response, keyword, city):
        """Parse search results page."""
        cards = response.css(
            'div.company-card, '
            'div[class*=search-result], '
            'div[class*=company-item], '
            'div[class*=result-item], '
            'div[class*=listing]'
        )

        items_found = 0
        for card in cards:
            item = self._parse_card(card, response, keyword)
            if item:
                # Override city from search
                item['city'] = city.title()
                item['hq_city'] = city.title()
                yield item
                items_found += 1

        self.logger.info(
            f"[{keyword}][{city}] Found {items_found} companies"
        )

    def _parse_card(self, card, response, keyword):
        """Parse a single company card element."""
        item = LeadItem()

        # Company name
        name = (
            card.css('h2 a::text, h3 a::text').get('') or
            card.css('[class*=company-name]::text').get('') or
            card.css('a[class*=name]::text').get('') or
            card.css('td:first-child a::text').get('')
        ).strip()

        if not name or len(name) < 2:
            return None

        item['company_name'] = name
        item['source'] = 'fundoodata'

        # Profile link
        profile_href = (
            card.css('h2 a::attr(href), h3 a::attr(href)').get('') or
            card.css('[class*=company-name] a::attr(href)').get('') or
            card.css('td:first-child a::attr(href)').get('')
        )
        if profile_href:
            if not profile_href.startswith('http'):
                profile_href = f"https://www.fundoodata.com{profile_href}"
            item['source_url'] = profile_href
            item['profile_id'] = profile_href.rstrip('/').split('/')[-1]
        else:
            item['source_url'] = response.url

        # Location
        location = (
            card.css('[class*=location]::text').get('') or
            card.css('[class*=city]::text').get('') or
            card.css('.location::text').get('')
        ).strip()
        if location:
            parts = [p.strip() for p in location.split(',')]
            item['hq_city'] = parts[0]
            item['city'] = parts[0]
            if len(parts) > 1:
                item['hq_country'] = parts[-1]
            else:
                item['hq_country'] = 'India'
        else:
            item['hq_country'] = 'India'

        # Company size
        emp = (
            card.css('[class*=employee]::text').get('') or
            card.css('[class*=size]::text').get('')
        ).strip()
        if emp:
            item['company_size'] = emp

        # Revenue
        revenue = (
            card.css('[class*=revenue]::text').get('') or
            card.css('[class*=turnover]::text').get('')
        ).strip()
        if revenue:
            item['company_revenue'] = revenue

        # Website
        website = card.css(
            'a[class*=website]::attr(href), '
            'a[rel=nofollow]::attr(href)'
        ).get('')
        if website and 'fundoodata' not in website:
            item['company_website'] = website

        # Contact person
        person = card.css(
            '[class*=contact-person]::text, '
            '[class*=ceo]::text, '
            '[class*=director]::text'
        ).get('')
        if person:
            item['contact_person'] = person.strip()

        # Designation
        designation = card.css('[class*=designation]::text').get('')
        if designation:
            item['designation'] = designation.strip()

        item['sector'] = keyword_to_sector(keyword)
        item['industry'] = keyword
        item['search_keyword'] = keyword
        item['scraped_date'] = datetime.utcnow().isoformat()

        return item
