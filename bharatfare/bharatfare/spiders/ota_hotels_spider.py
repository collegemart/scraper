"""OTA Hotels spider: Scrapes hotel pricing data from publicly accessible
search results for competitive intelligence.

Targets Google Hotels for major corporate travel destinations.
"""

import json
import re
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import scrapy
from scrapy_playwright.page import PageMethod


# Major corporate travel destinations for hotel searches
HOTEL_DESTINATIONS = [
    # India
    'Delhi', 'Mumbai', 'Bangalore', 'Hyderabad', 'Chennai',
    'Pune', 'Kolkata', 'Ahmedabad', 'Gurgaon', 'Goa',
    'Jaipur', 'Chandigarh',
    # International
    'Dubai', 'Abu Dhabi', 'London', 'Singapore',
    'New York', 'San Francisco', 'Toronto',
    'Sydney', 'Hong Kong', 'Bangkok', 'Kuala Lumpur',
]


class OtaHotelsSpider(scrapy.Spider):
    """Scrapes hotel pricing data for BharatFare competitive intelligence."""

    name = "ota_hotels"

    custom_settings = {
        'DOWNLOAD_DELAY': 3.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'DOWNLOAD_TIMEOUT': 60,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'args': [
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
            ],
        },
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 45000,
        'ITEM_PIPELINES': {},  # Hotel data doesn't go through lead pipelines
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        today = datetime.now()
        self.checkin = (today + timedelta(days=14)).strftime('%Y-%m-%d')
        self.checkout = (today + timedelta(days=16)).strftime('%Y-%m-%d')

    def start_requests(self):
        for city in HOTEL_DESTINATIONS:
            url = (
                f"https://www.google.com/travel/hotels/{quote_plus(city)}"
                f"?q=hotels+in+{quote_plus(city)}"
                f"&dates={self.checkin}_{self.checkout}"
            )
            yield scrapy.Request(
                url=url,
                callback=self.parse_hotels,
                cb_kwargs={'city': city},
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

    async def parse_hotels(self, response, city):
        pw_page = response.meta.get('playwright_page')

        if pw_page:
            try:
                # Scroll to load more
                for _ in range(5):
                    await pw_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await pw_page.wait_for_timeout(2000)

                content = await pw_page.content()
                response = response.replace(body=content.encode('utf-8'))
            except Exception as e:
                self.logger.debug(f"Scroll failed: {e}")
            finally:
                try:
                    await pw_page.close()
                except Exception:
                    pass

        items_found = 0

        # Extract from JSON-LD
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
                items_list = data if isinstance(data, list) else [data]
                for d in items_list:
                    if not isinstance(d, dict):
                        continue
                    schema_type = d.get('@type', '')
                    if schema_type in ('Hotel', 'LodgingBusiness', 'Accommodation'):
                        item = self._parse_hotel_json(d, response, city)
                        if item:
                            yield item
                            items_found += 1
            except json.JSONDecodeError:
                pass

        # Extract from HTML hotel cards
        hotel_cards = response.css(
            '[class*=hotel-card], '
            '[class*=property-card], '
            'div[class*=result], '
            'div[data-hotel-id], '
            'a[class*=PVOOXe], '    # Google Hotels
            'div[class*=kCsInf]'    # Google Hotels result
        )

        for card in hotel_cards:
            texts = card.css('::text').getall()
            full_text = ' '.join(t.strip() for t in texts if t.strip())

            if len(full_text) < 15:
                continue

            item = {
                'source_url': response.url,
                'source_domain': 'google.com/travel/hotels',
                'scraped_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'city': city,
                'checkin': self.checkin,
                'checkout': self.checkout,
            }

            # Hotel name
            name = (
                card.css('h2::text, h3::text').get('') or
                card.css('[class*=name]::text, [class*=title]::text').get('') or
                card.css('span[class*=BgYkof]::text').get('')  # Google Hotels
            ).strip()

            if not name or len(name) < 3:
                continue
            item['hotel_name'] = name

            # Price
            price_text = card.css(
                '[class*=price]::text, '
                'span[class*=kixHKb]::text'  # Google Hotels
            ).get('')
            if price_text:
                item['price_per_night'] = price_text.strip()
            else:
                price_match = re.search(
                    r'[\$₹£€]\s?\d[\d,]*\.?\d*',
                    full_text
                )
                if price_match:
                    item['price_per_night'] = price_match.group(0).strip()

            # Rating
            rating = card.css(
                '[class*=rating]::text, '
                '[aria-label*=rating]::attr(aria-label), '
                'span[class*=KFi5wf]::text'  # Google Hotels
            ).get('')
            if rating:
                item['rating'] = rating.strip()

            # Star category
            star_text = card.css('[class*=star]::text, [aria-label*=star]::attr(aria-label)').get('')
            if star_text:
                star_match = re.search(r'(\d)', star_text)
                if star_match:
                    item['star_category'] = f"{star_match.group(1)}-star"

            # Amenities
            amenities = card.css('[class*=amenity]::text, [class*=feature]::text').getall()
            if amenities:
                item['amenities'] = ', '.join(a.strip() for a in amenities[:5] if a.strip())

            # Must have name and price
            if item.get('hotel_name') and item.get('price_per_night'):
                yield item
                items_found += 1

        self.logger.info(
            f"[{city}] Found {items_found} hotels"
        )

    def _parse_hotel_json(self, data, response, city):
        """Parse hotel from JSON-LD structured data."""
        name = data.get('name', '')
        if not name:
            return None

        item = {
            'source_url': response.url,
            'source_domain': 'google.com/travel/hotels',
            'scraped_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'city': city,
            'checkin': self.checkin,
            'checkout': self.checkout,
            'hotel_name': name,
        }

        if data.get('starRating'):
            rating = data['starRating']
            if isinstance(rating, dict):
                item['star_category'] = f"{rating.get('ratingValue', '')}-star"
            else:
                item['star_category'] = f"{rating}-star"

        if data.get('aggregateRating'):
            ar = data['aggregateRating']
            item['rating'] = str(ar.get('ratingValue', ''))

        addr = data.get('address', {})
        if isinstance(addr, dict):
            item['address'] = addr.get('streetAddress', '')
            if not item.get('city'):
                item['city'] = addr.get('addressLocality', city)

        offers = data.get('offers', data.get('priceRange', ''))
        if isinstance(offers, list) and offers:
            offer = offers[0]
            price = offer.get('price', '')
            currency = offer.get('priceCurrency', 'INR')
            if price:
                item['price_per_night'] = f"{currency} {price}"
        elif isinstance(offers, str) and offers:
            item['price_per_night'] = offers

        amenities = data.get('amenityFeature', [])
        if amenities:
            item['amenities'] = ', '.join(
                a.get('name', '') if isinstance(a, dict) else str(a)
                for a in amenities[:8]
            )

        return item
