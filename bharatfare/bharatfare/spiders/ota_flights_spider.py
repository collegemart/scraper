"""OTA Flights spider: Scrapes flight pricing data from publicly accessible
search results for competitive intelligence.

Uses the universal spider's Playwright engine to render JS-heavy OTA pages.
Targets Google Flights structured data.
"""

import json
import re
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import scrapy
from scrapy_playwright.page import PageMethod

from bharatfare.items import LeadItem


# Popular corporate travel routes (origin, destination)
FLIGHT_ROUTES = [
    # India domestic
    ('Delhi', 'Mumbai'), ('Delhi', 'Bangalore'), ('Delhi', 'Hyderabad'),
    ('Delhi', 'Chennai'), ('Mumbai', 'Bangalore'), ('Mumbai', 'Delhi'),
    ('Mumbai', 'Hyderabad'), ('Bangalore', 'Delhi'), ('Bangalore', 'Mumbai'),
    ('Chennai', 'Delhi'), ('Kolkata', 'Delhi'), ('Pune', 'Delhi'),
    ('Hyderabad', 'Delhi'), ('Ahmedabad', 'Mumbai'),
    # India to International
    ('Delhi', 'Dubai'), ('Mumbai', 'Dubai'), ('Delhi', 'London'),
    ('Mumbai', 'London'), ('Delhi', 'Singapore'), ('Mumbai', 'Singapore'),
    ('Bangalore', 'Singapore'), ('Delhi', 'New York'),
    ('Mumbai', 'New York'), ('Delhi', 'Toronto'),
    ('Chennai', 'Singapore'), ('Hyderabad', 'Dubai'),
    # International routes
    ('London', 'Dubai'), ('London', 'New York'), ('Dubai', 'Singapore'),
    ('Singapore', 'Hong Kong'), ('London', 'Singapore'),
]


class OtaFlightsSpider(scrapy.Spider):
    """Scrapes flight pricing data for BharatFare competitive intelligence."""

    name = "ota_flights"

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
        'ITEM_PIPELINES': {},  # Flight data doesn't go through lead pipelines
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Default to searches 7 and 14 days out
        today = datetime.now()
        self.search_dates = [
            (today + timedelta(days=7)).strftime('%Y-%m-%d'),
            (today + timedelta(days=14)).strftime('%Y-%m-%d'),
            (today + timedelta(days=30)).strftime('%Y-%m-%d'),
        ]

    def start_requests(self):
        for origin, destination in FLIGHT_ROUTES:
            for date in self.search_dates:
                # Google Flights URL
                url = (
                    f"https://www.google.com/travel/flights"
                    f"?q=flights+from+{quote_plus(origin)}+to+{quote_plus(destination)}"
                    f"+on+{date}"
                )
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_flights,
                    cb_kwargs={
                        'origin': origin,
                        'destination': destination,
                        'date': date,
                    },
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

    async def parse_flights(self, response, origin, destination, date):
        pw_page = response.meta.get('playwright_page')

        if pw_page:
            try:
                # Scroll to load more results
                for _ in range(3):
                    await pw_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await pw_page.wait_for_timeout(2000)

                # Click "Show more" if available
                try:
                    show_more = await pw_page.query_selector('button[aria-label*="more"]')
                    if show_more:
                        await show_more.click()
                        await pw_page.wait_for_timeout(2000)
                except Exception:
                    pass

                content = await pw_page.content()
                response = response.replace(body=content.encode('utf-8'))
            except Exception as e:
                self.logger.debug(f"Page interaction failed: {e}")
            finally:
                try:
                    await pw_page.close()
                except Exception:
                    pass

        items_found = 0

        # Extract from JSON-LD structured data
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
                if isinstance(data, list):
                    for d in data:
                        item = self._parse_flight_json(d, response, origin, destination, date)
                        if item:
                            yield item
                            items_found += 1
                elif isinstance(data, dict):
                    item = self._parse_flight_json(data, response, origin, destination, date)
                    if item:
                        yield item
                        items_found += 1
            except json.JSONDecodeError:
                pass

        # Extract from HTML flight cards
        flight_cards = response.css(
            '[class*=flight-card], '
            '[class*=result-item], '
            'li[class*=itinerary], '
            '[data-ved] [role=listitem], '
            'div[class*=pIav2d], '  # Google Flights
            'div[class*=yR1fYc]'   # Google Flights result row
        )

        for card in flight_cards:
            texts = card.css('::text').getall()
            full_text = ' '.join(t.strip() for t in texts if t.strip())

            if len(full_text) < 20:
                continue

            item = {
                'source_url': response.url,
                'source_domain': 'google.com/travel/flights',
                'scraped_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'route_origin': origin,
                'route_destination': destination,
                'search_date': date,
            }

            # Extract airline
            airline = card.css(
                '[class*=airline]::text, '
                '[class*=carrier]::text, '
                'span[class*=Ir0Voe]::text'  # Google Flights
            ).get('')
            if airline:
                item['airline'] = airline.strip()

            # Price
            price_text = card.css(
                '[class*=price]::text, '
                '[class*=fare]::text, '
                'span[class*=YMlIz]::text'  # Google Flights
            ).get('')
            if price_text:
                item['price'] = price_text.strip()
            else:
                # Try regex on full text
                price_match = re.search(
                    r'[\$₹£€]\s?\d[\d,]*\.?\d*|\d[\d,]*\.?\d*\s*(?:USD|INR|EUR|GBP)',
                    full_text
                )
                if price_match:
                    item['price'] = price_match.group(0).strip()

            # Times
            dep_time = card.css('[class*=depart]::text, [class*=departure]::text').get('')
            arr_time = card.css('[class*=arrival]::text, [class*=arrive]::text').get('')
            if dep_time:
                item['departure_time'] = dep_time.strip()
            if arr_time:
                item['arrival_time'] = arr_time.strip()

            # Duration
            duration = card.css('[class*=duration]::text').get('')
            if duration:
                item['duration'] = duration.strip()

            # Stops
            stops = card.css('[class*=stop]::text, [class*=layover]::text').get('')
            if stops:
                item['stops'] = stops.strip()

            # Must have at least airline or price
            if item.get('airline') or item.get('price'):
                yield item
                items_found += 1

        self.logger.info(
            f"[{origin}→{destination}][{date}] {items_found} flights"
        )

    def _parse_flight_json(self, data, response, origin, destination, date):
        """Parse flight data from JSON-LD structured data."""
        if not isinstance(data, dict):
            return None

        schema_type = data.get('@type', '')
        if schema_type not in ('Flight', 'FlightReservation', 'Offer', ''):
            return None

        name = data.get('name', '') or data.get('flightNumber', '')
        if not name:
            return None

        item = {
            'source_url': response.url,
            'source_domain': 'google.com/travel/flights',
            'scraped_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'route_origin': origin,
            'route_destination': destination,
            'search_date': date,
        }

        item['airline'] = str(data.get('airline', {}).get('name', '') or data.get('provider', {}).get('name', '') or '').strip()
        item['flight_number'] = str(data.get('flightNumber', '')).strip()

        dep = data.get('departureTime', '') or ''
        arr = data.get('arrivalTime', '') or ''
        if dep:
            item['departure_time'] = dep
        if arr:
            item['arrival_time'] = arr

        dep_airport = data.get('departureAirport', {})
        arr_airport = data.get('arrivalAirport', {})
        if isinstance(dep_airport, dict):
            item['departure_airport'] = dep_airport.get('name', '') or dep_airport.get('iataCode', '')
        if isinstance(arr_airport, dict):
            item['arrival_airport'] = arr_airport.get('name', '') or arr_airport.get('iataCode', '')

        price = data.get('offers', [{}])[0].get('price', '') if data.get('offers') else ''
        if not price:
            price = data.get('price', '') or ''
        if price:
            currency = data.get('offers', [{}])[0].get('priceCurrency', 'INR') if data.get('offers') else 'INR'
            item['price'] = f"{currency} {price}"

        return item if (item.get('airline') or item.get('price')) else None
