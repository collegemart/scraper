"""Universal spider: scrapes ANY website using a real browser (Playwright).

Finds the actual structured data on any page - flight prices, product listings,
business contacts, etc. - by detecting arrays of similar records in JSON/HTML.

Usage:
  scrapy crawl universal -a url=https://example.com -o output.csv:csv
  scrapy crawl universal -a url=https://skyscanner.com/flights -a max_pages=5
"""

import json
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse

import scrapy

# ── Regex patterns ────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
PHONE_INDIAN_RE = re.compile(
    r'(?:\+91[\s\-]?)?[6-9]\d{9}\b'
    r'|(?:\+91[\s\-]?)?\d{2,4}[\s\-]\d{6,8}\b'
)
# International: must start with + country code, then formatted digits
PHONE_INTL_RE = re.compile(
    r'\+\d{1,3}[\s\-]\(?\d{1,5}\)?[\s\-]?\d[\d\s\-]{4,10}\d'
)
# Formatted landline: must have parens or multiple dashes/spaces separating groups
PHONE_FORMATTED_RE = re.compile(
    r'\(?\d{2,5}\)[\s\-]?\d[\d\s\-]{5,10}\d'
    r'|\d{2,5}[\s\-]\d{3,5}[\s\-]\d{3,5}'
)
GST_RE = re.compile(r'\b\d{2}[A-Z]{5}\d{4}[A-Z]\d[Z][A-Z\d]\b')
PRICE_RE = re.compile(
    r'[\$\u20b9\u00a3\u20ac]\s?\d[\d,]*\.?\d*'
    r'|\d[\d,]*\.?\d*\s*(?:USD|INR|EUR|GBP)'
)

EMAIL_BLACKLIST = {
    'example.com', 'test.com', 'email.com', 'domain.com',
    'yoursite.com', 'company.com', 'sentry.io', 'wixpress.com',
    'w3.org', 'schema.org', 'googleapis.com', 'google.com',
    'facebook.com', 'twitter.com', 'instagram.com', 'youtube.com',
}

# Keys that indicate real data, not UI metadata
DATA_KEYS = {
    # Contact / business
    'name', 'title', 'company', 'company_name', 'companyname', 'firm',
    'email', 'mail', 'phone', 'telephone', 'mobile', 'contact',
    'address', 'city', 'state', 'country', 'location', 'pincode', 'zip',
    'website', 'url', 'link', 'gst',
    # Product / commerce
    'price', 'cost', 'amount', 'currency', 'discount', 'offer', 'fare', 'fee',
    'rating', 'score', 'reviews', 'review_count', 'stars',
    'category', 'brand', 'seller', 'vendor', 'supplier',
    'description', 'summary', 'details', 'features',
    # Travel / flights
    'origin', 'destination', 'departure', 'arrival', 'duration',
    'airline', 'carrier', 'flight', 'route', 'stops', 'layover',
    'cabin', 'class', 'fare', 'baggage', 'direct', 'nonstop', 'transfer',
    'departuretime', 'arrivaltime', 'departuredate', 'arrivaldate',
    'flightnumber', 'airport', 'terminal', 'gate', 'seat',
    'outbound', 'inbound', 'leg', 'segment', 'itinerary',
    # Hotel
    'hotel', 'room', 'checkin', 'checkout', 'guests', 'nights',
    'amenities', 'property', 'accommodation', 'roomtype',
    # Time / date
    'date', 'time', 'datetime', 'timestamp', 'published', 'created',
    # People / social
    'username', 'user', 'author', 'owner', 'fullname', 'display_name',
    'followers', 'following', 'posts', 'bio', 'headline', 'experience',
    'profession', 'designation', 'role', 'position', 'department',
    # E-commerce
    'sku', 'asin', 'isbn', 'upc', 'model', 'manufacturer', 'weight',
    'color', 'size', 'material', 'condition', 'availability',
    # Generic
    'id', 'status', 'type', 'label', 'image', 'photo', 'thumbnail',
    'quantity', 'stock', 'count', 'total', 'min', 'max', 'logo',
}

# Junk keys from React/framework internals
JUNK_KEYS = {
    '$$typeof', '_owner', '_store', 'ref', 'key', 'props', 'children',
    '__typename', 'typename', 'node', 'edges', 'cursor', 'pageinfo',
    'hasnextpage', 'haspreviouspage', 'startcursor', 'endcursor',
    'componentname', 'componenttype', 'modulename', 'classname',
    '__n', '__c', '_self', '_source', 'fiber', 'statenode',
}

NAV_WORDS = {
    'home', 'about', 'contact', 'login', 'signup', 'register', 'menu',
    'back', 'next', 'prev', 'more', 'view all', 'click here',
    'submit', 'download', 'upload', 'share', 'follow', 'subscribe',
    'privacy', 'terms', 'sitemap', 'faq', 'help', 'careers', 'cookie',
    'how it works', 'get started', 'learn more', 'read more',
    'see all', 'show more', 'load more',
}


def _clean_email(email):
    if not email:
        return None
    email = email.strip().lower()
    domain = email.split('@')[-1]
    if domain in EMAIL_BLACKLIST:
        return None
    if len(email) < 6 or len(email) > 80:
        return None
    return email


def _clean_phone(phone):
    if not phone:
        return None
    digits = re.sub(r'\D', '', str(phone))
    if digits.startswith('91') and len(digits) == 12:
        digits = digits[2:]
    if digits.startswith('0') and len(digits) == 11:
        digits = digits[1:]
    if 7 <= len(digits) <= 12:
        return digits
    return None


def _is_data_key(key):
    """Check if a JSON key looks like it holds real data."""
    k = key.lower().replace('-', '_')
    if k in JUNK_KEYS:
        return False
    if k in DATA_KEYS:
        return True
    # Partial match
    return any(dk in k for dk in DATA_KEYS)


def _flatten_dict(obj, prefix='', max_depth=6):
    """Flatten nested dict into key-value pairs for CSV output."""
    items = {}
    if max_depth <= 0:
        return items
    for k, v in obj.items():
        # Skip internal/framework keys
        if k.startswith('_') or k.startswith('$') or k == '@context':
            continue
        key = f"{prefix}{k}" if not prefix else f"{prefix}_{k}"
        if isinstance(v, dict):
            items.update(_flatten_dict(v, key, max_depth - 1))
        elif isinstance(v, list):
            if not v:
                continue
            if all(isinstance(x, (str, int, float)) for x in v):
                items[key] = ', '.join(str(x) for x in v[:8])
            # Array of objects (e.g. flight legs, stops) - flatten each with index
            elif (len(v) <= 6 and
                  all(isinstance(x, dict) for x in v)):
                for i, sub in enumerate(v):
                    sub_flat = _flatten_dict(sub, f'{key}_{i + 1}', max_depth - 1)
                    items.update(sub_flat)
                items[f'{key}_count'] = str(len(v))
        elif isinstance(v, (str, int, float, bool)):
            items[key] = str(v)
    return items


class UniversalSpider(scrapy.Spider):
    name = "universal"

    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'args': ['--disable-blink-features=AutomationControlled'],
        },
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 30000,
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'DOWNLOAD_DELAY': 1,
        'DOWNLOAD_TIMEOUT': 45,
        'DEPTH_LIMIT': 3,
        'CLOSESPIDER_PAGECOUNT': 500,
        'ITEM_PIPELINES': {},
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 100,
            'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 200,
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 300,
            'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 400,
            'scrapy.downloadermiddlewares.stats.DownloaderStats': 500,
        },
        'ROBOTSTXT_OBEY': False,
    }

    def __init__(self, url=None, max_pages=50, follow='true',
                 depth=2, scroll='true', *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not url:
            raise ValueError("Provide -a url=https://...")
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        self.start_url = url
        self.max_pages = int(max_pages)
        self.should_follow = follow.lower() in ('true', '1', 'yes')
        self.max_depth = int(depth)
        self.should_scroll = scroll.lower() in ('true', '1', 'yes')
        self.pages_crawled = 0
        self.yielded_keys = set()

        parsed = urlparse(url)
        self.base_domain = parsed.netloc
        self.allowed_domains = [parsed.netloc]

    def start_requests(self):
        yield scrapy.Request(
            url=self.start_url,
            callback=self.parse_page,
            meta={
                'playwright': True,
                'playwright_include_page': True,
                'playwright_page_methods': [
                    {"method": "wait_for_load_state", "args": ["networkidle"],
                     "kwargs": {"timeout": 15000}},
                ],
                'depth': 0,
            },
            errback=self.errback_close_page,
            dont_filter=True,
        )

    async def parse_page(self, response):
        if self.pages_crawled >= self.max_pages:
            page = response.meta.get('playwright_page')
            if page:
                await page.close()
            return

        self.pages_crawled += 1
        depth = response.meta.get('depth', 0)
        page = response.meta.get('playwright_page')

        # Auto-scroll for lazy-loaded content
        if page and self.should_scroll:
            try:
                await self._auto_scroll(page)
                content = await page.content()
                response = response.replace(body=content.encode('utf-8'))
            except Exception as e:
                self.logger.debug(f"Scroll failed: {e}")

        if page:
            try:
                await page.close()
            except Exception:
                pass

        items_found = 0

        # Strategy 1: Find data arrays in JSON (the main approach)
        for item in self._extract_from_json(response):
            items_found += 1
            yield item

        # Strategy 2: JSON-LD structured data
        for item in self._extract_jsonld(response):
            items_found += 1
            yield item

        # Strategy 3: HTML listing blocks with real data
        for item in self._extract_listings(response):
            items_found += 1
            yield item

        # Strategy 4: Tables
        for item in self._extract_tables(response):
            items_found += 1
            yield item

        # Strategy 5: Contact info from page (last resort)
        if items_found == 0:
            for item in self._extract_contacts(response):
                items_found += 1
                yield item

        self.logger.info(
            f"[{self.pages_crawled}/{self.max_pages}] "
            f"{response.url[:80]} -> {items_found} items"
        )

        if self.should_follow and depth < self.max_depth:
            for req in self._follow_links(response, depth):
                yield req

    async def _auto_scroll(self, page):
        prev_height = 0
        for _ in range(5):
            curr_height = await page.evaluate("document.body.scrollHeight")
            if curr_height == prev_height:
                break
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            try:
                await page.wait_for_timeout(1500)
            except Exception:
                break
            prev_height = curr_height

    async def errback_close_page(self, failure):
        page = failure.request.meta.get('playwright_page')
        if page:
            try:
                await page.close()
            except Exception:
                pass
        self.logger.error(f"Request failed: {failure.request.url}: {failure.value}")

    # ── Yield helper ──────────────────────────────────────

    def _yield_item(self, response, **fields):
        """Create an item dict, dedup, return."""
        # Build dedup key
        vals = [str(fields.get(k, '')) for k in ('name', 'title', 'email', 'phone', 'url', 'detail_url', 'price')]
        dedup_key = '|'.join(v.lower().strip() for v in vals if v)

        if not dedup_key:
            return None
        if dedup_key in self.yielded_keys:
            return None
        self.yielded_keys.add(dedup_key)

        item = {
            'source_url': response.url,
            'source_domain': self.base_domain,
            'scraped_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        }
        for k, v in fields.items():
            sv = str(v).strip() if v is not None else ''
            if sv and sv != 'None' and sv != 'null' and len(sv) < 2000:
                item[k] = sv

        return item

    # ══════════════════════════════════════════════════════
    # Strategy 1: Find data arrays in JSON
    # This is the MAIN extraction method. It finds the largest
    # arrays of similarly-structured dicts in __NEXT_DATA__,
    # window.__DATA__, or any embedded JSON.
    # ══════════════════════════════════════════════════════

    def _extract_from_json(self, response):
        json_blobs = []

        # __NEXT_DATA__
        nd = response.css('script#__NEXT_DATA__::text').get()
        if nd:
            try:
                json_blobs.append(json.loads(nd))
            except Exception:
                pass

        # <script type="application/json"> (React server components, etc.)
        for script in response.css('script[type="application/json"]::text').getall():
            try:
                json_blobs.append(json.loads(script))
            except Exception:
                pass

        # Inline <script> with JSON data
        for script in response.css('script:not([src]):not([type])::text').getall():
            chunk = script[:200000]
            for pattern in [
                r'window\.__\w+__\s*=\s*(\{.+\})\s*;',
                r'window\.\w+\s*=\s*(\{.+\})\s*;',
                r'var\s+\w+\s*=\s*(\[.+\])\s*;',
                r'const\s+\w+\s*=\s*(\{.+\})\s*;',
            ]:
                for m in re.finditer(pattern, chunk, re.DOTALL):
                    try:
                        json_blobs.append(json.loads(m.group(1)))
                    except Exception:
                        pass

        if not json_blobs:
            return

        # Find all arrays of dicts in the JSON tree
        all_arrays = []
        for blob in json_blobs:
            self._find_arrays(blob, all_arrays, depth=0)

        if not all_arrays:
            return

        # Score and pick the best arrays
        scored = []
        for arr in all_arrays:
            score = self._score_array(arr)
            if score > 0:
                scored.append((score, arr))

        if not scored:
            return

        scored.sort(key=lambda x: x[0], reverse=True)

        # Extract items from top arrays - collect from ALL good ones
        extracted = 0
        seen_keys = set()
        for score, arr in scored[:5]:  # top 5 arrays
            if score < 2:
                break
            self.logger.info(
                f"Found data array: {len(arr)} records, score={score:.1f}, "
                f"sample keys={list(arr[0].keys())[:6]}"
            )
            for obj in arr:
                flat = _flatten_dict(obj)
                # Filter out tiny/empty records and long hashes
                real_vals = {}
                for k, v in flat.items():
                    sv = str(v).strip()
                    if sv and len(sv) > 1:
                        # Skip values that are just long hex hashes
                        if re.match(r'^[a-f0-9]{20,}$', sv, re.I):
                            continue
                        real_vals[k] = sv
                if len(real_vals) < 2:
                    continue

                # Dedup
                dk = '|'.join(sorted(list(real_vals.values())[:5]))
                if dk in seen_keys:
                    continue
                seen_keys.add(dk)

                item = self._yield_item(response, **real_vals)
                if item:
                    yield item
                    extracted += 1

            if extracted > 0:
                self.logger.info(f"Extracted {extracted} items from array")

    def _find_arrays(self, obj, results, depth=0):
        """Recursively find arrays of dicts in a JSON tree."""
        if depth > 12:
            return

        if isinstance(obj, list):
            # Check if this is an array of dicts with similar keys
            dicts = [x for x in obj if isinstance(x, dict) and len(x) >= 2]
            if len(dicts) >= 2:
                # Check key consistency - items should share >50% of keys
                key_sets = [set(d.keys()) for d in dicts[:10]]
                if key_sets:
                    common = key_sets[0]
                    for ks in key_sets[1:]:
                        common = common & ks
                    union = set()
                    for ks in key_sets:
                        union |= ks
                    if union and len(common) / len(union) > 0.3:
                        results.append(dicts)

            # Recurse into list items
            for item in obj:
                if isinstance(item, (dict, list)):
                    self._find_arrays(item, results, depth + 1)

        elif isinstance(obj, dict):
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    self._find_arrays(v, results, depth + 1)

    def _score_array(self, arr):
        """Score an array of dicts by how useful the data looks."""
        if len(arr) < 2:
            return 0

        sample = arr[:10]
        all_keys = set()
        for d in sample:
            all_keys.update(k.lower().replace('-', '_') for k in d.keys())

        # Skip React/framework internal arrays
        if all_keys & JUNK_KEYS:
            junk_ratio = len(all_keys & JUNK_KEYS) / len(all_keys) if all_keys else 0
            if junk_ratio > 0.3:
                return 0

        # Score based on how many data-relevant keys exist
        data_key_count = sum(1 for k in all_keys if _is_data_key(k))
        if data_key_count == 0:
            return 0

        score = 0
        # More data keys = better
        score += data_key_count * 1.5
        # More items = better (log scale to not over-favor huge arrays)
        score += min(len(arr), 100) * 0.1
        # Has price/cost = very useful
        if any('price' in k or 'cost' in k or 'amount' in k or 'fare' in k or 'fee' in k
               for k in all_keys):
            score += 5
        # Has contact info = very useful
        if any(k in all_keys for k in ('email', 'phone', 'telephone', 'mobile')):
            score += 5
        # Has name/title = good identifier
        if any(k in all_keys for k in ('name', 'title', 'company', 'company_name')):
            score += 2
        # Travel-specific bonuses
        if any(k in all_keys or 'airline' in k or 'carrier' in k or 'flight' in k
               or 'departure' in k or 'arrival' in k or 'origin' in k or 'destination' in k
               for k in all_keys):
            score += 4
        if any('duration' in k or 'stops' in k or 'layover' in k
               or 'cabin' in k or 'baggage' in k or 'terminal' in k
               for k in all_keys):
            score += 3
        # Hotel-specific
        if any('hotel' in k or 'room' in k or 'checkin' in k
               or 'checkout' in k or 'amenities' in k or 'property' in k
               for k in all_keys):
            score += 4

        # Bonus for nested richness - items with sub-objects have deeper data
        nested_count = 0
        flat_vals = 0
        for d in sample:
            for v in d.values():
                if isinstance(v, (str, int, float)):
                    flat_vals += 1
                elif isinstance(v, (dict, list)) and v:
                    nested_count += 1
        # Nested data means richer extraction when flattened
        if nested_count > len(sample):
            score += min(nested_count / len(sample), 3) * 2
        # But must still have some flat values
        if flat_vals < len(sample):
            score *= 0.5

        return score

    # ══════════════════════════════════════════════════════
    # Strategy 2: JSON-LD structured data
    # ══════════════════════════════════════════════════════

    def _extract_jsonld(self, response):
        for script in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(script)
            except Exception:
                continue

            objects = []
            if isinstance(data, list):
                objects = data
            elif isinstance(data, dict):
                if '@graph' in data:
                    objects = data['@graph']
                elif data.get('@type') == 'ItemList':
                    objects = [
                        el.get('item', el) for el in data.get('itemListElement', [])
                        if isinstance(el, dict)
                    ]
                else:
                    objects = [data]

            for obj in objects:
                if not isinstance(obj, dict):
                    continue

                name = obj.get('name', '')
                if not name or len(str(name)) < 2:
                    continue

                flat = _flatten_dict(obj)
                flat['name'] = str(name)

                # Must have something useful beyond just a name
                useful_keys = {'phone', 'telephone', 'email', 'address', 'price',
                               'rating', 'description', 'url'}
                has_useful = any(
                    any(uk in k.lower() for uk in useful_keys)
                    for k in flat
                    if flat[k]
                )
                if not has_useful:
                    continue

                item = self._yield_item(response, **flat)
                if item:
                    yield item

    # ══════════════════════════════════════════════════════
    # Strategy 3: HTML listing blocks
    # ══════════════════════════════════════════════════════

    def _extract_listings(self, response):
        candidates = []

        selectors = [
            'div[class*=listing]', 'div[class*=result]',
            'div[class*=card]', 'div[class*=product]',
            'div[class*=flight]', 'div[class*=hotel]',
            'div[class*=itinerary]', 'div[class*=offer]',
            'div[class*=deal]', 'div[class*=price]',
            'article', 'li[class*=item]', 'li[class*=result]',
            '[data-testid]',
            '#results > *', '[class*=results] > *',
            '[id*=result] > *', '[id*=listing] > *',
            '[id*=append] > *',
        ]

        for sel in selectors:
            try:
                items = response.css(sel)
                count = len(items)
                if count < 3:
                    continue

                score = self._score_html_blocks(items)
                if score > 0:
                    candidates.append((score, count, items))
            except Exception:
                pass

        # Auto-detect parent with repeated children
        for parent in response.css('ul, ol, div, section, main, [role=list]'):
            children = parent.css(':scope > *')
            count = len(children)
            if count < 3:
                continue
            sample = children[:min(8, count)]
            text_count = sum(
                1 for c in sample
                if len(' '.join(c.css('::text').getall()).strip()) > 30
            )
            if text_count < len(sample) * 0.4:
                continue

            score = self._score_html_blocks(children)
            if score > 0:
                candidates.append((score, count, children))

        if not candidates:
            return

        candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        best_score, best_count, best_items = candidates[0]

        if best_score < 5:
            return

        self.logger.info(f"Found {best_count} listing blocks (score={best_score:.1f})")

        for block in best_items:
            texts = block.css('::text').getall()
            full_text = ' '.join(t.strip() for t in texts if t.strip())
            if len(full_text) < 15:
                continue

            fields = {}

            # Name from various sources
            name = self._extract_name(block)
            if name and len(name) >= 3 and name.lower().strip() not in NAV_WORDS:
                fields['name'] = name

            # Link
            link = block.css('a::attr(href)').get('')
            if link and not link.startswith(('javascript:', '#', 'mailto:', 'tel:')):
                fields['detail_url'] = urljoin(response.url, link)

            # Scan for structured data
            block_html = block.get()
            scan = full_text + ' ' + block_html

            emails = [e for e in EMAIL_RE.findall(scan) if _clean_email(e)]
            phones = PHONE_INDIAN_RE.findall(scan) or PHONE_INTL_RE.findall(scan) or PHONE_FORMATTED_RE.findall(scan)
            prices = PRICE_RE.findall(full_text)
            gsts = GST_RE.findall(scan)

            if emails:
                fields['email'] = _clean_email(emails[0])
            if phones:
                fields['phone'] = _clean_phone(phones[0])
            if prices:
                fields['price'] = prices[0].strip()
            if gsts:
                fields['gst'] = gsts[0]

            # Extract from CSS classes
            for sel_str, key in [
                ('[class*=price]::text, .price::text, [class*=fare]::text', 'price'),
                ('[class*=rating]::text, .rating::text, [class*=score]::text', 'rating'),
                ('[class*=address]::text, .address::text', 'address'),
                ('[class*=location]::text, .location::text, [class*=city]::text', 'location'),
                ('[class*=date]::text, time::text, [class*=depart]::text, [class*=arrive]::text', 'date'),
                ('[class*=duration]::text, [class*=travel-time]::text', 'duration'),
                ('[class*=airline]::text, [class*=carrier]::text, [class*=operator]::text', 'airline'),
                ('[class*=origin]::text, [class*=depart]::text', 'from'),
                ('[class*=destination]::text, [class*=arrive]::text', 'to'),
                ('[class*=stop]::text, [class*=layover]::text, [class*=transfer]::text', 'stops'),
                ('[class*=cabin]::text, [class*=class]::text', 'cabin'),
                ('[class*=hotel]::text, [class*=property-name]::text', 'hotel'),
                ('[class*=room]::text, [class*=accommodation]::text', 'room'),
            ]:
                val = block.css(sel_str).get('')
                if val and val.strip() and key not in fields:
                    fields[key] = val.strip()[:300]

            # Data attributes
            for attr in ('data-name', 'data-company', 'data-title',
                         'data-price', 'data-id', 'data-rating'):
                val = block.attrib.get(attr, '').strip()
                if val:
                    fields[attr.replace('data-', '')] = val

            # Indian directory button pattern
            for btn in block.css('button[title]'):
                title = btn.attrib.get('title', '')
                if '#' in title:
                    parts = title.split('#')
                    if len(parts) >= 3 and len(parts[2].strip()) > 2:
                        fields['name'] = parts[2].strip()

            # QUALITY GATE: must have at least one real data field
            real = {k for k in fields
                    if k not in ('name', 'detail_url') and fields[k]}
            if not real and not fields.get('name'):
                continue
            # If we only have name + detail_url, skip
            if not real:
                continue

            item = self._yield_item(response, **fields)
            if item:
                yield item

    def _score_html_blocks(self, items):
        """Score a set of HTML blocks by data richness."""
        score = 0
        sample = items[:min(8, len(items))]
        count = len(items)

        for block in sample:
            btxt = ' '.join(block.css('::text').getall())
            if EMAIL_RE.search(btxt) or PHONE_INDIAN_RE.search(btxt):
                score += 4
            elif PHONE_INTL_RE.search(btxt) or PHONE_FORMATTED_RE.search(btxt):
                score += 3
            if PRICE_RE.search(btxt):
                score += 4
            if block.css('[class*=price], [class*=rating], [class*=address]'):
                score += 2
            if len(btxt) > 80:
                score += 1

        return (score / len(sample)) * min(count, 50) if sample else 0

    def _extract_name(self, block):
        """Extract most likely name/title from an HTML block."""
        for attr in ('data-name', 'data-company', 'data-title'):
            val = block.attrib.get(attr, '').strip()
            if val and len(val) > 2:
                return val

        for btn in block.css('button[title]'):
            title = btn.attrib.get('title', '')
            if '#' in title:
                parts = title.split('#')
                if len(parts) >= 3 and len(parts[2].strip()) > 2:
                    return parts[2].strip()

        for sel in [
            'h1 a::text', 'h2 a::text', 'h3 a::text', 'h4 a::text',
            'h1::text', 'h2::text', 'h3::text', 'h4::text',
            '.name::text', '.title::text', '.company-name::text',
            '[class*=name]::text', '[itemprop=name]::text',
            'strong::text', 'b::text',
        ]:
            found = block.css(sel).get('')
            if found and 3 < len(found.strip()) < 150:
                return found.strip()

        for a in block.css('a'):
            text = a.css('::text').get('').strip()
            if text and 3 < len(text) < 150:
                cls = (a.attrib.get('class', '') or '').lower()
                if not any(w in cls for w in ('btn', 'button', 'nav', 'menu', 'icon')):
                    return text

        return ''

    # ══════════════════════════════════════════════════════
    # Strategy 4: Tables
    # ══════════════════════════════════════════════════════

    def _extract_tables(self, response):
        for table in response.css('table'):
            headers = [
                h.css('::text').get('').strip()
                for h in table.css('thead th, tr:first-child th, tr:first-child td')
            ]
            if not headers or len(headers) < 2:
                continue

            rows = table.css('tbody tr, tr')[1:]
            if len(rows) < 2:
                continue

            for row in rows:
                cells = row.css('td')
                if len(cells) < 2:
                    continue

                fields = {}
                for i, cell in enumerate(cells):
                    header = headers[i] if i < len(headers) else f'col_{i}'
                    val = cell.css('::text').get('').strip()
                    if val:
                        key = re.sub(r'[^a-zA-Z0-9_]', '_', header.lower()).strip('_') or f'col_{i}'
                        fields[key] = val[:500]

                    link = cell.css('a::attr(href)').get('')
                    if link and not link.startswith(('#', 'javascript:')):
                        lkey = f'{key}_url' if 'key' in dir() else f'col_{i}_url'
                        fields[lkey] = urljoin(response.url, link)

                if len(fields) >= 2:
                    first_key = list(fields.keys())[0]
                    if 'name' not in fields:
                        fields['name'] = fields[first_key]

                    item = self._yield_item(response, **fields)
                    if item:
                        yield item

    # ══════════════════════════════════════════════════════
    # Strategy 5: Contact extraction (last resort)
    # ══════════════════════════════════════════════════════

    def _extract_contacts(self, response):
        """Extract all contact info from a page. Only runs when nothing else found."""
        raw_html = response.text
        body_text = ' '.join(response.css('body *::text').getall())

        emails = [e for e in EMAIL_RE.findall(raw_html) if _clean_email(e)]
        phones = PHONE_INDIAN_RE.findall(raw_html) or PHONE_INTL_RE.findall(raw_html) or PHONE_FORMATTED_RE.findall(raw_html)
        gsts = GST_RE.findall(raw_html)

        if not emails and not phones:
            return

        fields = {}

        title = (
            response.css('meta[property="og:site_name"]::attr(content)').get('')
            or response.css('meta[property="og:title"]::attr(content)').get('')
            or response.css('title::text').get('')
        ).strip()
        if title:
            for sep in [' | ', ' - ', ' :: ', ' — ', ' – ']:
                if sep in title:
                    title = title.split(sep)[0].strip()
            fields['name'] = title

        desc = (
            response.css('meta[name=description]::attr(content)').get('')
            or response.css('meta[property="og:description"]::attr(content)').get('')
        )
        if desc:
            fields['description'] = desc[:500]

        if emails:
            fields['email'] = _clean_email(emails[0])
            if len(emails) > 1:
                fields['email_2'] = _clean_email(emails[1])
        if phones:
            fields['phone'] = _clean_phone(phones[0])
            if len(phones) > 1:
                fields['phone_2'] = _clean_phone(phones[1])
        if gsts:
            fields['gst'] = gsts[0]

        canonical = response.css('link[rel=canonical]::attr(href)').get('')
        if canonical:
            fields['url'] = canonical

        item = self._yield_item(response, **fields)
        if item:
            yield item

    # ══════════════════════════════════════════════════════
    # Link following
    # ══════════════════════════════════════════════════════

    def _follow_links(self, response, current_depth):
        seen = set()
        priority_words = [
            'page=', 'pageno=', '/page/', 'p=', 'start=', 'offset=',
            'listing', 'directory', 'companies', 'search', 'results',
            'members', 'vendors', 'suppliers', 'business', 'category',
            'profile', 'company', 'detail', 'product', 'flight',
            'hotel', 'explore', 'browse', 'all',
        ]

        for link in response.css('a[href]'):
            href = link.attrib.get('href', '')
            if not href or href.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
                continue

            full_url = urljoin(response.url, href)
            parsed = urlparse(full_url)

            if parsed.netloc != self.base_domain:
                continue

            skip_exts = (
                '.jpg', '.jpeg', '.png', '.gif', '.pdf', '.css', '.js',
                '.svg', '.ico', '.woff', '.woff2', '.ttf', '.mp4', '.mp3',
            )
            if any(full_url.lower().endswith(ext) for ext in skip_exts):
                continue

            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if clean_url in seen:
                continue
            seen.add(clean_url)

            is_priority = any(p in full_url.lower() for p in priority_words)

            yield scrapy.Request(
                url=full_url,
                callback=self.parse_page,
                meta={
                    'playwright': True,
                    'playwright_include_page': True,
                    'playwright_page_methods': [
                        {"method": "wait_for_load_state", "args": ["networkidle"],
                         "kwargs": {"timeout": 15000}},
                    ],
                    'depth': current_depth + 1,
                },
                errback=self.errback_close_page,
                priority=1 if is_priority else 0,
                dont_filter=False,
            )
