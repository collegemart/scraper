"""Enrichment spider: visits company websites and extracts contact emails
and phone numbers (info@, hr@, admin@, travel@, procurement@)."""

import csv
import os
from datetime import datetime
from urllib.parse import urlparse

import scrapy

from bharatfare.items import LeadItem
from bharatfare.utils import (
    extract_contact_from_response,
    pick_best_email,
    filter_target_emails,
    CONTACT_PATHS,
)


class WebsiteEmailsSpider(scrapy.Spider):
    """Phase 4 enrichment spider.

    Reads the merged CSV from earlier phases, visits each company_website,
    and extracts target email addresses and phone numbers from homepage +
    contact pages.
    """

    name = "website_emails"

    custom_settings = {
        'DOWNLOAD_DELAY': 1.0,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_TIMEOUT': 15,
        'RETRY_TIMES': 1,
        'ROBOTSTXT_OBEY': False,
        'STRICT_CONTACT_REQUIRED': True,
        'ITEM_PIPELINES': {
            'bharatfare.pipelines.CleaningPipeline': 100,
            'bharatfare.pipelines.DropIncompletePipeline': 200,
            'bharatfare.pipelines.DeduplicationPipeline': 300,
            'bharatfare.pipelines.TravelLeadScoringPipeline': 400,
        },
    }

    def __init__(self, input_csv=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_csv = input_csv or self._find_latest_csv()

    def _find_latest_csv(self):
        output_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.abspath(__file__)
            ))),
            'output',
        )
        candidates = []
        if os.path.isdir(output_dir):
            for f in os.listdir(output_dir):
                if f.endswith('.csv'):
                    path = os.path.join(output_dir, f)
                    candidates.append((os.path.getmtime(path), path))
        if candidates:
            candidates.sort(reverse=True)
            return candidates[0][1]
        return None

    def start_requests(self):
        if not self.input_csv or not os.path.exists(self.input_csv):
            self.logger.error(
                f"Input CSV not found: {self.input_csv}. "
                f"Pass -a input_csv=path/to/file.csv"
            )
            return

        self.logger.info(f"Reading leads from: {self.input_csv}")
        seen_domains = set()
        stats = {'total': 0, 'has_website': 0, 'has_contact': 0, 'to_visit': 0}

        with open(self.input_csv, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stats['total'] += 1
                website = (row.get('company_website') or '').strip()
                has_email = bool((row.get('contact_email') or '').strip())
                has_phone = bool((row.get('phone') or '').strip())

                if not website:
                    continue
                stats['has_website'] += 1

                if has_email and has_phone:
                    stats['has_contact'] += 1
                    continue

                # Normalize URL
                if not website.startswith('http'):
                    website = f"https://{website}"

                domain = urlparse(website).netloc.lower()
                if domain in seen_domains or not domain:
                    continue
                seen_domains.add(domain)
                stats['to_visit'] += 1

                yield scrapy.Request(
                    url=website,
                    callback=self.parse_homepage,
                    cb_kwargs={'row': dict(row), 'base_url': website},
                    meta={'impersonate': 'chrome124'},
                    dont_filter=True,
                    errback=self.errback_log,
                )

        self.logger.info(
            f"CSV: {stats['total']} rows, {stats['has_website']} with websites, "
            f"{stats['has_contact']} already have contacts, "
            f"{stats['to_visit']} unique domains to visit"
        )

    def errback_log(self, failure):
        self.logger.debug(f"Failed: {failure.request.url}")

    def parse_homepage(self, response, row, base_url):
        contact = extract_contact_from_response(response)

        targets, _ = filter_target_emails(contact['all_emails'])
        has_good_email = bool(targets)
        has_phone = bool(contact['all_phones'])

        # If we have both a target email and phone, yield immediately
        if has_good_email and has_phone:
            yield self._make_item(row, contact, response.url)
            return

        # Try to find contact page link on current page
        contact_links = self._find_contact_links(response)
        if contact_links:
            for link in contact_links[:3]:
                yield scrapy.Request(
                    url=response.urljoin(link),
                    callback=self.parse_contact_page,
                    cb_kwargs={
                        'row': row,
                        'homepage_contact': contact,
                        'base_url': base_url,
                    },
                    meta={'impersonate': 'chrome124'},
                    dont_filter=True,
                    errback=self.errback_log,
                )
            return

        # No contact links found — try common paths
        parsed = urlparse(base_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        yielded_contact_req = False
        for path in CONTACT_PATHS[:4]:
            contact_url = f"{origin}{path}"
            if contact_url != response.url:
                yield scrapy.Request(
                    url=contact_url,
                    callback=self.parse_contact_page,
                    cb_kwargs={
                        'row': row,
                        'homepage_contact': contact,
                        'base_url': base_url,
                    },
                    meta={'impersonate': 'chrome124'},
                    dont_filter=True,
                    errback=self.errback_log,
                )
                yielded_contact_req = True
                break  # Try one path at a time

        # If no contact path to try, yield whatever we have
        if not yielded_contact_req and (contact['best_email'] or contact['best_phone']):
            yield self._make_item(row, contact, response.url)

    def parse_contact_page(self, response, row, homepage_contact, base_url):
        page_contact = extract_contact_from_response(response)

        # Merge homepage + contact page results
        all_emails = list(dict.fromkeys(
            homepage_contact['all_emails'] + page_contact['all_emails']
        ))
        all_phones = list(dict.fromkeys(
            homepage_contact['all_phones'] + page_contact['all_phones']
        ))

        targets, others = filter_target_emails(all_emails)
        best_email = pick_best_email(targets + others)
        best_phone = all_phones[0] if all_phones else ''

        merged = {
            'best_email': best_email,
            'all_emails': all_emails,
            'best_phone': best_phone,
            'all_phones': all_phones,
        }

        if best_email or best_phone:
            yield self._make_item(row, merged, response.url)

    def _find_contact_links(self, response):
        """Find contact/about page links on the page."""
        links = []
        for a in response.css('a'):
            href = a.attrib.get('href', '')
            if not href or href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
                continue
            text = (a.css('::text').get('') or '').lower().strip()
            href_lower = href.lower()

            if any(kw in href_lower for kw in (
                'contact', 'reach-us', 'get-in-touch',
            )):
                links.append(href)
            elif any(kw in text for kw in (
                'contact', 'reach us', 'get in touch',
            )):
                links.append(href)
        return links

    def _make_item(self, row, contact, fetched_url):
        """Create a LeadItem with original row data + extracted contact info."""
        item = LeadItem()

        field_map = {
            'company_name': 'company_name',
            'company_website': 'company_website',
            'sector': 'sector',
            'company_size': 'company_size',
            'hq_city': 'hq_city',
            'hq_country': 'hq_country',
            'city': 'city',
            'office_locations': 'office_locations',
            'contact_person': 'contact_person',
            'designation': 'designation',
            'contact_linkedin': 'contact_linkedin',
            'linkedin_company_url': 'linkedin_company_url',
            'company_revenue': 'company_revenue',
            'has_international_hiring': 'has_international_hiring',
            'estimated_travel_frequency': 'estimated_travel_frequency',
            'source_url': 'source_url',
            'industry': 'industry',
            'search_keyword': 'search_keyword',
        }

        for csv_col, item_field in field_map.items():
            val = row.get(csv_col, '')
            if val:
                item[item_field] = val

        # Set extracted contact info
        if contact['best_email']:
            item['contact_email'] = contact['best_email']
            item['email'] = contact['best_email']

        # Keep existing phone if we have one, otherwise use extracted
        existing_phone = (row.get('phone') or '').strip()
        if existing_phone:
            item['phone'] = existing_phone
        elif contact['best_phone']:
            item['phone'] = contact['best_phone']

        item['source'] = row.get('source', 'website_emails')
        item['scraped_date'] = datetime.utcnow().isoformat()

        return item
