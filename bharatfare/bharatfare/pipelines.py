import re
from datetime import datetime

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


# High-travel sectors get higher base scores
SECTOR_TRAVEL_SCORES = {
    'IT': 30,
    'Consulting': 35,
    'Staffing & Recruitment': 25,
    'Shipping & Logistics': 30,
    'Oil & Gas': 35,
    'Pharma & Healthcare': 25,
    'Finance': 30,
    'Manufacturing': 15,
    'Construction': 10,
    'Import / Export': 25,
    'Marketing & Advertising': 15,
    'Events': 20,
    'Real Estate': 10,
}

# Cities that signal high travel / international operations
TARGET_CITIES = {
    'london', 'dubai', 'delhi', 'new delhi', 'bangalore', 'bengaluru',
    'mumbai', 'hyderabad', 'pune', 'gurgaon', 'gurugram', 'noida',
    'chennai', 'singapore', 'hong kong', 'new york',
}


class CleaningPipeline:
    """Normalize and clean all lead fields."""

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Strip whitespace from all string fields
        for field in adapter.field_names():
            value = adapter.get(field)
            if isinstance(value, str):
                adapter[field] = value.strip()

        # Normalize phone numbers
        phone = adapter.get('phone', '')
        if phone:
            digits = re.sub(r'\D', '', str(phone))
            if digits.startswith('91') and len(digits) == 12:
                digits = digits[2:]
            if digits.startswith('0') and len(digits) == 11:
                digits = digits[1:]
            if len(digits) == 10:
                adapter['phone'] = digits
            else:
                adapter['phone'] = phone

        # Normalize company name
        name = adapter.get('company_name', '')
        if name:
            adapter['company_name'] = re.sub(r'\s+', ' ', name).strip()

        # Clean address
        address = adapter.get('address', '')
        if address:
            adapter['address'] = re.sub(r'\s+', ' ', address).strip()

        # Normalize city names
        city_map = {
            'bengaluru': 'Bangalore',
            'new delhi': 'Delhi',
            'delhi ncr': 'Delhi NCR',
            'gurugram': 'Gurgaon',
        }
        city = adapter.get('city', '').lower()
        adapter['city'] = city_map.get(city, adapter.get('city', '').title())

        # Sync hq_city with city if not separately set
        if not adapter.get('hq_city'):
            adapter['hq_city'] = adapter.get('city', '')

        # Set hq_country if missing
        if not adapter.get('hq_country'):
            adapter['hq_country'] = 'India'

        # Copy website <-> company_website for compat
        if adapter.get('website') and not adapter.get('company_website'):
            adapter['company_website'] = adapter.get('website')
        elif adapter.get('company_website') and not adapter.get('website'):
            adapter['website'] = adapter.get('company_website')

        # Copy email <-> contact_email
        if adapter.get('email') and not adapter.get('contact_email'):
            adapter['contact_email'] = adapter.get('email')
        elif adapter.get('contact_email') and not adapter.get('email'):
            adapter['email'] = adapter.get('contact_email')

        # Unify contact_person from role-specific fields
        if not adapter.get('contact_person'):
            adapter['contact_person'] = (
                adapter.get('corporate_travel_manager') or
                adapter.get('hr_manager_name') or
                adapter.get('procurement_manager_name') or
                ''
            )

        # Unify contact_linkedin from role-specific LinkedIn fields
        if not adapter.get('contact_linkedin'):
            adapter['contact_linkedin'] = (
                adapter.get('hr_manager_linkedin') or
                adapter.get('procurement_manager_linkedin') or
                ''
            )

        if not adapter.get('scraped_date'):
            adapter['scraped_date'] = datetime.utcnow().isoformat()

        return item


class DropIncompletePipeline:
    """Drop items missing minimum required fields.
    Requires company_name. Contact info (email/phone) is preferred but not
    mandatory during initial scraping — the website_emails enrichment spider
    handles that. Use STRICT_CONTACT_REQUIRED=True to enforce."""

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if not adapter.get('company_name'):
            raise DropItem("Missing company_name")

        # In strict mode (website_emails spider), require contact info
        strict = spider.settings.getbool('STRICT_CONTACT_REQUIRED', False)
        if strict:
            has_email = bool(
                (adapter.get('contact_email') or adapter.get('email') or '').strip()
            )
            has_phone = bool((adapter.get('phone') or '').strip())
            if not has_email and not has_phone:
                raise DropItem(
                    f"No contact info for {adapter.get('company_name')}"
                )

        return item


class DeduplicationPipeline:
    """Drop duplicate companies using:
    1. source:profile_id exact match
    2. Normalized company name (strips Pvt/Ltd/LLP/Inc/etc.) + city
    3. Website domain match (two companies sharing same domain = duplicate)
    """

    # Suffixes to strip for fuzzy name matching
    STRIP_SUFFIXES = re.compile(
        r'\b(pvt|private|ltd|limited|llp|inc|incorporated|corp|corporation|'
        r'co|company|opc|llc|plc|gmbh|ag|sa|srl|bv|nv|pty)\b',
        re.IGNORECASE,
    )

    def __init__(self):
        self.seen_ids = set()
        self.seen_names = set()
        self.seen_domains = set()

    def _normalize_name(self, name):
        """Normalize company name for fuzzy dedup."""
        name = name.lower().strip()
        name = self.STRIP_SUFFIXES.sub('', name)
        name = re.sub(r'[.,\-()&]+', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        return name

    def _extract_domain(self, url):
        """Extract root domain from URL."""
        if not url:
            return ''
        try:
            from urllib.parse import urlparse
            netloc = urlparse(url).netloc.lower()
            netloc = netloc.lstrip('www.')
            return netloc
        except Exception:
            return ''

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # 1. Exact match on source:profile_id
        source = adapter.get('source', '')
        profile_id = adapter.get('profile_id', '')
        if source and profile_id:
            key = f"{source}:{profile_id}"
            if key in self.seen_ids:
                raise DropItem(f"Duplicate profile_id: {key}")
            self.seen_ids.add(key)

        # 2. Normalized name + city match
        name = (adapter.get('company_name', '') or '').strip()
        city = (adapter.get('city', '') or '').lower().strip()
        if name:
            norm_key = f"{self._normalize_name(name)}|{city}"
            if norm_key in self.seen_names:
                raise DropItem(f"Duplicate company (normalized): {name} in {city}")
            self.seen_names.add(norm_key)

        # 3. Website domain dedup — only for real company websites
        website = adapter.get('company_website') or adapter.get('website') or ''
        domain = self._extract_domain(website)
        # Skip dedup for: empty domains, directory profile pages, or social media
        skip_domains = {
            'indiamart.com', 'dir.indiamart.com', 'tradeindia.com',
            'exportersindia.com', 'indianyellowpages.com', 'justdial.com',
            'clutch.co', 'goodfirms.co', 'fundoodata.com', 'sulekha.com',
            'linkedin.com', 'facebook.com', 'twitter.com',
        }
        is_directory_url = any(d in domain for d in skip_domains)
        if domain and domain not in ('', 'localhost') and not is_directory_url:
            if domain in self.seen_domains:
                raise DropItem(f"Duplicate domain: {domain} for {name}")
            self.seen_domains.add(domain)

        return item


class TravelLeadScoringPipeline:
    """Score each lead 0-100 for corporate travel potential.

    Scored on:
      - Sector (0-35 pts)
      - Company size (0-25 pts)
      - Revenue / turnover (0-15 pts)
      - Office presence in target cities (0-15 pts)
      - Has contact info (0-10 pts)
    """

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        score = 0

        # 1. Sector score (0-35)
        sector = (adapter.get('sector') or '').strip()
        score += SECTOR_TRAVEL_SCORES.get(sector, 5)

        # 2. Company size score (0-25)
        size_str = str(adapter.get('company_size') or '').lower().replace(',', '')
        emp_count = self._parse_employee_count(size_str)
        if emp_count >= 1000:
            score += 25
        elif emp_count >= 500:
            score += 20
        elif emp_count >= 100:
            score += 15
        elif emp_count >= 50:
            score += 10
        elif emp_count >= 10:
            score += 5

        # 3. Revenue / turnover score (0-15)
        revenue_str = str(
            adapter.get('company_revenue') or adapter.get('annual_turnover') or ''
        ).lower()
        if any(t in revenue_str for t in ('crore', 'cr', 'billion', 'bn')):
            score += 15
        elif any(t in revenue_str for t in ('lakh', 'lac', 'million')):
            score += 8

        # 4. Target city presence (0-15)
        all_locations = ' '.join([
            adapter.get('city') or '',
            adapter.get('hq_city') or '',
            adapter.get('office_locations') or '',
            adapter.get('address') or '',
        ]).lower()
        target_hits = sum(1 for c in TARGET_CITIES if c in all_locations)
        score += min(target_hits * 5, 15)

        # Also set target_office_presence flag
        found_targets = [c for c in ['london', 'dubai', 'delhi', 'bangalore']
                         if c in all_locations]
        if found_targets:
            adapter['target_office_presence'] = ', '.join(
                t.title() for t in found_targets
            )

        # 5. Contact info completeness (0-10)
        if adapter.get('email') or adapter.get('contact_email'):
            score += 4
        if adapter.get('phone'):
            score += 3
        if adapter.get('contact_person'):
            score += 3

        # Clamp to 0-100
        score = max(0, min(100, score))
        adapter['travel_score'] = score

        # Set estimated travel frequency
        if score >= 65:
            adapter['estimated_travel_frequency'] = 'High'
        elif score >= 40:
            adapter['estimated_travel_frequency'] = 'Medium'
        else:
            adapter['estimated_travel_frequency'] = 'Low'

        # Flag international hiring based on sector + size
        # Also respect pre-set values from signal spiders (e.g. Indeed)
        existing_hiring = (adapter.get('has_international_hiring') or '').strip().upper()
        if existing_hiring in ('Y', 'YES'):
            adapter['has_international_hiring'] = 'Y'
        elif emp_count >= 50 and sector in ('IT', 'Consulting', 'Oil & Gas', 'Finance',
                                             'Staffing & Recruitment', 'Pharma & Healthcare'):
            adapter['has_international_hiring'] = 'Y'
        else:
            adapter['has_international_hiring'] = 'N'

        return item

    @staticmethod
    def _parse_employee_count(s):
        """Try to extract a numeric employee count from strings like
        '100-500', '500+', '1000 employees', 'Above 250', etc."""
        if not s:
            return 0
        # Remove common words
        s = s.replace('employees', '').replace('people', '').replace('above', '')
        s = s.replace('+', '').strip()

        # Try range like "100-500" -> take upper bound
        range_match = re.search(r'(\d+)\s*[-to]+\s*(\d+)', s)
        if range_match:
            return int(range_match.group(2))

        # Try single number
        num_match = re.search(r'(\d+)', s)
        if num_match:
            return int(num_match.group(1))
        return 0
