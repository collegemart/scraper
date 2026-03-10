#!/usr/bin/env python3
"""
Advanced Data Enrichment Script for BharatFare Corporate Travel Leads.

Pipeline:
  1. Read scraped leads CSV (from Scrapy spiders)
  2. For each company:
     a. If no website: search DuckDuckGo/Google for official URL + LinkedIn page
     b. Scrape website homepage + contact pages for emails/phones
     c. If contacts not found on website (paywall/login): deep Google search
        for "{company name} contact email phone" to find contacts from
        third-party directories, LinkedIn, etc.
     d. Apply 3-tier email priority (Tier1: info@/hr@/admin@/travel@,
        Tier2: company domain emails, Tier3: personal gmail/yahoo)
     e. At least ONE contact (email or phone) must be present
  3. Save enriched leads with 16-column outreach-ready format

Usage:
  python enrich_leads.py [input.csv] [output.csv]
  python enrich_leads.py --workers 8 --limit 100 input.csv output.csv
"""

import csv
import os
import re
import sys
import time
import random
import warnings
import argparse
from datetime import datetime
from urllib.parse import urlparse, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore', message='.*renamed.*ddgs.*')
warnings.filterwarnings('ignore', category=RuntimeWarning)

from curl_cffi import requests as cf_requests
from duckduckgo_search import DDGS

# ═══════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════

REQUEST_TIMEOUT = 10
MAX_SEARCH_RETRIES = 2
MIN_DELAY = 0.5
MAX_DELAY = 2.0
SAVE_EVERY = 10
DDG_FAIL_THRESHOLD = 5
DDG_COOLDOWN = 60

# CSV output columns (16 columns for outreach) — same as before
OUTPUT_FIELDS = [
    'company_name', 'company_website', 'sector', 'company_size', 'hq_city',
    'office_locations', 'contact_email', 'contact_person', 'designation',
    'contact_linkedin', 'phone', 'linkedin_company_url', 'company_revenue',
    'has_international_hiring', 'estimated_travel_frequency', 'source_url',
]

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
]

# ═══════════════════════════════════════════════════════════════════════
# EMAIL EXTRACTION (3-TIER PRIORITY SYSTEM)
# ═══════════════════════════════════════════════════════════════════════

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

# Tier 1: targeted department prefixes (ordered by outreach priority)
TIER1_PREFIXES = (
    'travel@', 'procurement@', 'hr@', 'admin@', 'info@',
    'contact@', 'sales@', 'office@', 'corporate@', 'business@',
    'enquiry@', 'inquiry@', 'careers@', 'marketing@', 'support@',
    'bookings@', 'reservations@',
)

# Junk patterns to discard
JUNK_EMAIL_RE = re.compile(
    r'(example\.com|sentry\.io|cloudflare|gravatar|schema\.org|'
    r'googleapis|jquery|bootstrap|facebook\.com|twitter\.com|instagram|'
    r'wixpress|wordpress|w3\.org|github\.com|\.png|\.jpg|\.gif|'
    r'\.svg|\.css|\.js$|noreply|no-reply|unsubscribe|'
    r'webpack|localhost|placeholder|test@|dummy@|@sentry|'
    r'@wix\.com|@mailchimp|@sendgrid|@hubspot|@freshdesk|'
    r'@google\.com|@facebook\.com|@twitter\.com)',
    re.IGNORECASE,
)

GENERIC_DOMAINS = {
    'gmail.com', 'yahoo.com', 'yahoo.co.in', 'hotmail.com',
    'outlook.com', 'rediffmail.com', 'aol.com', 'mail.com',
    'ymail.com', 'protonmail.com', 'zoho.com', 'icloud.com',
    'live.com', 'msn.com', 'rocketmail.com',
}


def extract_all_emails(text):
    """Extract all valid emails from text, filtering junk."""
    if not text:
        return []
    raw = EMAIL_RE.findall(text)
    seen = set()
    result = []
    for email in raw:
        email = email.lower().strip().rstrip('.')
        if email in seen:
            continue
        seen.add(email)
        if JUNK_EMAIL_RE.search(email):
            continue
        if len(email) > 60 or len(email.split('.')[-1]) < 2:
            continue
        result.append(email)
    return result


def classify_emails_3tier(emails, company_domain=None):
    """Classify emails into 3 tiers based on the company domain.

    Returns: (tier1_list, tier2_list, tier3_list)
      Tier 1: targeted dept prefix on company domain
      Tier 2: any email on company domain or non-generic domain
      Tier 3: generic gmail/yahoo/etc emails
    """
    tier1, tier2, tier3 = [], [], []

    for email in emails:
        domain = email.split('@')[1] if '@' in email else ''

        is_company_domain = False
        if company_domain:
            cd = company_domain.lower().lstrip('www.')
            if domain == cd or domain.endswith('.' + cd):
                is_company_domain = True

        if is_company_domain:
            if any(email.startswith(prefix) for prefix in TIER1_PREFIXES):
                tier1.append(email)
            else:
                tier2.append(email)
        elif domain in GENERIC_DOMAINS:
            tier3.append(email)
        else:
            # Unknown domain — treat as tier2 if looks legit
            tier2.append(email)

    return tier1, tier2, tier3


def pick_best_email_3tier(emails, company_domain=None):
    """Pick the single best email using 3-tier priority.

    Returns (best_email, tier_number) or ('', 0) if none found.
    """
    tier1, tier2, tier3 = classify_emails_3tier(emails, company_domain)

    if tier1:
        for prefix in TIER1_PREFIXES:
            for e in tier1:
                if e.startswith(prefix):
                    return e, 1
        return tier1[0], 1

    if tier2:
        return tier2[0], 2

    # Tier 3: personal emails are fine (user said "gmail or anything will work")
    if tier3:
        return tier3[0], 3

    return '', 0


# ═══════════════════════════════════════════════════════════════════════
# PHONE EXTRACTION
# ═══════════════════════════════════════════════════════════════════════

TEL_HREF_RE = re.compile(r'href=["\']tel:([^"\']+)["\']', re.IGNORECASE)
MAILTO_RE = re.compile(r'href=["\']mailto:([^"\'?]+)["\']', re.IGNORECASE)
PHONE_LABEL_RE = re.compile(
    r'(?:phone|tel|mobile|call|fax|whatsapp|contact\s*(?:no|number|#))\s*'
    r'[:.)\-\s]*\s*(\+?[\d][\d\s\-\.\(\)]{7,18}\d)',
    re.IGNORECASE,
)


def extract_phones(text):
    """Extract phone numbers from HTML text."""
    if not text:
        return []
    phones = set()

    for m in TEL_HREF_RE.finditer(text):
        cleaned = _clean_phone(m.group(1).strip())
        if cleaned:
            phones.add(cleaned)

    for m in PHONE_LABEL_RE.finditer(text):
        cleaned = _clean_phone(m.group(1).strip())
        if cleaned:
            phones.add(cleaned)

    return list(phones)


def _clean_phone(raw):
    """Validate and clean a phone number string."""
    digits = re.sub(r'[\s\-\.\(\)\+]', '', raw)
    check = digits
    if check.startswith('91') and len(check) >= 12:
        check = check[2:]
    elif check.startswith('0'):
        check = check[1:]
    elif check.startswith('44') and len(check) >= 12:
        check = check[2:]
    elif check.startswith('971') and len(check) >= 12:
        check = check[3:]
    elif check.startswith('1') and len(check) == 11:
        check = check[1:]  # US/Canada

    if 7 <= len(check) <= 12 and check.isdigit():
        if raw.lstrip().startswith('+'):
            return '+' + digits
        return digits
    return ''


# ═══════════════════════════════════════════════════════════════════════
# WEB SEARCH (DuckDuckGo) — also used as deep Google fallback
# ═══════════════════════════════════════════════════════════════════════

import threading
_ddg_lock = threading.Lock()
_ddg_consecutive_failures = 0
_ddg_disabled_until = 0


def _ddg_search(query, max_results=5):
    """Search DuckDuckGo and return list of {title, href, body} dicts."""
    global _ddg_consecutive_failures, _ddg_disabled_until

    with _ddg_lock:
        if _ddg_consecutive_failures >= DDG_FAIL_THRESHOLD:
            if time.time() < _ddg_disabled_until:
                return []
            _ddg_consecutive_failures = 0

    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results))
        with _ddg_lock:
            _ddg_consecutive_failures = 0
        return results
    except Exception as e:
        with _ddg_lock:
            _ddg_consecutive_failures += 1
            if _ddg_consecutive_failures >= DDG_FAIL_THRESHOLD:
                _ddg_disabled_until = time.time() + DDG_COOLDOWN
        return []


def search_company_website(company_name, city=''):
    """Search for a company's official website and LinkedIn page.

    Returns: (website_url, linkedin_url)
    """
    query = f'"{company_name}"'
    if city:
        query += f' {city}'
    query += ' official website'

    website = ''
    linkedin = ''
    results = []

    for attempt in range(MAX_SEARCH_RETRIES):
        results = _ddg_search(query, max_results=8)
        if results:
            break
        time.sleep(random.uniform(2, 5))

    skip_domains = {
        'linkedin.com', 'facebook.com', 'twitter.com', 'instagram.com',
        'youtube.com', 'wikipedia.org', 'crunchbase.com', 'glassdoor.com',
        'ambitionbox.com', 'indeed.com', 'justdial.com', 'sulekha.com',
        'indiamart.com', 'tradeindia.com', 'exportersindia.com',
        'indianyellowpages.com', 'clutch.co', 'goodfirms.co',
        'zaubacorp.com', 'tofler.in', 'fundoodata.com',
        'google.com', 'bing.com', 'yahoo.com', 'reddit.com',
        'quora.com', 'medium.com',
        'naukri.com', 'mouthshut.com', 'trustpilot.com',
        'g2.com', 'capterra.com', 'dnb.com', 'owler.com',
    }

    company_lower = company_name.lower().strip()
    company_words = set(
        w for w in re.split(r'[\s.,()-]+', company_lower)
        if len(w) > 2 and w not in ('pvt', 'ltd', 'llp', 'inc', 'the', 'and', 'opc')
    )

    for r in results:
        url = r.get('href', '') or r.get('link', '')
        title = (r.get('title', '') or '').lower()
        body = (r.get('body', '') or '').lower()
        if not url:
            continue

        parsed = urlparse(url)
        domain = parsed.netloc.lower().lstrip('www.')

        if 'linkedin.com/company' in url and not linkedin:
            linkedin = url
            continue

        if any(sd in domain for sd in skip_domains):
            continue

        snippet = title + ' ' + body + ' ' + domain
        matching_words = sum(1 for w in company_words if w in snippet)
        if matching_words < min(2, len(company_words)):
            continue

        if not website and domain:
            website = f"{parsed.scheme}://{parsed.netloc}"

    return website, linkedin


def deep_search_contacts(company_name, city='', website=''):
    """DEEP SEARCH: When website doesn't show contacts (paywall/login/hidden),
    search multiple queries on DuckDuckGo to find contacts from third-party
    sources, directories, LinkedIn, etc.

    Returns: (email, phone, contact_person, linkedin_url)
    """
    email = ''
    phone = ''
    contact_person = ''
    linkedin_url = ''

    # Build multiple search queries for better coverage
    queries = []

    # Query 1: Direct contact search
    q1 = f'"{company_name}" contact email phone'
    if city:
        q1 += f' {city}'
    queries.append(q1)

    # Query 2: Specific email prefixes
    q2 = f'"{company_name}" "info@" OR "contact@" OR "hr@" OR "admin@" OR "sales@"'
    queries.append(q2)

    # Query 3: Search on directories that might have public contact
    q3 = f'"{company_name}" site:justdial.com OR site:indiamart.com OR site:sulekha.com OR site:fundoodata.com'
    queries.append(q3)

    # Query 4: LinkedIn company page (for person names)
    q4 = f'"{company_name}" site:linkedin.com/company'
    queries.append(q4)

    # Query 5: Search for key decision makers
    q5 = f'"{company_name}" "managing director" OR "CEO" OR "HR manager" OR "admin" email'
    queries.append(q5)

    # Query 6: Use company website domain if known
    if website:
        domain = _extract_domain(website)
        if domain:
            q6 = f'"@{domain}" email contact'
            queries.append(q6)

    all_emails = []
    all_phones = []
    all_text = ''

    for query in queries:
        _random_delay(1.0, 2.5)
        results = _ddg_search(query, max_results=5)

        for r in results:
            title = r.get('title', '') or ''
            body = r.get('body', '') or ''
            href = r.get('href', '') or ''
            snippet = f"{title} {body}"
            all_text += ' ' + snippet

            # Capture LinkedIn
            if 'linkedin.com/company' in href and not linkedin_url:
                linkedin_url = href

            # Capture LinkedIn person profile
            if 'linkedin.com/in/' in href and not contact_person:
                # Extract name from title like "John Doe - CEO - Company | LinkedIn"
                name_match = re.match(r'^([A-Za-z\s\.]+?)(?:\s*[-|–])', title)
                if name_match:
                    contact_person = name_match.group(1).strip()

            # Extract emails from snippets
            snippet_emails = extract_all_emails(snippet)
            all_emails.extend(snippet_emails)

            # Extract phones from snippets
            snippet_phones = extract_phones(snippet)
            all_phones.extend(snippet_phones)

    # Also try to scrape the top directory result pages
    directory_urls = []
    for query in queries[:2]:
        results = _ddg_search(query, max_results=3)
        for r in results:
            href = r.get('href', '') or ''
            if href and any(d in href for d in ('justdial.com', 'sulekha.com', 'yellowpages')):
                directory_urls.append(href)

    for dir_url in directory_urls[:2]:
        _random_delay(0.5, 1.5)
        html, _ = _fetch(dir_url)
        if html:
            dir_emails = extract_all_emails(html)
            dir_phones = extract_phones(html)
            all_emails.extend(dir_emails)
            all_phones.extend(dir_phones)

    # Deduplicate
    all_emails = list(dict.fromkeys(all_emails))
    all_phones = list(dict.fromkeys(all_phones))

    # Pick best email
    company_domain = _extract_domain(website) if website else ''
    if all_emails:
        email, _ = pick_best_email_3tier(all_emails, company_domain)

    if all_phones:
        phone = all_phones[0]

    return email, phone, contact_person, linkedin_url


def search_contact_fallback(company_name, city=''):
    """Fallback: search for contact info in search snippets.

    Returns: (email, phone) extracted from snippet text.
    """
    query = f'"{company_name}"'
    if city:
        query += f' "{city}"'
    query += ' contact email OR phone number'

    results = _ddg_search(query, max_results=5)

    all_text = ' '.join(
        f"{r.get('title', '')} {r.get('body', '')}" for r in results
    )

    emails = extract_all_emails(all_text)
    phones = extract_phones(all_text)

    best_email = emails[0] if emails else ''
    best_phone = phones[0] if phones else ''

    return best_email, best_phone


# ═══════════════════════════════════════════════════════════════════════
# WEBSITE SCRAPING
# ═══════════════════════════════════════════════════════════════════════

CONTACT_PATHS = [
    '/contact', '/contact-us', '/contactus', '/contact.html',
    '/about/contact', '/about-us', '/about', '/reach-us',
    '/get-in-touch', '/connect',
]

CONTACT_LINK_KEYWORDS = ['contact', 'reach us', 'get in touch', 'connect with']


def _fetch(url, timeout=REQUEST_TIMEOUT):
    """Fetch a URL with Chrome impersonation. Returns (text, final_url) or (None, None)."""
    try:
        resp = cf_requests.get(
            url,
            impersonate='chrome124',
            timeout=timeout,
            allow_redirects=True,
            headers={'User-Agent': random.choice(USER_AGENTS)},
        )
        if resp.status_code == 200:
            text = resp.text[:500_000] if resp.text else ''
            return text, str(resp.url)
        return None, None
    except Exception:
        return None, None


def _extract_domain(url):
    """Extract the base domain from a URL (without www.)."""
    try:
        return urlparse(url).netloc.lower().lstrip('www.')
    except Exception:
        return ''


def _find_contact_links(html, base_url):
    """Find contact/about page URLs in HTML."""
    links = []
    href_re = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.I | re.DOTALL)
    for m in href_re.finditer(html):
        href = m.group(1)
        text = re.sub(r'<[^>]+>', '', m.group(2)).strip().lower()

        if href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
            continue

        href_lower = href.lower()
        is_contact = (
            any(kw in href_lower for kw in ('contact', 'reach-us', 'get-in-touch', 'connect')) or
            any(kw in text for kw in CONTACT_LINK_KEYWORDS)
        )

        if is_contact:
            if href.startswith('http'):
                links.append(href)
            elif href.startswith('/'):
                parsed = urlparse(base_url)
                links.append(f"{parsed.scheme}://{parsed.netloc}{href}")

    return links[:3]


def scrape_website_contacts(url):
    """Scrape a company website for emails and phone numbers.

    Steps:
      1. Fetch homepage
      2. Extract emails + phones from homepage
      3. Find contact/about page links
      4. If no contact links found, try common paths (/contact, /contact-us, etc.)
      5. Scrape contact pages for more emails + phones
      6. Also extract mailto: links from all pages

    Returns dict: {emails: [...], phones: [...], pages_scraped: int}
    """
    result = {'emails': [], 'phones': [], 'pages_scraped': 0}

    html, final_url = _fetch(url)
    if not html:
        return result

    result['pages_scraped'] = 1
    homepage_emails = extract_all_emails(html)
    homepage_phones = extract_phones(html)

    for m in MAILTO_RE.finditer(html):
        email = m.group(1).strip().lower()
        if email and '@' in email and email not in homepage_emails:
            if not JUNK_EMAIL_RE.search(email):
                homepage_emails.append(email)

    result['emails'].extend(homepage_emails)
    result['phones'].extend(homepage_phones)

    has_tier1 = any(any(e.startswith(p) for p in TIER1_PREFIXES) for e in homepage_emails)
    if has_tier1 and homepage_phones:
        result['emails'] = list(dict.fromkeys(result['emails']))
        result['phones'] = list(dict.fromkeys(result['phones']))
        return result

    contact_links = _find_contact_links(html, final_url or url)

    if not contact_links:
        parsed = urlparse(final_url or url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        contact_links = [f"{origin}{path}" for path in CONTACT_PATHS[:4]]

    visited = {(final_url or url).rstrip('/')}
    for link in contact_links:
        normalized = link.rstrip('/')
        if normalized in visited:
            continue
        visited.add(normalized)

        _random_delay(0.5, 1.5)

        page_html, _ = _fetch(link)
        if not page_html:
            continue

        result['pages_scraped'] += 1

        page_emails = extract_all_emails(page_html)
        page_phones = extract_phones(page_html)

        for m in MAILTO_RE.finditer(page_html):
            email = m.group(1).strip().lower()
            if email and '@' in email and email not in page_emails:
                if not JUNK_EMAIL_RE.search(email):
                    page_emails.append(email)

        result['emails'].extend(page_emails)
        result['phones'].extend(page_phones)

        if any(any(e.startswith(p) for p in TIER1_PREFIXES) for e in page_emails):
            break

    result['emails'] = list(dict.fromkeys(result['emails']))
    result['phones'] = list(dict.fromkeys(result['phones']))

    return result


# ═══════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════

def _random_delay(lo=MIN_DELAY, hi=MAX_DELAY):
    """Sleep for a random duration to avoid rate limiting."""
    time.sleep(random.uniform(lo, hi))


def _save_csv(rows, filepath, fields):
    """Save rows to CSV atomically (write to temp, then rename)."""
    tmp = filepath + '.tmp'
    with open(tmp, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    if os.path.exists(filepath):
        os.remove(filepath)
    os.rename(tmp, filepath)


def _find_latest_csv(output_dir):
    """Find the most recent CSV in the output directory."""
    candidates = []
    if os.path.isdir(output_dir):
        for f in os.listdir(output_dir):
            if f.endswith('.csv') and 'enriched' not in f.lower():
                path = os.path.join(output_dir, f)
                candidates.append((os.path.getmtime(path), path))
    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][1]
    return None


# ═══════════════════════════════════════════════════════════════════════
# MAIN ENRICHMENT PIPELINE
# ═══════════════════════════════════════════════════════════════════════

def enrich_single_lead(row, idx=0, total=0):
    """Enrich a single lead with website, email, phone, LinkedIn.

    Returns the enriched row dict. Enforces that at least one contact
    (email or phone) must be present by running deep search fallback.
    """
    company = (row.get('company_name') or '').strip()
    city = (row.get('hq_city') or '').strip()
    existing_website = (row.get('company_website') or '').strip()
    existing_email = (row.get('contact_email') or '').strip()
    existing_phone = (row.get('phone') or '').strip()
    existing_linkedin = (row.get('linkedin_company_url') or '').strip()

    if not company:
        return row

    label = f"[{idx}/{total}] {company}"

    # Skip if already fully enriched
    if existing_email and existing_phone and existing_website:
        print(f"  {label} [SKIP] already has email+phone+website")
        return row

    # ── Step 1: Find website via search if missing ──────────────────
    website_url = existing_website
    linkedin_url = existing_linkedin

    if not website_url or any(d in website_url for d in ('indianyellowpages', 'justdial', 'indiamart')):
        print(f"  {label} [SEARCH] Looking up website...")
        _random_delay(1.5, 3.0)
        found_website, found_linkedin = search_company_website(company, city)
        if found_website:
            website_url = found_website
            row['company_website'] = website_url
            print(f"    → Website: {website_url}")
        if found_linkedin and not linkedin_url:
            linkedin_url = found_linkedin
            row['linkedin_company_url'] = linkedin_url

    # ── Step 2: Scrape website for contact info ─────────────────────
    company_domain = _extract_domain(website_url) if website_url else ''
    best_email = existing_email
    best_phone = existing_phone
    email_tier = 0

    if website_url and website_url.startswith('http'):
        print(f"  {label} [SCRAPE] Crawling website...")
        _random_delay()

        contacts = scrape_website_contacts(website_url)
        print(
            f"    Scraped {contacts['pages_scraped']} pages: "
            f"{len(contacts['emails'])} emails, {len(contacts['phones'])} phones"
        )

        if contacts['emails'] and not best_email:
            best_email, email_tier = pick_best_email_3tier(
                contacts['emails'], company_domain
            )
            if best_email:
                tier_names = {1: 'Tier1-Targeted', 2: 'Tier2-Domain', 3: 'Tier3-Personal'}
                print(f"    → Email: {best_email} ({tier_names.get(email_tier, '?')})")

        if contacts['phones'] and not best_phone:
            best_phone = contacts['phones'][0]
            print(f"    → Phone: {best_phone}")

    # ── Step 3: DEEP SEARCH if still missing contact info ───────────
    # This is the key improvement: when websites hide contacts behind
    # paywalls or login, we search Google/DuckDuckGo extensively
    if not best_email and not best_phone:
        print(f"  {label} [DEEP SEARCH] No contacts on website, searching directories...")
        _random_delay(1.5, 3.0)
        ds_email, ds_phone, ds_person, ds_linkedin = deep_search_contacts(
            company, city, website_url
        )

        if ds_email:
            best_email = ds_email
            print(f"    → Deep Search email: {ds_email}")
        if ds_phone:
            best_phone = ds_phone
            print(f"    → Deep Search phone: {ds_phone}")
        if ds_person and not row.get('contact_person'):
            row['contact_person'] = ds_person
            print(f"    → Contact person: {ds_person}")
        if ds_linkedin and not linkedin_url:
            linkedin_url = ds_linkedin
            row['linkedin_company_url'] = linkedin_url

    # ── Step 4: Last resort — simple snippet search ─────────────────
    if not best_email and not best_phone:
        print(f"  {label} [FALLBACK] Snippet search...")
        _random_delay(1.5, 3.0)
        fb_email, fb_phone = search_contact_fallback(company, city)
        if fb_email:
            best_email = fb_email
            print(f"    → Fallback email: {fb_email}")
        if fb_phone:
            best_phone = fb_phone
            print(f"    → Fallback phone: {fb_phone}")

    # ── Step 5: Update the row ──────────────────────────────────────
    if best_email:
        row['contact_email'] = best_email
    if best_phone:
        row['phone'] = best_phone
    if website_url:
        row['company_website'] = website_url
    if linkedin_url:
        row['linkedin_company_url'] = linkedin_url

    # Log final status
    has_contact = bool(best_email or best_phone)
    status = "✓" if has_contact else "✗ NO CONTACT"
    print(f"  {label} [{status}]")

    return row


def enrich_leads(input_csv, output_csv, max_workers=8, limit=None):
    """Main enrichment loop with parallel processing."""

    print(f"\n{'='*60}")
    print(f"BharatFare Advanced Data Enrichment")
    print(f"{'='*60}")
    print(f"Input:  {input_csv}")
    print(f"Output: {output_csv}")
    print(f"Workers: {max_workers}")

    with open(input_csv, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)
    if limit:
        rows = rows[:limit]
        total = len(rows)
        print(f"Limiting to first {limit} leads")

    print(f"Loaded {total} leads to enrich\n")

    # Pre-scan statistics
    pre_email = sum(1 for r in rows if (r.get('contact_email') or '').strip())
    pre_phone = sum(1 for r in rows if (r.get('phone') or '').strip())
    pre_website = sum(1 for r in rows if (r.get('company_website') or '').strip())
    print(f"Pre-enrichment: {pre_email} emails, {pre_phone} phones, {pre_website} websites")

    # Sort: leads WITH websites first
    rows.sort(key=lambda r: (0 if (r.get('company_website') or '').strip() else 1))
    print(f"Sorted: website-first processing order")
    print(f"{'─'*60}\n")

    # Load already-enriched rows (for resume capability)
    already_done = set()
    enriched_rows = []
    if os.path.exists(output_csv):
        with open(output_csv, 'r', encoding='utf-8', errors='replace') as f:
            existing = list(csv.DictReader(f))
        for r in existing:
            key = (r.get('company_name', '').lower().strip(),
                   r.get('hq_city', '').lower().strip())
            already_done.add(key)
            enriched_rows.append(r)
        print(f"Resuming: {len(already_done)} leads already enriched\n")

    # Filter out already-done leads
    pending_rows = []
    for i, row in enumerate(rows):
        key = (row.get('company_name', '').lower().strip(),
               row.get('hq_city', '').lower().strip())
        if key not in already_done:
            pending_rows.append((i + 1, row))

    print(f"Pending: {len(pending_rows)} leads to process\n")

    new_count = 0
    enriched_email = 0
    enriched_phone = 0
    errors = 0

    # Use ThreadPoolExecutor for parallel enrichment
    if max_workers > 1 and len(pending_rows) > 1:
        print(f"Processing with {max_workers} parallel workers...\n")

        # Process in batches to allow periodic saving
        batch_size = SAVE_EVERY * max_workers
        for batch_start in range(0, len(pending_rows), batch_size):
            batch = pending_rows[batch_start:batch_start + batch_size]

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                for idx, row in batch:
                    future = executor.submit(
                        enrich_single_lead, row, idx, total
                    )
                    futures[future] = (idx, row)

                for future in as_completed(futures):
                    idx, original_row = futures[future]
                    try:
                        enriched = future.result()
                        key = (enriched.get('company_name', '').lower().strip(),
                               enriched.get('hq_city', '').lower().strip())
                        already_done.add(key)
                        enriched_rows.append(enriched)
                        new_count += 1

                        if (enriched.get('contact_email') or '').strip():
                            enriched_email += 1
                        if (enriched.get('phone') or '').strip():
                            enriched_phone += 1

                    except Exception as e:
                        errors += 1
                        print(f"  [ERROR] Lead #{idx}: {e}")
                        enriched_rows.append(original_row)

            # Save after each batch
            _save_csv(enriched_rows, output_csv, OUTPUT_FIELDS)
            print(f"\n  --- Saved {len(enriched_rows)} rows (batch) ---\n")
    else:
        # Sequential processing
        for i, (idx, row) in enumerate(pending_rows):
            try:
                enriched = enrich_single_lead(row, idx, total)
                enriched_rows.append(enriched)
                key = (enriched.get('company_name', '').lower().strip(),
                       enriched.get('hq_city', '').lower().strip())
                already_done.add(key)
                new_count += 1

                if (enriched.get('contact_email') or '').strip():
                    enriched_email += 1
                if (enriched.get('phone') or '').strip():
                    enriched_phone += 1

            except KeyboardInterrupt:
                print("\n\n[INTERRUPTED] Saving progress...")
                _save_csv(enriched_rows, output_csv, OUTPUT_FIELDS)
                print(f"Saved {len(enriched_rows)} rows to {output_csv}")
                sys.exit(0)
            except Exception as e:
                errors += 1
                print(f"  [ERROR] {e}")
                enriched_rows.append(row)

            if new_count % SAVE_EVERY == 0 and new_count > 0:
                _save_csv(enriched_rows, output_csv, OUTPUT_FIELDS)
                print(f"\n  --- Saved {len(enriched_rows)} rows (batch {new_count}) ---\n")

    # Final save (all rows)
    _save_csv(enriched_rows, output_csv, OUTPUT_FIELDS)

    # ── Strict filtering: remove rows with NO email AND NO phone ──
    before_filter = len(enriched_rows)
    filtered_rows = [
        r for r in enriched_rows
        if (r.get('contact_email') or '').strip() or (r.get('phone') or '').strip()
    ]
    dropped = before_filter - len(filtered_rows)
    if dropped > 0:
        print(f"\n[FILTER] Removed {dropped} leads with no email and no phone")
        _save_csv(filtered_rows, output_csv, OUTPUT_FIELDS)
        enriched_rows = filtered_rows

    # ── Summary ─────────────────────────────────────────────────────
    final_email = sum(1 for r in enriched_rows if (r.get('contact_email') or '').strip())
    final_phone = sum(1 for r in enriched_rows if (r.get('phone') or '').strip())
    final_website = sum(1 for r in enriched_rows if (r.get('company_website') or '').strip())
    final_both = sum(
        1 for r in enriched_rows
        if (r.get('contact_email') or '').strip() and (r.get('phone') or '').strip()
    )
    final_linkedin = sum(1 for r in enriched_rows if (r.get('linkedin_company_url') or '').strip())

    print(f"\n{'='*60}")
    print(f"ENRICHMENT COMPLETE")
    print(f"{'='*60}")
    print(f"Total leads:       {len(enriched_rows)}")
    print(f"Newly enriched:    {new_count}")
    print(f"Errors:            {errors}")
    print(f"")
    print(f"{'─'*40}")
    print(f"{'Metric':<25} {'Before':>8} {'After':>8}")
    print(f"{'─'*40}")
    print(f"{'Has email':<25} {pre_email:>8} {final_email:>8}")
    print(f"{'Has phone':<25} {pre_phone:>8} {final_phone:>8}")
    print(f"{'Has website':<25} {pre_website:>8} {final_website:>8}")
    print(f"{'Has BOTH email+phone':<25} {'':>8} {final_both:>8}")
    print(f"{'Has LinkedIn':<25} {'':>8} {final_linkedin:>8}")
    print(f"{'─'*40}")
    print(f"\nOutput: {output_csv}")

    # Email tier breakdown
    tier1_count = 0
    tier2_count = 0
    tier3_count = 0
    for r in enriched_rows:
        e = (r.get('contact_email') or '').strip()
        if not e:
            continue
        domain = e.split('@')[1] if '@' in e else ''
        if domain in GENERIC_DOMAINS:
            tier3_count += 1
        elif any(e.startswith(p) for p in TIER1_PREFIXES):
            tier1_count += 1
        else:
            tier2_count += 1

    print(f"\nEmail quality breakdown:")
    print(f"  Tier 1 (info@/hr@/admin@/travel@ etc.): {tier1_count}")
    print(f"  Tier 2 (company domain):                 {tier2_count}")
    print(f"  Tier 3 (personal gmail/yahoo/etc.):      {tier3_count}")
    print(f"{'='*60}")


# ═══════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='BharatFare Lead Enrichment — find emails, phones, websites'
    )
    parser.add_argument('input_csv', nargs='?', default=None,
                        help='Input CSV file (default: latest in output/)')
    parser.add_argument('output_csv', nargs='?', default=None,
                        help='Output CSV file (default: output/enriched_leads_TIMESTAMP.csv)')
    parser.add_argument('--workers', type=int, default=8,
                        help='Number of parallel workers (default: 8)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Process only first N leads (for testing)')
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, 'output')

    input_csv = args.input_csv
    if not input_csv:
        input_csv = _find_latest_csv(output_dir)
        if not input_csv:
            input_csv = os.path.join(output_dir, 'fresh_merged.csv')

    output_csv = args.output_csv
    if not output_csv:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        output_csv = os.path.join(output_dir, f'enriched_leads_{timestamp}.csv')

    if not os.path.exists(input_csv):
        print(f"ERROR: Input file not found: {input_csv}")
        print(f"Usage: python {sys.argv[0]} [input.csv] [output.csv]")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)
    enrich_leads(input_csv, output_csv, max_workers=args.workers, limit=args.limit)
