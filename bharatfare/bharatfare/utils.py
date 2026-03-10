"""Email and phone extraction utilities for scraping company contact info."""

import re

# ── Email Extraction ────────────────────────────────────────────────

EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
)

# Target email prefixes for corporate travel outreach (ordered by priority)
TARGET_PREFIXES = (
    'travel@', 'procurement@', 'hr@', 'admin@', 'info@',
    'contact@', 'sales@', 'office@', 'corporate@', 'careers@',
    'business@', 'enquiry@', 'inquiry@', 'support@', 'mail@',
)

# Common contact page paths to try on company websites
CONTACT_PATHS = [
    '/contact',
    '/contact-us',
    '/contactus',
    '/about/contact',
    '/about-us',
    '/about',
    '/reach-us',
    '/get-in-touch',
]

# False-positive patterns to exclude from emails
_JUNK_PATTERNS = re.compile(
    r'(example\.com|sentry\.io|cloudflare|gravatar|schema\.org|'
    r'googleapis|jquery|bootstrap|facebook|twitter|instagram|'
    r'wixpress|wordpress|w3\.org|github\.com|\.png|\.jpg|\.gif|'
    r'\.svg|\.css|\.js|noreply|no-reply|unsubscribe|'
    r'webpack|localhost|placeholder|test@|dummy@)',
    re.IGNORECASE,
)


def extract_emails(text):
    """Extract all valid-looking email addresses from text/HTML."""
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
        if _JUNK_PATTERNS.search(email):
            continue
        if len(email) > 60:
            continue
        if len(email.split('.')[-1]) < 2:
            continue
        result.append(email)
    return result


def filter_target_emails(emails):
    """Separate target-prefix emails from others."""
    targets = []
    others = []
    for email in emails:
        if any(email.startswith(prefix) for prefix in TARGET_PREFIXES):
            targets.append(email)
        else:
            others.append(email)
    return targets, others


def pick_best_email(emails):
    """Select the best email for corporate travel outreach by prefix priority."""
    if not emails:
        return ''
    for prefix in TARGET_PREFIXES:
        for email in emails:
            if email.startswith(prefix):
                return email
    return emails[0] if emails else ''


def extract_emails_from_response(response):
    """Extract emails from a Scrapy response body.

    Returns (best_email, all_emails) tuple.
    """
    all_emails = extract_emails(response.text)
    targets, others = filter_target_emails(all_emails)
    combined = targets + others
    best = pick_best_email(combined)
    return best, combined


# ── Phone Extraction ────────────────────────────────────────────────

# Indian phone patterns: +91, 0-prefixed, plain 10-digit
_PHONE_INDIA = re.compile(
    r'(?:\+91[\s\-]?)?(?:0?\d{2,5}[\s\-]?)?\d{6,10}'
)

# International phone patterns (broad)
_PHONE_INTL = re.compile(
    r'\+?\d{1,4}[\s\-\.]?\(?\d{1,5}\)?[\s\-\.]?\d{2,5}[\s\-\.]?\d{2,5}(?:[\s\-\.]?\d{1,5})?'
)

# More targeted: find tel: and href="tel:" links, and common phone label patterns
_TEL_HREF = re.compile(r'href=["\']tel:([^"\']+)["\']', re.IGNORECASE)
_PHONE_LABEL = re.compile(
    r'(?:phone|tel|mobile|call|fax|whatsapp|contact)\s*[:.\-)\s]*\s*'
    r'(\+?[\d][\d\s\-\.\(\)]{7,18}\d)',
    re.IGNORECASE,
)


def extract_phones(text):
    """Extract phone numbers from text/HTML.

    Returns a deduplicated list of cleaned phone numbers.
    """
    if not text:
        return []

    phones = set()

    # 1. tel: href links (most reliable)
    for match in _TEL_HREF.finditer(text):
        raw = match.group(1).strip()
        cleaned = _clean_phone(raw)
        if cleaned:
            phones.add(cleaned)

    # 2. Labeled phone numbers (e.g. "Phone: +91 98765 43210")
    for match in _PHONE_LABEL.finditer(text):
        raw = match.group(1).strip()
        cleaned = _clean_phone(raw)
        if cleaned:
            phones.add(cleaned)

    return list(phones)


def _clean_phone(raw):
    """Clean and validate a phone number string.

    Returns cleaned digits or empty string if invalid.
    """
    # Strip common separators
    digits = re.sub(r'[\s\-\.\(\)\+]', '', raw)

    # Remove leading country codes for validation
    check = digits
    if check.startswith('91') and len(check) >= 12:
        check = check[2:]
    elif check.startswith('0'):
        check = check[1:]
    elif check.startswith('44') and len(check) >= 12:
        check = check[2:]
    elif check.startswith('971') and len(check) >= 12:
        check = check[3:]

    # Valid if remaining is 7-12 digits (most phone numbers)
    if 7 <= len(check) <= 12 and check.isdigit():
        # Return original with + prefix if it had country code
        if raw.lstrip().startswith('+'):
            return '+' + digits
        return digits

    return ''


def extract_contact_from_response(response):
    """Extract both emails and phones from a Scrapy response.

    Returns dict with keys: best_email, all_emails, best_phone, all_phones
    """
    best_email, all_emails = extract_emails_from_response(response)
    all_phones = extract_phones(response.text)

    # Also try CSS selectors for phone/email on the page
    for selector in ('a[href^="tel:"]::attr(href)',):
        for href in response.css(selector).getall():
            raw = href.replace('tel:', '').strip()
            cleaned = _clean_phone(raw)
            if cleaned and cleaned not in all_phones:
                all_phones.append(cleaned)

    for selector in ('a[href^="mailto:"]::attr(href)',):
        for href in response.css(selector).getall():
            email = href.replace('mailto:', '').strip().split('?')[0].lower()
            if email and '@' in email and email not in all_emails:
                if not _JUNK_PATTERNS.search(email):
                    all_emails.append(email)
                    # Re-pick best email with new candidates
                    best_email = pick_best_email(
                        [e for e in all_emails if any(
                            e.startswith(p) for p in TARGET_PREFIXES
                        )] or all_emails
                    )

    best_phone = all_phones[0] if all_phones else ''

    return {
        'best_email': best_email,
        'all_emails': all_emails,
        'best_phone': best_phone,
        'all_phones': all_phones,
    }
