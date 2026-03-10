"""Shared constants and helpers for all corporate travel lead spiders."""


# ── Corporate Travel Keywords (for directory spiders) ─────────────────

CORPORATE_TRAVEL_KEYWORDS = [
    # IT & Consulting (highest travel frequency)
    'it services', 'it consulting', 'software company',
    'management consulting', 'business consulting',
    'IT outsourcing', 'cloud computing company',
    # Staffing & Recruitment (international hiring signals)
    'recruitment agency', 'staffing company', 'manpower consultancy',
    'executive search firm', 'HR consulting',
    # Shipping & Logistics
    'shipping company', 'logistics company', 'freight forwarding',
    'courier company', 'supply chain company',
    # Oil & Gas / Energy
    'oil and gas company', 'energy company', 'petroleum company',
    'renewable energy company', 'solar energy company',
    # Pharma & Healthcare
    'pharmaceutical company', 'biotech company', 'medical devices company',
    'hospital chain', 'healthcare services',
    # Finance & Banking
    'financial services', 'fintech company', 'investment company',
    'insurance company', 'wealth management',
    # Manufacturing & Engineering
    'manufacturing company', 'engineering company',
    'auto parts manufacturer', 'steel company', 'chemical company',
    # Construction & Infrastructure
    'construction company', 'infrastructure company', 'EPC company',
    # Export / Import & Trading
    'export import company', 'trading company',
    'textile exporter', 'gem jewellery exporter',
    # Marketing & Advertising
    'marketing agency', 'advertising agency', 'digital marketing agency',
    'media company', 'PR agency',
    # Event Management
    'event management company', 'MICE company',
    # Real Estate
    'real estate company', 'property developer',
    # Travel & Hospitality
    'travel agency', 'corporate travel', 'tour operator',
    'hotel booking', 'hospitality company',
    # Other high-travel sectors
    'BPO company', 'KPO company', 'ITES company',
    'co-working space', 'relocation services',
    'visa services', 'foreign exchange',
    'airline ticketing', 'car rental company',
    'cab service', 'law firm', 'architecture firm',
    'audit firm', 'education company', 'edtech company',
]

CITIES_INDIA = [
    'delhi', 'mumbai', 'bangalore', 'hyderabad', 'chennai',
    'pune', 'kolkata', 'ahmedabad', 'gurgaon', 'noida',
    'chandigarh', 'jaipur', 'lucknow', 'indore',
    'surat', 'vadodara', 'coimbatore', 'kochi', 'vizag',
    'bhubaneswar', 'nagpur', 'ludhiana', 'faridabad',
    'ghaziabad', 'thane', 'navi mumbai',
]

CITIES_INTERNATIONAL = [
    'london', 'manchester', 'birmingham',
    'dubai', 'abu dhabi', 'riyadh', 'doha',
    'singapore', 'hong kong', 'kuala lumpur', 'bangkok',
    'new york', 'san francisco', 'toronto',
    'sydney', 'melbourne',
    'frankfurt', 'amsterdam', 'paris',
]


# ── Clutch.co Constants ──────────────────────────────────────────────

CLUTCH_CATEGORIES = [
    'it-services',
    'consulting',
    'financial',
    'pharmaceutical',
    'logistics',
    'oil-gas',
    'staffing',
    'marketing',
    'web-development',
]

CLUTCH_LOCATIONS = [
    'india',
    'india/delhi',
    'india/mumbai',
    'india/bangalore',
    'india/hyderabad',
    'india/chennai',
    'india/pune',
    'united-kingdom/london',
    'uae/dubai',
    'singapore',
    'united-states',
]


# ── GoodFirms Constants ─────────────────────────────────────────────

GOODFIRMS_CATEGORIES = [
    'it-services',
    'consulting-companies',
    'financial-services-companies',
    'pharmaceutical-companies',
    'logistics-companies',
    'oil-gas-companies',
    'staffing-companies',
    'digital-marketing-companies',
]

GOODFIRMS_LOCATIONS = [
    'india',
    'india/delhi',
    'india/mumbai',
    'india/bangalore',
    'india/hyderabad',
    'india/pune',
    'uk/london',
    'uae/dubai',
    'singapore',
    'us',
]


# ── Indeed Constants ─────────────────────────────────────────────────

INDEED_JOB_QUERIES = [
    'corporate travel manager',
    'travel coordinator',
    'procurement manager',
    'business travel',
    'frequent travel required',
    'corporate travel analyst',
    'travel desk executive',
]

INDEED_DOMAINS = [
    ('in.indeed.com', [
        'Delhi', 'Mumbai', 'Bangalore', 'Hyderabad',
        'Chennai', 'Pune', 'Gurgaon', 'Noida', 'Kolkata',
    ]),
    ('www.indeed.co.uk', [
        'London', 'Manchester', 'Birmingham',
    ]),
    ('www.indeed.com', [
        'New York', 'San Francisco', 'Chicago',
    ]),
    ('ae.indeed.com', [
        'Dubai', 'Abu Dhabi',
    ]),
    ('www.indeed.com.sg', [
        'Singapore',
    ]),
]


# ── Google Maps Constants ────────────────────────────────────────────

GOOGLE_MAPS_QUERIES = [
    # India
    'IT companies in Delhi',
    'IT consulting firms in Mumbai',
    'consulting firms in Bangalore',
    'software companies in Hyderabad',
    'IT companies in Pune',
    'logistics companies in Delhi',
    'pharmaceutical companies in Mumbai',
    'financial services companies in Delhi',
    'oil and gas companies in Mumbai',
    'staffing agencies in Bangalore',
    'BPO companies in Gurgaon',
    'manufacturing companies in Ahmedabad',
    'export companies in Surat',
    'engineering firms in Chennai',
    'travel agencies in Delhi',
    'corporate travel agencies in Mumbai',
    # International
    'IT companies in London',
    'consulting firms in London',
    'IT companies in Dubai',
    'staffing agencies in Dubai',
    'consulting firms in Singapore',
    'IT companies in Singapore',
    'technology companies in New York',
    'consulting firms in Sydney',
    'logistics companies in Dubai',
    'corporate travel agencies in London',
    'travel management companies in Dubai',
]


# ── Sector Mapping Helpers ───────────────────────────────────────────

def keyword_to_sector(keyword):
    """Map search keyword to a high-level sector name."""
    kw = keyword.lower()
    if any(t in kw for t in ('it ', 'software', 'fintech', 'cloud', 'bpo', 'kpo', 'ites', 'web dev', 'outsourc', 'edtech')):
        return 'IT'
    if 'consult' in kw:
        return 'Consulting'
    if any(t in kw for t in ('recruit', 'staffing', 'manpower', 'executive search', 'hr consult')):
        return 'Staffing & Recruitment'
    if any(t in kw for t in ('shipping', 'logistics', 'freight', 'courier', 'supply chain')):
        return 'Shipping & Logistics'
    if any(t in kw for t in ('oil', 'gas', 'energy', 'petroleum', 'solar', 'renewable')):
        return 'Oil & Gas'
    if any(t in kw for t in ('pharma', 'biotech', 'medical', 'hospital', 'healthcare')):
        return 'Pharma & Healthcare'
    if any(t in kw for t in ('financial', 'investment', 'banking', 'insurance', 'wealth')):
        return 'Finance'
    if any(t in kw for t in ('manufactur', 'engineer', 'auto parts', 'steel', 'chemical')):
        return 'Manufacturing'
    if any(t in kw for t in ('construct', 'infrastructure', 'epc')):
        return 'Construction'
    if any(t in kw for t in ('export', 'import', 'trading', 'textile', 'gem', 'jewel')):
        return 'Import / Export'
    if any(t in kw for t in ('marketing', 'advertising', 'digital', 'media', 'pr agency')):
        return 'Marketing & Advertising'
    if any(t in kw for t in ('event', 'mice')):
        return 'Events'
    if any(t in kw for t in ('real estate', 'property')):
        return 'Real Estate'
    if any(t in kw for t in ('travel', 'tour', 'hotel', 'hospitality', 'airline', 'cab', 'car rental')):
        return 'Travel & Hospitality'
    if any(t in kw for t in ('law firm', 'legal', 'audit', 'architect')):
        return 'Professional Services'
    if any(t in kw for t in ('co-working', 'relocation', 'visa', 'foreign exchange')):
        return 'Business Services'
    if 'education' in kw:
        return 'Education'
    return 'Other'


def clutch_category_to_sector(category_slug):
    """Map Clutch.co category slug to sector name."""
    mapping = {
        'it-services': 'IT',
        'consulting': 'Consulting',
        'financial': 'Finance',
        'pharmaceutical': 'Pharma & Healthcare',
        'logistics': 'Shipping & Logistics',
        'oil-gas': 'Oil & Gas',
        'staffing': 'Staffing & Recruitment',
        'marketing': 'Marketing & Advertising',
        'web-development': 'IT',
    }
    return mapping.get(category_slug, 'Other')


def goodfirms_category_to_sector(category_slug):
    """Map GoodFirms category slug to sector name."""
    mapping = {
        'it-services': 'IT',
        'consulting-companies': 'Consulting',
        'financial-services-companies': 'Finance',
        'pharmaceutical-companies': 'Pharma & Healthcare',
        'logistics-companies': 'Shipping & Logistics',
        'oil-gas-companies': 'Oil & Gas',
        'staffing-companies': 'Staffing & Recruitment',
        'digital-marketing-companies': 'Marketing & Advertising',
    }
    return mapping.get(category_slug, 'Other')


def keyword_to_hyphenated(keyword):
    """Convert keyword to hyphenated URL slug for sites like IndianYellowPages."""
    return keyword.lower().strip().replace(' ', '-')
