import scrapy


class LeadItem(scrapy.Item):
    # Company identification
    company_name = scrapy.Field()
    company_website = scrapy.Field()
    profile_id = scrapy.Field()
    source = scrapy.Field()
    source_url = scrapy.Field()
    linkedin_company_url = scrapy.Field()

    # Industry & sector
    industry = scrapy.Field()
    sector = scrapy.Field()           # e.g. "IT", "Consulting", "Pharma", "Oil & Gas"
    business_type = scrapy.Field()

    # Company size & revenue
    company_size = scrapy.Field()     # employee count
    company_revenue = scrapy.Field()  # revenue range if available
    annual_turnover = scrapy.Field()
    year_established = scrapy.Field()

    # Headquarters & locations
    hq_city = scrapy.Field()
    hq_country = scrapy.Field()
    city = scrapy.Field()             # kept for backward compat with existing spiders
    state = scrapy.Field()
    address = scrapy.Field()
    pincode = scrapy.Field()
    office_locations = scrapy.Field()  # comma-separated list of office cities

    # Contact information
    contact_email = scrapy.Field()     # official contact (info@, travel@, hr@, procurement@)
    email = scrapy.Field()             # kept for backward compat
    phone = scrapy.Field()
    contact_person = scrapy.Field()
    designation = scrapy.Field()

    # HR / Procurement / Travel Manager
    hr_manager_name = scrapy.Field()
    hr_manager_linkedin = scrapy.Field()
    procurement_manager_name = scrapy.Field()
    procurement_manager_linkedin = scrapy.Field()
    corporate_travel_manager = scrapy.Field()
    contact_linkedin = scrapy.Field()  # unified LinkedIn profile of primary contact

    # Travel signals
    travel_score = scrapy.Field()              # computed score 0-100
    estimated_travel_frequency = scrapy.Field() # high/medium/low
    has_international_hiring = scrapy.Field()   # True/False
    target_office_presence = scrapy.Field()     # London/Dubai/Delhi/Bangalore presence

    # Business metadata
    gst_number = scrapy.Field()
    supplier_rating = scrapy.Field()
    member_since = scrapy.Field()
    scraped_date = scrapy.Field()
    search_keyword = scrapy.Field()

    # Legacy compat (alias)
    website = scrapy.Field()
