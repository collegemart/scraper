# Create New Spider

Create a new custom Scrapy spider for a specific website.

## Instructions

Create a spider for: $ARGUMENTS

1. First analyze the target website using `fetch` and/or `playwright` MCP tools
2. Study the existing spider patterns in `bharatfare/spiders/` for consistency
3. Create a new spider file in `bharatfare/spiders/` that:
   - Uses `scrapy.Spider` as base class
   - Follows the project's naming convention (`<site>_spider.py`)
   - Yields `LeadItem` objects if scraping business data, or plain dicts for other data
   - Includes anti-detection settings (impersonate, download delay, concurrent limits)
   - Handles pagination
   - Has proper error handling and logging
4. Add appropriate custom_settings for the spider
5. Test the spider with a single search query first
6. Report the results and ask if adjustments are needed
