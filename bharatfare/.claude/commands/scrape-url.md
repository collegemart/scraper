# Scrape URL

Scrape data from the provided URL using the universal spider.

## Instructions

The user wants to scrape: $ARGUMENTS

1. First, use the `fetch` MCP tool to preview the page and understand its structure
2. Analyze what data is available (products, contacts, listings, prices, etc.)
3. Run the universal spider with appropriate parameters:
   ```
   scrapy crawl universal -a url="<URL>" -a max_pages=50 -a depth=2 -a follow=true -a scroll=true -o output/<domain>_<timestamp>.csv -s LOG_FILE=output/<domain>_<timestamp>.log
   ```
4. Check the output CSV and report what was extracted
5. If results are poor, suggest adjustments to spider parameters or a custom extraction approach
