# Analyze Website for Scraping

Analyze a website's structure to plan the best scraping strategy.

## Instructions

Analyze: $ARGUMENTS

1. Use the `fetch` MCP tool to get the page content
2. Use the `playwright` MCP tool to render the page if it's JavaScript-heavy
3. Report on:
   - **Data format**: Is data in JSON (__NEXT_DATA__, API responses), HTML tables, structured HTML cards, or unstructured text?
   - **Anti-scraping measures**: Rate limiting, CAPTCHAs, Cloudflare protection, login walls, IP bans
   - **Pagination**: How does pagination work (URL params, infinite scroll, load-more buttons)?
   - **Dynamic content**: Does the page require JavaScript rendering?
   - **API endpoints**: Any XHR/fetch API calls that return structured data (check network activity)?
   - **Data fields available**: What structured fields can be extracted?
4. Recommend whether to:
   - Use the existing universal spider
   - Create a new custom spider
   - Use direct API calls instead of HTML scraping
5. Provide a sample extraction approach with CSS selectors or JSON paths
