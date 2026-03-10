# Debug Spider

Debug a spider that's not working correctly.

## Instructions

Debug: $ARGUMENTS

1. Read the spider source code
2. Check recent log files in `output/` for errors
3. Common issues to check:
   - **403/429 responses**: Anti-bot detection - check download delay, user agent, headers
   - **Empty results**: Selectors may be wrong - use `fetch`/`playwright` to inspect current page structure
   - **Partial data**: Check item pipeline drops and cleaning transformations
   - **Timeouts**: Check DOWNLOAD_TIMEOUT and page complexity
   - **Playwright errors**: Check browser installation, async reactor config
4. Use the `fetch` MCP tool to test-fetch one of the spider's target URLs
5. Compare the fetched page structure against the spider's CSS selectors / XPath / JSON parsing
6. Suggest and implement fixes
7. Test the fix with a limited run (`-s CLOSESPIDER_ITEMCOUNT=5`)
