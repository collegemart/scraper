# Run Spiders

Run one or more scrapy spiders and monitor the results.

## Instructions

Run: $ARGUMENTS

If no specific spider is mentioned, ask the user which spider(s) to run from:
- `indiamart` - IndiaMART B2B directory
- `tradeindia` - TradeIndia B2B directory
- `exportersindia` - ExportersIndia directory
- `indianyellowpages` - IndianYellowPages directory
- `universal` - Universal spider (requires URL argument)
- `all` - Run all 4 directory spiders sequentially

Steps:
1. Set up the output directory and generate a timestamp for filenames
2. Run the spider(s) using the project's virtual environment scrapy:
   ```
   "c:/Users/mrkar/Desktop/bharatfare ai/scraper/.venv/Scripts/scrapy.exe" crawl <spider> -o output/<spider>_<timestamp>.csv -s LOG_FILE=output/<spider>_<timestamp>.log
   ```
3. Monitor the log for errors and progress
4. When complete, report: items scraped, pages crawled, errors encountered, time elapsed
5. Preview the first few rows of the CSV output
