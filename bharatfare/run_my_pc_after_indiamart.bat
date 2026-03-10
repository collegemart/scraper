@echo off
echo ============================================
echo BharatFare - Your PC Runner (After IndiaMart)
echo Running: TradeIndia, ExportersIndia, IndianYellowPages, JustDial
echo ============================================

call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERROR: Virtual environment not found!
    pause
    exit /b 1
)

if not exist output mkdir output

echo.
echo [1/4] Running TradeIndia spider...
scrapy crawl tradeindia -o output\tradeindia.csv:csv -s LOG_FILE=output\tradeindia.log
echo TradeIndia done!

echo.
echo [2/4] Running ExportersIndia spider...
scrapy crawl exportersindia -o output\exportersindia.csv:csv -s LOG_FILE=output\exportersindia.log
echo ExportersIndia done!

echo.
echo [3/4] Running IndianYellowPages spider...
scrapy crawl indianyellowpages -o output\indianyellowpages.csv:csv -s LOG_FILE=output\indianyellowpages.log
echo IndianYellowPages done!

echo.
echo [4/4] Running JustDial spider...
scrapy crawl justdial -o output\justdial.csv:csv -s LOG_FILE=output\justdial.log
echo JustDial done!

echo.
echo ============================================
echo YOUR PC SPIDERS DONE!
echo Now wait for IndiaMart output (already running)
echo then merge everything using merge_all.py
echo ============================================
pause
