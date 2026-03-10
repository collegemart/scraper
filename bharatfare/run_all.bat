@echo off
echo ============================================
echo BharatFare Lead Scraper - Run All
echo ============================================

set VENV=C:\Users\mrkar\Desktop\bharatfare ai\scraper\.venv\Scripts
set PROJECT=C:\Users\mrkar\Desktop\bharatfare ai\scraper\bharatfare

cd /d "%PROJECT%"

if not exist output mkdir output

for /f "tokens=1-3 delims=/ " %%a in ('date /t') do set DATE=%%c-%%a-%%b
for /f "tokens=1-2 delims=: " %%a in ('time /t') do set TIME=%%a%%b
set TIMESTAMP=%DATE%_%TIME%

echo [%date% %time%] Starting IndiaMART spider...
"%VENV%\scrapy.exe" crawl indiamart -o "output\indiamart_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\indiamart_%TIMESTAMP%.log"
echo [%date% %time%] IndiaMART done.

echo.
echo [%date% %time%] Starting TradeIndia spider...
"%VENV%\scrapy.exe" crawl tradeindia -o "output\tradeindia_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\tradeindia_%TIMESTAMP%.log"
echo [%date% %time%] TradeIndia done.

echo.
echo [%date% %time%] Starting ExportersIndia spider...
"%VENV%\scrapy.exe" crawl exportersindia -o "output\exportersindia_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\exportersindia_%TIMESTAMP%.log"
echo [%date% %time%] ExportersIndia done.

echo.
echo [%date% %time%] Starting IndianYellowPages spider...
"%VENV%\scrapy.exe" crawl indianyellowpages -o "output\indianyellowpages_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\indianyellowpages_%TIMESTAMP%.log"
echo [%date% %time%] IndianYellowPages done.

echo.
echo ============================================
echo All 4 spiders complete!
echo Check output folder for CSV files.
echo ============================================
pause
