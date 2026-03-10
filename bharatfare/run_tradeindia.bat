@echo off
echo ============================================
echo BharatFare Lead Scraper - TradeIndia
echo ============================================

set VENV=C:\Users\mrkar\Desktop\bharatfare ai\scraper\.venv\Scripts
set PROJECT=C:\Users\mrkar\Desktop\bharatfare ai\scraper\bharatfare

cd /d "%PROJECT%"

:: Create output directory if it doesn't exist
if not exist output mkdir output

:: Generate timestamp for filenames
for /f "tokens=1-3 delims=/ " %%a in ('date /t') do set DATE=%%c-%%a-%%b
for /f "tokens=1-2 delims=: " %%a in ('time /t') do set TIME=%%a%%b
set TIMESTAMP=%DATE%_%TIME%

echo [%date% %time%] Starting TradeIndia spider...
"%VENV%\scrapy.exe" crawl tradeindia -o "output\tradeindia_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\tradeindia_%TIMESTAMP%.log"

echo [%date% %time%] TradeIndia spider finished.
echo Output: output\tradeindia_%TIMESTAMP%.csv
echo Log:    output\tradeindia_%TIMESTAMP%.log
pause
