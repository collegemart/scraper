@echo off
echo ============================================
echo BharatFare Lead Scraper - IndiaMART
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

echo [%date% %time%] Starting IndiaMART spider...
"%VENV%\scrapy.exe" crawl indiamart -o "output\indiamart_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\indiamart_%TIMESTAMP%.log"

echo [%date% %time%] IndiaMART spider finished.
echo Output: output\indiamart_%TIMESTAMP%.csv
echo Log:    output\indiamart_%TIMESTAMP%.log
pause
