@echo off
echo ============================================
echo BharatFare - Corporate Travel Lead Scraper
echo ============================================
echo.
echo Running 8 spiders + email enrichment across 4 phases:
echo   Phase 1: Indian Directories (indiamart, tradeindia, exportersindia, iyp)
echo   Phase 2: International Directories (clutch, goodfirms)
echo   Phase 3: Signal Enrichment (indeed, googlemaps)
echo   Phase 4: Email Enrichment (website_emails)
echo.
echo Output: 16-column outreach-ready CSV with contact emails
echo ============================================

set VENV=C:\Users\mrkar\Desktop\bharatfare ai\scraper\.venv\Scripts
set PROJECT=C:\Users\mrkar\Desktop\bharatfare ai\scraper\bharatfare

cd /d "%PROJECT%"

if not exist output mkdir output

for /f "tokens=1-3 delims=/ " %%a in ('date /t') do set DATE=%%c-%%a-%%b
for /f "tokens=1-2 delims=: " %%a in ('time /t') do set TIME=%%a%%b
set TIMESTAMP=%DATE%_%TIME%

echo.
echo === Phase 1: Indian Business Directories ===
echo.

echo [%date% %time%] Starting IndiaMART spider...
"%VENV%\scrapy.exe" crawl indiamart -o "output\corporate_travel_indiamart_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\corporate_travel_indiamart_%TIMESTAMP%.log"
echo [%date% %time%] IndiaMART done.

echo.
echo [%date% %time%] Starting TradeIndia spider...
"%VENV%\scrapy.exe" crawl tradeindia -o "output\corporate_travel_tradeindia_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\corporate_travel_tradeindia_%TIMESTAMP%.log"
echo [%date% %time%] TradeIndia done.

echo.
echo [%date% %time%] Starting ExportersIndia spider...
"%VENV%\scrapy.exe" crawl exportersindia -o "output\corporate_travel_exportersindia_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\corporate_travel_exportersindia_%TIMESTAMP%.log"
echo [%date% %time%] ExportersIndia done.

echo.
echo [%date% %time%] Starting IndianYellowPages spider...
"%VENV%\scrapy.exe" crawl indianyellowpages -o "output\corporate_travel_iyp_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\corporate_travel_iyp_%TIMESTAMP%.log"
echo [%date% %time%] IndianYellowPages done.

echo.
echo === Phase 2: International Directories ===
echo.

echo [%date% %time%] Starting Clutch.co spider...
"%VENV%\scrapy.exe" crawl clutch -o "output\corporate_travel_clutch_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\corporate_travel_clutch_%TIMESTAMP%.log"
echo [%date% %time%] Clutch.co done.

echo.
echo [%date% %time%] Starting GoodFirms spider...
"%VENV%\scrapy.exe" crawl goodfirms -o "output\corporate_travel_goodfirms_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\corporate_travel_goodfirms_%TIMESTAMP%.log"
echo [%date% %time%] GoodFirms done.

echo.
echo === Phase 3: Signal Enrichment ===
echo.

echo [%date% %time%] Starting Indeed job signal spider...
"%VENV%\scrapy.exe" crawl indeed -o "output\corporate_travel_indeed_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\corporate_travel_indeed_%TIMESTAMP%.log"
echo [%date% %time%] Indeed done.

echo.
echo [%date% %time%] Starting Google Maps spider...
"%VENV%\scrapy.exe" crawl googlemaps -o "output\corporate_travel_googlemaps_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\corporate_travel_googlemaps_%TIMESTAMP%.log"
echo [%date% %time%] Google Maps done.

echo.
echo === Phase 3.5: Merge all CSVs ===
echo.

echo [%date% %time%] Merging all spider outputs...
set MERGED=output\corporate_travel_leads_%TIMESTAMP%.csv
set FIRST=1
for %%f in (output\corporate_travel_*_%TIMESTAMP%.csv) do (
    if !FIRST!==1 (
        copy "%%f" "%MERGED%" >nul
        set FIRST=0
    ) else (
        for /f "skip=1 delims=" %%l in ('type "%%f"') do echo %%l>>"%MERGED%"
    )
)
echo [%date% %time%] Merged CSV: %MERGED%

echo.
echo === Phase 4: Email Enrichment ===
echo.

echo [%date% %time%] Starting Website Email spider...
echo   (Visiting company websites to extract contact emails)
"%VENV%\scrapy.exe" crawl website_emails -a input_csv="%MERGED%" -o "output\corporate_travel_emails_%TIMESTAMP%.csv:csv" -s LOG_FILE="output\corporate_travel_emails_%TIMESTAMP%.log"
echo [%date% %time%] Email enrichment done.

echo.
echo ============================================
echo All phases complete!
echo.
echo CSV files in: output\
echo   - Per-spider CSVs: corporate_travel_{spider}_%TIMESTAMP%.csv
echo   - Merged leads:    %MERGED%
echo   - Email-enriched:  output\corporate_travel_emails_%TIMESTAMP%.csv
echo.
echo CSV Columns (16):
echo   Company Name, Website, Industry, Employee Size, HQ,
echo   Office Locations, Official Email, Contact Person,
echo   Role, LinkedIn Profile, Phone, LinkedIn Company URL,
echo   Revenue Range, Hiring Internationally, Travel Freq,
echo   Source URL
echo.
echo Target emails: info@, hr@, admin@, travel@, procurement@
echo Sort by estimated_travel_frequency to prioritize leads.
echo ============================================
pause
