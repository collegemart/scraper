@echo off
echo ============================================
echo BharatFare - Friend's Laptop Spider Runner
echo Running: Fundoodata, Clutch, GoodFirms, GoogleMaps, Indeed
echo ============================================

call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERROR: Virtual environment not found!
    echo Please run setup first.
    pause
    exit /b 1
)

if not exist output mkdir output

echo.
echo [1/5] Running Fundoodata spider...
scrapy crawl fundoodata -o output\fundoodata.csv:csv -s LOG_FILE=output\fundoodata.log
echo Fundoodata done!

echo.
echo [2/5] Running Clutch spider...
scrapy crawl clutch -o output\clutch.csv:csv -s LOG_FILE=output\clutch.log
echo Clutch done!

echo.
echo [3/5] Running GoodFirms spider...
scrapy crawl goodfirms -o output\goodfirms.csv:csv -s LOG_FILE=output\goodfirms.log
echo GoodFirms done!

echo.
echo [4/5] Running Indeed spider...
scrapy crawl indeed -o output\indeed.csv:csv -s LOG_FILE=output\indeed.log
echo Indeed done!

echo.
echo [5/5] Running Google Maps spider...
scrapy crawl googlemaps -o output\googlemaps.csv:csv -s LOG_FILE=output\googlemaps.log
echo GoogleMaps done!

echo.
echo ============================================
echo ALL DONE! Check your output folder.
echo Now zip the output folder and send it to your partner.
echo ============================================
pause
