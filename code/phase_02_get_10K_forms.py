"""
Leo Howard, Copyright 2025.

Set-up virtual venv:
python3 -m venv finance_llm_python_venv

Activate virtual env in Windows:
finance_llm_python_venv/Scripts/activate
python -c "import sys; print(sys.executable)"

pip install requests beautifulsoup4 pandas lxml

python code/phase_02_get_10K_forms.py
"""

import os
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import random
import re
import json
import shutil
from urllib.parse import urljoin

CSV_FILE = "./data/02_va_sp500_filtered_by_net_income.csv"
OUTPUT_DIR = os.path.join(".", "data", "10k_filings")
MAX_FILINGS_PER_TICKER = 3  # Get last 3 years of 10-K filings
REQUEST_DELAY = (1, 2)  # SEC allows up to 10 requests per second, but we'll be conservative

# SEC EDGAR API configuration - FIXED User-Agent (SEC requires proper identification)
SEC_BASE_URL = "https://www.sec.gov"
EDGAR_API_BASE = "https://data.sec.gov"
HEADERS = {
    'User-Agent': 'MyCompany myname@mycompany.com',  # SEC requires proper identification
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive'
}

os.makedirs(OUTPUT_DIR, exist_ok=True)
# Clear existing data
shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_cik_from_ticker(ticker):
    """
    Get the Central Index Key (CIK) for a given ticker symbol using SEC's company tickers mapping.
    """
    # Handle special ticker mappings (companies with multiple share classes)
    ticker_mappings = {
        'GOOGL': 'GOOG',  # Alphabet files under GOOG
        'BRK.B': 'BRK-B', 'BRK-B': 'BRK.A',  # Berkshire Hathaway
        'BF.B': 'BF-B'     # Brown-Forman
    }
    
    # Try original ticker first, then mapped version
    tickers_to_try = [ticker.upper()]
    if ticker.upper() in ticker_mappings:
        tickers_to_try.append(ticker_mappings[ticker.upper()])
    
    try:
        # Use SEC's company tickers mapping file
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=HEADERS, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # Try each ticker variant
            for try_ticker in tickers_to_try:
                for key, company_info in data.items():
                    if isinstance(company_info, dict) and company_info.get('ticker', '').upper() == try_ticker:
                        cik = company_info.get('cik_str')
                        if cik is not None:
                            return str(cik).zfill(10)
        
        # FALLBACK: Try company name search for known companies
        return search_by_company_name(ticker)
            
    except Exception as e:
        pass  # Silently handle errors
    
    return None

def search_by_company_name(ticker):
    """
    Search by company name for known ticker mappings
    """
    company_names = {
        'GOOGL': 'Alphabet Inc',
        'GOOG': 'Alphabet Inc', 
        'AFL': 'AFLAC',
        'APD': 'Air Products',
        'ALLE': 'Allegion'
    }
    
    if ticker.upper() in company_names:
        try:
            search_url = "https://www.sec.gov/cgi-bin/browse-edgar"
            params = {
                'company': company_names[ticker.upper()],
                'match': 'contains',
                'action': 'getcompany',
                'count': '1'
            }
            
            response = requests.get(search_url, headers=HEADERS, params=params, timeout=15)
            if response.status_code == 200:
                # Try to extract CIK from response
                cik_match = re.search(r'CIK=(\d+)', response.text)
                if cik_match:
                    return str(cik_match.group(1)).zfill(10)
        except:
            pass
    
    return None
    """
    Fallback method to find CIK by searching SEC's website directly
    """
    try:
        search_url = "https://www.sec.gov/edgar/searchedgar/companysearch.html"
        # Use the browse-edgar endpoint for company search
        browse_url = "https://www.sec.gov/cgi-bin/browse-edgar"
        
        params = {
            'action': 'getcompany',
            'CIK': ticker,
            'type': '10-K',
            'dateb': '',
            'count': '1',
            'output': 'xml'
        }
        
        response = requests.get(browse_url, headers=HEADERS, params=params, timeout=15)
        
        if response.status_code == 200:
            # Try to extract CIK from the response
            soup = BeautifulSoup(response.content, 'xml')
            # Look for CIK in the XML response
            company_info = soup.find('company-info')
            if company_info:
                cik_elem = company_info.find('cik')
                if cik_elem:
                    return str(cik_elem.text).zfill(10)
        
    except Exception as e:
        pass
    
    return None

def search_10k_filings(ticker, max_filings=MAX_FILINGS_PER_TICKER):
    """
    Search for 10-K filings using multiple methods
    """
    # Method 1: Try with CIK lookup first
    cik = get_cik_from_ticker(ticker)
    
    if cik:
        # Try the submissions API with CIK
        try:
            submissions_url = f"{EDGAR_API_BASE}/submissions/CIK{cik}.json"
            response = requests.get(submissions_url, headers=HEADERS, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                recent_filings = data.get('filings', {}).get('recent', {})
                
                filings = []
                forms = recent_filings.get('form', [])
                accession_numbers = recent_filings.get('accessionNumber', [])
                filing_dates = recent_filings.get('filingDate', [])
                
                for i, form in enumerate(forms):
                    if form == '10-K' and len(filings) < max_filings:
                        if i < len(accession_numbers) and i < len(filing_dates):
                            filings.append({
                                'cik': cik,
                                'accession_number': accession_numbers[i],
                                'filing_date': filing_dates[i],
                                'report_date': filing_dates[i],
                                'form': form
                            })
                
                if filings:
                    return filings
        except Exception as e:
            pass
    
    # Method 2: Direct ticker search via browse-edgar
    try:
        search_url = "https://www.sec.gov/cgi-bin/browse-edgar"
        params = {
            'action': 'getcompany',
            'ticker': ticker.upper(),
            'type': '10-K',
            'dateb': '',
            'count': str(max_filings),
            'search_text': ''
        }
        
        response = requests.get(search_url, headers=HEADERS, params=params, timeout=15)
        
        if response.status_code == 200:
            # Parse HTML response to extract filing information
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for the filings table
            filings_table = soup.find('table', class_='tableFile2')
            if not filings_table:
                filings_table = soup.find('table')  # Fallback to any table
            
            filings = []
            if filings_table:
                rows = filings_table.find_all('tr')
                
                for row in rows[1:max_filings+1]:  # Skip header row
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        form_type = cells[0].get_text(strip=True)
                        if form_type == '10-K':
                            # Extract filing date
                            filing_date = cells[3].get_text(strip=True) if len(cells) > 3 else 'Unknown'
                            
                            # Find the document link
                            doc_link = cells[1].find('a')
                            if doc_link and 'href' in doc_link.attrs:
                                href = doc_link['href']
                                # Extract accession number from URL
                                accession_match = re.search(r'Accession-Number=([^&]+)', href)
                                if not accession_match:
                                    accession_match = re.search(r'/([0-9-]+)/', href)
                                
                                if accession_match:
                                    accession_number = accession_match.group(1)
                                    
                                    # Extract CIK from URL if we don't have it
                                    if not cik:
                                        cik_match = re.search(r'CIK=([0-9]+)', href)
                                        if cik_match:
                                            cik = str(cik_match.group(1)).zfill(10)
                                    
                                    filings.append({
                                        'cik': cik,
                                        'accession_number': accession_number,
                                        'filing_date': filing_date,
                                        'report_date': filing_date,
                                        'form': '10-K'
                                    })
            
            if filings:
                return filings
                
    except Exception as e:
        pass
    
    return []

def download_10k_document(filing_info, ticker):
    """
    Download the actual 10-K document text from SEC EDGAR - FIXED URL construction
    """
    try:
        accession_number = filing_info['accession_number']
        cik = filing_info.get('cik')
        
        if not cik:
            return f"No CIK available for {ticker}"
        
        # FIXED: Proper SEC EDGAR URL construction
        accession_clean = accession_number.replace('-', '')
        
        # The correct SEC EDGAR URL format
        urls_to_try = [
            # Primary document (most common)
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{accession_number}-10-k.htm",
            # Alternative formats
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{accession_number}.txt",
            # Sometimes the file has a different naming pattern
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/form10k.htm",
            # Try the detail URL if available
        ]
        
        # Add detail URL from filing info if available
        if 'detail_url' in filing_info:
            urls_to_try.insert(0, filing_info['detail_url'])
        
        for url in urls_to_try:
            try:
                response = requests.get(url, headers=HEADERS, timeout=20)
                
                if response.status_code == 200:
                    content_type = response.headers.get('content-type', '').lower()
                    
                    if 'html' in content_type or url.endswith('.htm'):
                        # Parse HTML and extract text
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Remove unwanted elements
                        for element in soup(['script', 'style', 'nav', 'header', 'footer']):
                            element.decompose()
                        
                        # Get text content
                        text = soup.get_text()
                        
                    else:
                        # Plain text content
                        text = response.text
                    
                    # Check if we got substantial content (10-K should be long)
                    if len(text) > 10000:  # 10K+ characters minimum
                        return text
                        
            except Exception as e:
                continue
        
        return f"Could not download 10-K document for {ticker}. All URLs failed."
        
    except Exception as e:
        return f"Error downloading 10-K document: {str(e)}"

def clean_10k_text(text):
    """
    Clean and format 10-K text for better readability.
    """
    # Remove excessive whitespace
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    
    # Remove page markers and other artifacts
    text = re.sub(r'^\s*\d+\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'<[^>]+>', '', text)  # Remove any remaining HTML tags
    
    # Remove SEC header/footer boilerplate
    text = re.sub(r'UNITED STATES\s*SECURITIES AND EXCHANGE COMMISSION.*?(?=FORM 10-K)', '', text, flags=re.DOTALL | re.IGNORECASE)
    
    # Limit length to avoid extremely large files
    if len(text) > 1000000:  # 1MB limit
        text = text[:1000000] + "\n\n[Document truncated due to size...]"
    
    return text.strip()

# Load CSV file
try:
    df = pd.read_csv(CSV_FILE)
    tickers = df["Ticker"].dropna().unique()
    print(f"Found {len(tickers)} tickers to process")
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit(1)

# Process tickers
# tickers_to_use = tickers[:5]  # Test with first 5 tickers
tickers_to_use = tickers  # Use this line for all tickers
print(f"Using {len(tickers_to_use)} tickers: {', '.join(tickers_to_use)}")

successful_tickers = 0
for i, ticker in enumerate(tickers_to_use):
    print(f"\n[{i+1}/{len(tickers_to_use)}] Processing {ticker}...")
    
    ticker_dir = os.path.join(OUTPUT_DIR, ticker)
    os.makedirs(ticker_dir, exist_ok=True)
    
    # Search for 10-K filings
    filings = search_10k_filings(ticker)
    
    if not filings:
        print(f"  No 10-K filings found for {ticker}")
        continue
    
    print(f"  Found {len(filings)} 10-K filings for {ticker}")
    
    success_count = 0
    for j, filing in enumerate(filings):
        try:
            filing_date = filing.get('filing_date', 'Unknown')
            accession_number = filing.get('accession_number', 'Unknown')
            
            print(f"    Processing 10-K filed {filing_date}...")
            
            # Download the 10-K document
            document_text = download_10k_document(filing, ticker)
            
            if not document_text.startswith("Error") and not document_text.startswith("Could not"):
                success_count += 1
                
                # Clean the text
                clean_text = clean_10k_text(document_text)
                
                # Create filename
                safe_date = filing_date.replace('-', '_')
                filename = os.path.join(ticker_dir, f"{ticker}_10K_{safe_date}_{accession_number.replace('-', '_')}.txt")
                
                # Save the filing
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(f"Ticker: {ticker}\n")
                    f.write(f"Form Type: 10-K\n")
                    f.write(f"Accession Number: {accession_number}\n")
                    f.write(f"Filing Date: {filing_date}\n")
                    f.write(f"Downloaded: {datetime.now()}\n")
                    f.write("="*80 + "\n\n")
                    f.write(clean_text)
                
                print(f"      ✓ Successfully saved 10-K filing ({len(clean_text):,} chars)")
            else:
                print(f"      ✗ Failed to download: {document_text}")
            
            # Delay between filings
            time.sleep(random.uniform(*REQUEST_DELAY))
            
        except Exception as e:
            print(f"      ✗ Error processing filing: {e}")
            continue
    
    if success_count > 0:
        successful_tickers += 1
    
    print(f"  Completed {ticker}: {success_count}/{len(filings)} filings successfully saved")
    
    # Longer delay between tickers
    if i < len(tickers_to_use) - 1:
        time.sleep(random.uniform(2, 4))

print(f"\n10-K scraping completed!")
print(f"Successfully processed {successful_tickers}/{len(tickers_to_use)} tickers")
print(f"Check '{OUTPUT_DIR}' directory for results.")
print("\nNote: SEC requires proper User-Agent identification.")
print("If you get blocked, update the User-Agent with your actual contact info.")