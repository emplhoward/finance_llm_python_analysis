"""
Leo Howard, Copyright 2025.

Set-up virtual venv:
python3 -m venv finance_llm_python_venv

Activate virtual env in Windows:
finance_llm_python_venv/Scripts/activate
python -c "import sys; print(sys.executable)"

pip install feedparser beautifulsoup4 requests

python code/phase_02_news_scraping.py
"""

import os
import time
import pandas as pd
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import random
import re
from urllib.parse import unquote
import shutil

CSV_FILE = "./data/02_va_sp500_filtered_by_net_income.csv"
OUTPUT_DIR = os.path.join(".", "data", "news")
MAX_ARTICLES = 5
REQUEST_DELAY = (2, 4)  # Longer delays to avoid being blocked

os.makedirs(OUTPUT_DIR, exist_ok=True)
shutil.rmtree(OUTPUT_DIR, ignore_errors=True); os.makedirs(OUTPUT_DIR, exist_ok=True)

def decode_google_news_url(url):
    """Attempt to decode Google News URL to get the actual article URL"""
    try:
        # Method 1: Look for URL in the Google News redirect
        if 'news.google.com' in url and '/articles/' in url:
            # Try to make a request to Google News URL and follow redirects
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            try:
                response = requests.get(url, headers=headers, allow_redirects=False, timeout=10)
                if 'location' in response.headers:
                    return response.headers['location']
            except:
                pass
        
        # Method 2: Try to extract URL from the encoded part
        if '/articles/' in url:
            # Sometimes the actual URL is base64 encoded or otherwise hidden
            # This is a simplified approach - Google's encoding is complex
            pass
            
        return url
    except:
        return url

def get_alternative_news_sources(ticker):
    """Get news from alternative sources like Yahoo Finance"""
    articles = []
    
    # Yahoo Finance RSS
    yahoo_rss = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"
    
    try:
        feed = feedparser.parse(yahoo_rss)
        for entry in feed.entries[:MAX_ARTICLES]:
            articles.append({
                'title': entry.title,
                'link': entry.link,
                'published': entry.get('published', 'Unknown'),
                'source': 'Yahoo Finance'
            })
    except Exception as e:
        # print(f"    Error with Yahoo Finance RSS: {e}")
    
    # MarketWatch RSS (they often have good RSS feeds)
    try:
        marketwatch_rss = f"https://feeds.marketwatch.com/marketwatch/topstories/"
        feed = feedparser.parse(marketwatch_rss)
        ticker_articles = [entry for entry in feed.entries if ticker.upper() in entry.title.upper()]
        
        for entry in ticker_articles[:MAX_ARTICLES//2]:
            articles.append({
                'title': entry.title,
                'link': entry.link,
                'published': entry.get('published', 'Unknown'),
                'source': 'MarketWatch'
            })
    except Exception as e:
        # print(f"    Error with MarketWatch RSS: {e}")
    
    return articles

def extract_article_content(url, timeout=15):
    """Extract article content with better handling for different news sites"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
    }
    
    try:
        # print(f"    Attempting to scrape: {url[:100]}...")
        
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, timeout=timeout, allow_redirects=True)
        response.raise_for_status()
        
        # Check if we got redirected to a paywall or login page
        final_url = response.url.lower()
        if any(blocked in final_url for blocked in ['paywall', 'subscribe', 'login', 'register']):
            return "Content blocked by paywall or subscription requirement."
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'advertisement', 'ads']):
            element.decompose()
        
        # Try multiple approaches to find article content
        content_text = ""
        
        # Method 1: Look for JSON-LD structured data
        json_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_scripts:
            try:
                import json
                data = json.loads(script.string)
                if isinstance(data, dict) and 'articleBody' in data:
                    content_text = data['articleBody']
                    break
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and 'articleBody' in item:
                            content_text = item['articleBody']
                            break
            except:
                continue
        
        # Method 2: Try various content selectors
        if not content_text:
            content_selectors = [
                'div[data-module="ArticleBody"]',  # Yahoo Finance
                '.caas-body',  # Yahoo
                'div.article-body',
                'div.story-body',
                'div.entry-content',
                'div.post-content',
                'div.content-body',
                'article',
                '[role="main"] p',
                '.article-content p',
                '.story-content p'
            ]
            
            for selector in content_selectors:
                elements = soup.select(selector)
                if elements:
                    if selector.endswith(' p'):
                        # For paragraph selectors
                        paragraphs = elements
                    else:
                        # For container selectors
                        paragraphs = []
                        for element in elements:
                            paragraphs.extend(element.find_all('p'))
                    
                    texts = []
                    for p in paragraphs:
                        text = p.get_text().strip()
                        if len(text) > 30 and not any(skip in text.lower() for skip in ['subscribe', 'sign up', 'advertisement', 'cookie']):
                            texts.append(text)
                    
                    if len(texts) > 2:  # Need at least 3 substantial paragraphs
                        content_text = '\n\n'.join(texts)
                        break
        
        # Method 3: Fallback to all paragraphs
        if not content_text:
            all_paragraphs = soup.find_all('p')
            substantial_paragraphs = []
            
            for p in all_paragraphs:
                text = p.get_text().strip()
                # Filter out navigation, ads, etc.
                if (len(text) > 50 and 
                    not any(skip in text.lower() for skip in [
                        'cookie', 'subscribe', 'sign up', 'advertisement', 'follow us',
                        'newsletter', 'privacy policy', 'terms of service', 'all rights reserved'
                    ])):
                    substantial_paragraphs.append(text)
            
            if len(substantial_paragraphs) >= 2:
                content_text = '\n\n'.join(substantial_paragraphs[:15])  # Limit to first 15 paragraphs
        
        if content_text and len(content_text) > 200:
            return content_text
        else:
            return f"Minimal content extracted. Site may require JavaScript or have anti-scraping measures. Content length: {len(content_text)}"
            
    except requests.exceptions.RequestException as e:
        return f"Request failed: {str(e)}"
    except Exception as e:
        return f"Extraction error: {str(e)}"

# Load CSV file
try:
    df = pd.read_csv(CSV_FILE)
    tickers = df["Ticker"].dropna().unique()
    print(f"Found {len(tickers)} tickers to process")
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit(1)

# Process only first few tickers for testing
# tickers_to_use = tickers[:5]  # Test with first 5 tickers
tickers_to_use = tickers
print(f"Using {len(tickers_to_use)} tickers: {', '.join(tickers_to_use)}")

for i, ticker in enumerate(tickers_to_use):
    # print(f"\n[{i+1}/{len(tickers_to_use)}] Processing {ticker}...")
    
    ticker_dir = os.path.join(OUTPUT_DIR, ticker)
    os.makedirs(ticker_dir, exist_ok=True)
    
    # Try alternative news sources first (they're more reliable)
    articles = get_alternative_news_sources(ticker)
    
    # If we don't have enough articles, try Google News as backup
    if len(articles) < MAX_ARTICLES:
        try:
            google_rss = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en&gl=US&ceid=US:en"
            feed = feedparser.parse(google_rss)
            
            for entry in feed.entries[:MAX_ARTICLES-len(articles)]:
                articles.append({
                    'title': entry.title,
                    'link': entry.link,
                    'published': entry.get('published', 'Unknown'),
                    'source': 'Google News'
                })
        except Exception as e:
            # print(f"  Error with Google News: {e}")
    
    if not articles:
        # print(f"  No articles found for {ticker}")
        continue
    
    count = 0
    for j, article in enumerate(articles):
        try:
            title = article['title'].replace('\n', ' ').strip()
            link = article['link']
            source = article['source']
            
            # print(f"  [{j+1}/{len(articles)}] Processing from {source}: {title[:60]}...")
            
            # Skip Google News URLs for now (they're too problematic)
            if 'news.google.com' in link and '/articles/' in link:
                # print(f"    Skipping Google News redirect URL")
                continue
            
            # Extract content
            article_text = extract_article_content(link)
            
            if "error" not in article_text.lower() and len(article_text) > 200:
                count += 1
                safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).strip()[:40]
                filename = os.path.join(ticker_dir, f"{ticker}_{count}_{safe_title}.txt")
                
                with open(filename, "w", encoding="utf-8") as f:
                    f.write(f"Ticker: {ticker}\n")
                    f.write(f"Source: {source}\n")
                    f.write(f"Title: {title}\n")
                    f.write(f"Link: {link}\n")
                    f.write(f"Published: {article['published']}\n")
                    f.write(f"Scraped: {datetime.now()}\n")
                    f.write("="*80 + "\n\n")
                    f.write(article_text)
                
                # print(f"    ✓ Successfully saved article {count} ({len(article_text)} chars)")
            else:
                # print(f"    ✗ Content extraction failed or insufficient content")
            
            # Delay between articles
            time.sleep(random.uniform(*REQUEST_DELAY))
            
        except Exception as e:
            # print(f"    ✗ Error processing article: {e}")
            continue
    
    # print(f"  Completed {ticker}: {count} articles successfully saved")
    
    # Longer delay between tickers
    if i < len(tickers_to_use) - 1:
        time.sleep(random.uniform(3, 6))

print(f"\nScraping test completed! Check '{OUTPUT_DIR}' directory for results.")
print("If this works well, you can increase the number of tickers_to_use or remove the limit.")