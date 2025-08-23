"""

Leo Howard, Copyright 2025.

Set-up virtual venv:
python3 -m venv ai_financial_news_env

Activate virtual env:
ai_financial_news_env/Scripts/activate
where.exe python.exe
source ai_financial_news_env/bin/activate
which python

"""

import requests
from bs4 import BeautifulSoup

url = "https://news.ycombinator.com/"
html = requests.get(url).text
soup = BeautifulSoup(html, "html.parser")

# Example: grab all story titles
for item in soup.select(".storylink"):
    print(item.get_text())
