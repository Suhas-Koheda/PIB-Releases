
import os
import re
import json
import time
import requests
from bs4 import BeautifulSoup
from parse_release import extract_release_data

import concurrent.futures
import threading

# Use a session for connection pooling
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount('https://', adapter)
session.mount('http://', adapter)
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
})

print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def extract_prids(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    prids = []
    
    # Target the main lists (usually one per ministry)
    release_lists = soup.find_all('ul', class_='release_list')
    
    if release_lists:
        for r_list in release_lists:
            # We want each release once. Each release is in an <li>
            items = r_list.find_all('li', recursive=False)
            for li in items:
                # 1. Search for an explicit "English" link within this item
                eng_link = None
                for a in li.find_all('a'):
                    if a.get_text(strip=True).lower() == 'english':
                        eng_link = a
                        break
                
                href = None
                if eng_link:
                    href = eng_link.get('href')
                else:
                    # 2. Fallback to the main title link (direct child)
                    title_link = li.find('a', href=True, recursive=False)
                    if title_link:
                        href = title_link['href']
                
                if href:
                    match = re.search(r'PRID=(\d+)', href)
                    if match:
                        prid = match.group(1)
                        if prid not in prids:
                            prids.append(prid)
    else:
        # Fallback to greedy search in content-area if structure differs
        content_div = soup.find('div', class_='content-area') or soup
        for a in content_div.find_all('a', href=True):
            # Only consider links that look like releases
            href = a.get('href', '')
            if 'PressReleasePage.aspx' in href or 'PressReleseDetail.aspx' in href:
                match = re.search(r'PRID=(\d+)', href)
                if match:
                    prid = match.group(1)
                    if prid not in prids:
                        prids.append(prid)
                
    return prids

def download_page(prid):
    url = f"https://pib.gov.in/PressReleasePage.aspx?PRID={prid}"
    try:
        response = session.get(url, timeout=30, verify=False)
        response.raise_for_status()
        return response.text
    except Exception as e:
        safe_print(f"Error downloading {url}: {e}")
        return None

def fetch_discovery_html(day, month, year):
    # reg=3 is PIB Delhi (the standard for English releases)
    url = f"https://www.pib.gov.in/AllRelease.aspx?d={day}&m={month}&y={year}&lang=1&reg=3"
    safe_print(f"Fetching discovery page: {url}")
    try:
        # Use a fresh session for discovery to match proven one-liner logic
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        response = s.get(url, timeout=30, verify=False)
        response.raise_for_status()
        
        # Verify discovery results
        soup = BeautifulSoup(response.text, 'html.parser')
        r_lists = soup.find_all('ul', class_='release_list')
        li_count = sum(len(l.find_all('li', recursive=False)) for l in r_lists)
        safe_print(f"Discovery: Found {len(r_lists)} ministry lists containing {li_count} items.")
            
        return response.text
    except Exception as e:
        safe_print(f"Error fetching discovery page: {e}")
        return None

def process_releases(output_file="extracted_data.json", limit=0, date_args=None):
    discovery_html = None
    
    if date_args:
        discovery_html = fetch_discovery_html(*date_args)
    elif os.path.exists("all_releases_en.html"):
        with open("all_releases_en.html", "r", encoding="utf-8") as f:
            discovery_html = f.read()
    
    if not discovery_html:
        print("Error: No discovery content available. Please provide a date.")
        return

    prids = extract_prids(discovery_html)
    print(f"Found {len(prids)} release IDs to process.")
    
    if limit > 0:
        prids = prids[:limit]
        print(f"Limiting to first {limit} PRIDs")
    
    results = []
    processed_prids = set()
    processed_lock = threading.Lock()
    
    def process_single_prid(prid):
        with processed_lock:
            if prid in processed_prids:
                return None
        
        html = download_page(prid)
        if not html:
            return None
            
        data = extract_release_data(html)
        
        # Robust English detection (allows curly quotes, dashes, etc.)
        def is_likely_english(text):
            if not text: return False
            # Check ratio of ASCII + common punctuation/symbols
            detectable = sum(1 for c in text if ord(c) < 128 or c in "‘’“”–—")
            return (detectable / len(text)) > 0.90 if len(text) > 0 else False

        is_english = is_likely_english(data.get('title', ''))
        
        # If not English, attempt to switch to the English version discovered on-page
        final_data = data
        if not is_english:
            english_url = data.get('languages', {}).get('English')
            if english_url:
                eng_match = re.search(r'PRID=(\d+)', english_url)
                if eng_match:
                    eng_prid = eng_match.group(1)
                    if eng_prid != prid:
                        with processed_lock:
                            if eng_prid not in processed_prids:
                                processed_prids.add(eng_prid)
                                # safe_print(f"Switching {prid} -> English {eng_prid}")
                                eng_html = download_page(eng_prid)
                                if eng_html:
                                    final_data = extract_release_data(eng_html)
                                    final_data['original_prid'] = prid
        
        with processed_lock:
             processed_prids.add(prid)
             
        safe_print(f"Processed {prid} -> {final_data.get('title')[:40]}...")
        return final_data

    # Use ThreadPoolExecutor for concurrent downloads
    max_workers = 10
    print(f"Starting processing with {max_workers} workers...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_prid = {executor.submit(process_single_prid, prid): prid for prid in prids}
        for future in concurrent.futures.as_completed(future_to_prid):
            try:
                data = future.result()
                if data:
                    results.append(data)
            except Exception as exc:
                safe_print(f'Exception: {exc}')
        
    print(f"\nExtracted {len(results)} records.")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Saved to {output_file}")

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    import sys
    from datetime import datetime
    
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    
    date_args = None
    if len(sys.argv) >= 5:
        date_args = (sys.argv[2], sys.argv[3], sys.argv[4])
    elif not os.path.exists("all_releases_en.html"):
        now = datetime.now()
        date_args = (str(now.day), str(now.month), str(now.year))
        print(f"Defaulting to today's date: {date_args[0]}-{date_args[1]}-{date_args[2]}")
        
    process_releases(limit=limit, date_args=date_args)
