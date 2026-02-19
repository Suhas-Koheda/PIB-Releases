
import os
import re
import json
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from parse_release import extract_release_data

import concurrent.futures
import threading
from datetime import datetime

import random

# Common browser User-Agents to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# Use a session for connection pooling
session = requests.Session()
# Reduced pool size slightly to be less aggressive
adapter = requests.adapters.HTTPAdapter(pool_connections=10, pool_maxsize=10)
session.mount('https://', adapter)
session.mount('http://', adapter)

print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def extract_prids(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    prids = []
    
    release_lists = soup.find_all('ul', class_='release_list')
    
    if release_lists:
        for r_list in release_lists:
            items = r_list.find_all('li', recursive=False)
            for li in items:
                eng_link = None
                for a in li.find_all('a'):
                    if a.get_text(strip=True).lower() == 'english':
                        eng_link = a
                        break
                
                href = None
                if eng_link:
                    href = eng_link.get('href')
                else:
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
        content_div = soup.find('div', class_='content-area') or soup
        for a in content_div.find_all('a', href=True):
            href = a.get('href', '')
            if 'PressReleasePage.aspx' in href or 'PressReleseDetail.aspx' in href:
                match = re.search(r'PRID=(\d+)', href)
                if match:
                    prid = match.group(1)
                    if prid not in prids:
                        prids.append(prid)
                
    return prids

def download_page(prid, retries=5):
    url = f"https://pib.gov.in/PressReleasePage.aspx?PRID={prid}"
    
    for attempt in range(retries):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://pib.gov.in/AllRelease.aspx"
            }
            
            # More aggressive jitter to look human
            time.sleep(random.uniform(1.0, 3.0))
            
            response = session.get(url, timeout=30, verify=False, headers=headers)
            
            if response.status_code == 403:
                # If we get a 403, the server is onto us. Sleep longer.
                wait_time = (2 ** attempt) * 5 + random.uniform(5, 10)
                safe_print(f"!!! 403 Forbidden for {prid}. Waiting {wait_time:.1f}s before retry {attempt+1}/{retries}...")
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            return response.text
        except Exception as e:
            if attempt == retries - 1:
                safe_print(f"Error downloading {url} after {retries} attempts: {e}")
            else:
                time.sleep(random.uniform(2, 5))
    return None

def fetch_discovery_html(day, month, year, retries=3):
    url = f"https://www.pib.gov.in/AllRelease.aspx?d={day}&m={month}&y={year}&lang=1&reg=3"
    safe_print(f"Fetching discovery page: {url}")
    
    for attempt in range(retries):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            response = session.get(url, timeout=30, verify=False, headers=headers)
            
            if response.status_code == 403:
                time.sleep(10 + random.uniform(5, 15))
                continue
                
            response.raise_for_status()
            return response.text
        except Exception as e:
            time.sleep(5)
    return None

def save_as_parquet(results, root_dir="."):
    if not results:
        return

    # Prepare data for DataFrames
    data_rows = []
    meta_rows = []

    for item in results:
        # Extract year from timestamp (e.g., "18 FEB 2026 3:55PM")
        ts = item['metadata'].get('timestamp', '')
        year_match = re.search(r'\d{4}', ts)
        year = year_match.group(0) if year_match else str(datetime.now().year)
        
        # 1. Full Data Row
        data_rows.append({
            "year": year,
            "prid": item['metadata'].get('prid'),
            "title": item.get('title'),
            "text": item.get('text'),
            "images": json.dumps(item.get('images')), # Parquet prefers strings/serials for nested lists in simple schemas
            "ministry": item['metadata'].get('ministry'),
            "timestamp": ts,
            "url": item['metadata'].get('url')
        })

        # 2. Metadata Row (Minimal)
        meta_rows.append({
            "year": year,
            "prid": item['metadata'].get('prid'),
            "title": item.get('title'),
            "ministry": item['metadata'].get('ministry'),
            "timestamp": ts,
            "url": item['metadata'].get('url'),
            "release_id_text": item['metadata'].get('release_id_text')
        })

    df_data = pd.DataFrame(data_rows)
    df_meta = pd.DataFrame(meta_rows)

    # Save by year partitions
    for year in df_data['year'].unique():
        # Path: data/year=YYYY/english/data.parquet
        data_path = os.path.join(root_dir, "data", f"year={year}", "english")
        os.makedirs(data_path, exist_ok=True)
        df_data[df_data['year'] == year].to_parquet(os.path.join(data_path, "data.parquet"), index=False)
        safe_print(f"Stored data parquet: {data_path}/data.parquet")

        # Path: metadata/parquet/year=YYYY/metadata.parquet
        meta_path = os.path.join(root_dir, "metadata", "parquet", f"year={year}")
        os.makedirs(meta_path, exist_ok=True)
        df_meta[df_meta['year'] == year].to_parquet(os.path.join(meta_path, "metadata.parquet"), index=False)
        safe_print(f"Stored metadata parquet: {meta_path}/metadata.parquet")

processed_prids = set()
processed_lock = threading.Lock()

def process_single_prid(prid):
    with processed_lock:
        if prid in processed_prids:
            return None
    
    html = download_page(prid)
    if not html:
        return None
        
    data = extract_release_data(html, prid=prid)
    
    def is_likely_english(text):
        if not text: return False
        detectable = sum(1 for c in text if ord(c) < 128 or c in "‘’“”–—")
        return (detectable / len(text)) > 0.90 if len(text) > 0 else False

    is_english = is_likely_english(data.get('title', ''))
    
    final_data = data
    if not is_english:
        english_url = data.get('metadata', {}).get('languages', {}).get('English')
        if english_url:
            eng_match = re.search(r'PRID=(\d+)', english_url)
            if eng_match:
                eng_prid = eng_match.group(1)
                if eng_prid != prid:
                    with processed_lock:
                        if eng_prid not in processed_prids:
                            processed_prids.add(eng_prid)
                            eng_html = download_page(eng_prid)
                            if eng_html:
                                final_data = extract_release_data(eng_html, prid=eng_prid)
                                final_data['original_prid'] = prid
    
    with processed_lock:
         processed_prids.add(prid)
         
    safe_print(f"Processed {prid} -> {final_data.get('title')[:40]}...")
    return final_data

import argparse
from datetime import datetime, timedelta

def run_range(start_date_str, end_date_str, max_workers=10, output_file="extracted_data.json"):
    # Convert strings to datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    current_date = start_date
    all_results = []
    
    while current_date <= end_date:
        d, m, y = str(current_date.day), str(current_date.month), str(current_date.year)
        safe_print(f"\n--- Processing Date: {y}-{m.zfill(2)}-{d.zfill(2)} ---")
        
        discovery_html = fetch_discovery_html(d, m, y)
        if discovery_html:
            prids = extract_prids(discovery_html)
            if prids:
                safe_print(f"Found {len(prids)} releases for this date.")
                
                # We process each day's releases
                results = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_prid = {executor.submit(process_single_prid, prid): prid for prid in prids}
                    for future in concurrent.futures.as_completed(future_to_prid):
                        try:
                            data = future.result()
                            if data:
                                results.append(data)
                                all_results.append(data)
                        except Exception as exc:
                            safe_print(f'Exception processing PRID: {exc}')
                
                # Save incremental Parquet for the day
                if results:
                    save_as_parquet(results)
            else:
                safe_print("No releases found for this date.")
        
        current_date += timedelta(days=1)

    # Save final consolidated JSON
    if all_results:
        print(f"\nTotal extracted {len(all_results)} records across range.")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, ensure_ascii=False, indent=2)
        print(f"Saved consolidated JSON to {output_file}")
    else:
        print("No records found in the specified range.")

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    parser = argparse.ArgumentParser(description="PIB Press Release Scraper with Date Range support")
    parser.add_argument(
        "--start_date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--max_workers", 
        type=int, 
        default=10, 
        help="Number of concurrent workers for downloads"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="extracted_data.json",
        help="Path to the consolidated JSON output"
    )

    args = parser.parse_args()

    # Default logic: if no dates provided, target February 2026 (as requested)
    # or the current month if we are in another year.
    now = datetime.now()
    if not args.start_date:
        # Default to start of February 2026 for your specific task
        args.start_date = "2026-02-01"
    if not args.end_date:
        # Default to today or end of February
        if now.year == 2026 and now.month == 2:
            args.end_date = now.strftime("%Y-%m-%d")
        else:
            args.end_date = "2026-02-28"

    print(f"Starting crawl from {args.start_date} to {args.end_date}...")
    
    # We need to move process_single_prid inside or make it accessible
    # I'll keep it as it was but ensure it's defined properly.
    
    run_range(args.start_date, args.end_date, max_workers=args.max_workers, output_file=args.output)
