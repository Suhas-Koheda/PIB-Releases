
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

def get_prids_from_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Extract links like PressReleseDetail.aspx?PRID=XXXXX or PressReleasePage.aspx?PRID=XXXXX
    # Note extraction from inspect_all_releases.py showed /PressReleseDetail.aspx
    # regex for PRID
    prids = set()
    matches = re.findall(r'PRID=(\d+)', content)
    for m in matches:
        prids.add(m)
    
    return sorted(list(prids))

def download_page(prid):
    url = f"https://pib.gov.in/PressReleasePage.aspx?PRID={prid}"
    # safe_print(f"Downloading {url}...")
    try:
        response = session.get(url, timeout=30, verify=False)
        response.raise_for_status()
        return response.text
    except Exception as e:
        safe_print(f"Error downloading {url}: {e}")
        return None

def process_releases(input_file="all_releases_en.html", output_file="extracted_data.json", limit=0):
    prids = get_prids_from_file(input_file)
    print(f"Found {len(prids)} unique PRIDs in {input_file}")
    
    if limit > 0:
        prids = prids[:limit]
        print(f"Limiting to first {limit} PRIDs")
    
    if limit > 0:
        prids = prids[:limit]
        print(f"Limiting to first {limit} PRIDs")
    
    results = []
    processed_prids = set()
    processed_lock = threading.Lock()
    
    def process_single_prid(prid):
        # Check if already processed (in case added by English switch)
        with processed_lock:
            if prid in processed_prids:
                return None
        
        html = download_page(prid)
        if not html:
            return None
            
        data = extract_release_data(html)
        
        # Check for English link
        is_english = False
        def is_ascii(s):
            return all(ord(c) < 128 for c in s.replace(' ', '').replace('\n','')) if s else False

        if is_ascii(data.get('title', '')):
            is_english = True
        
        english_url = data.get('languages', {}).get('English')
        
        final_data = data
        
        if not is_english and english_url:
            # safe_print(f"Found English version for {prid}: {english_url}")
            eng_match = re.search(r'PRID=(\d+)', english_url)
            if eng_match:
                eng_prid = eng_match.group(1)
                if eng_prid != prid:
                    # Check if English PRID already processed
                    needed = False
                    with processed_lock:
                        if eng_prid not in processed_prids:
                            needed = True
                            # Mark as processed to avoid duplicates if encountered later
                            processed_prids.add(eng_prid)
                    
                    if needed:
                        safe_print(f"Switching {prid} -> English {eng_prid}")
                        eng_html = download_page(eng_prid)
                        if eng_html:
                            final_data = extract_release_data(eng_html)
                            final_data['original_prid'] = prid
                        else:
                            # Failed to get English, keep original? Or skip?
                            # Keep original but maybe mark it
                            pass
                    else:
                        # Already processed the English one, so we can skip this one?
                        # Or just return None because we want the English version to be in the list
                        # The English version will be processed via its own PRID if in list, 
                        # OR if we just added it to processed_prids, we should return it here?
                        # To simplify: If we switched, return the English data.
                        # If another thread handled the English PRID, we might duplicate work or just skip.
                        # For now, let's just proceed.
                        pass

        with processed_lock:
             processed_prids.add(prid)
             
        safe_print(f"Processed {prid} -> {final_data.get('title')[:30]}...")
        return final_data

    # Use ThreadPoolExecutor
    max_workers = 10
    print(f"Starting processing with {max_workers} workers...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_prid = {executor.submit(process_single_prid, prid): prid for prid in prids}
        
        for future in concurrent.futures.as_completed(future_to_prid):
            prid = future_to_prid[future]
            try:
                data = future.result()
                if data:
                    results.append(data)
            except Exception as exc:
                safe_print(f'{prid} generated an exception: {exc}')
        
    print(f"\nExtracted {len(results)} records.")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"Saved to {output_file}")

if __name__ == "__main__":
    # Disable warnings for unverified HTTPS
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    process_releases(limit=limit)
