import requests
from bs4 import BeautifulSoup
import re
import urllib3
import logging

# Disable SSL warnings for the older government server
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class PIBScraper:
    """
    A Python requests-based scraper for the PIB Archive.
    It simulates the ASP.NET WebForms AJAX callback to API_Data.aspx.
    """

    def __init__(self):
        self.session = requests.Session()
        self.base_url = "https://archive.pib.gov.in/archive2/"
        self.api_url = self.base_url + "API_Data.aspx"
        self.search_url = self.base_url + "AdvSearch.aspx"
        self.print_url = "https://archive.pib.gov.in/newsite/PrintRelease.aspx"
        
        # Standard Browser Headers
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def get_initial_state(self):
        """
        Step 1: Visit AdvSearch.aspx to establish a session.
        Step 2: Visit API_Data.aspx (the callback target) to obtain its specific __VIEWSTATE.
        """
        logging.info("Initializing session and harvesting ViewState...")
        # Get cookies
        self.session.get(self.search_url, verify=False)
        
        # Get target state
        r = self.session.get(self.api_url, verify=False)
        r.raise_for_status()
        
        soup = BeautifulSoup(r.text, 'html.parser')
        state = {}
        for field in ['__VIEWSTATE', '__VIEWSTATEGENERATOR']:
            tag = soup.find('input', {'name': field}) or soup.find('input', {'id': field})
            if tag:
                state[field] = tag.get('value', '')
        
        return state

    def search(self, day, month, year, search_text="", ministry=0, search_type=2):
        """
        Simulates the AJAX Callback POST to AdvSearch.aspx.
        Verified working configuration.
        """
        # Step 1: Harvest state from AdvSearch.aspx
        logging.info("Harvesting ViewState from AdvSearch.aspx...")
        r_main = self.session.get(self.search_url, verify=False)
        r_main.raise_for_status()
        
        soup = BeautifulSoup(r_main.text, 'html.parser')
        state = {}
        for field in ['__VIEWSTATE', '__VIEWSTATEGENERATOR', '__EVENTVALIDATION']:
            tag = soup.find('input', {'name': field}) or soup.find('input', {'id': field})
            if tag:
                state[field] = tag.get('value', '')
        
        # Step 2: Prepare Callback Param
        callback_param = f"1|{search_text}|{day}|{month}|{year}|{day}|{month}|{year}|{ministry}|{search_type}|1"
        
        # Step 3: Construct Payload
        payload = [
            ("__EVENTTARGET", ""),
            ("__EVENTARGUMENT", ""),
            ("__VIEWSTATE", state.get("__VIEWSTATE", "")),
            ("__VIEWSTATEGENERATOR", state.get("__VIEWSTATEGENERATOR", "")),
        ]
        if state.get("__EVENTVALIDATION"):
            payload.append(("__EVENTVALIDATION", state["__EVENTVALIDATION"]))
            
        payload.extend([
            ("__VIEWSTATEENCRYPTED", ""),
            ("searchtype", str(search_type)),
            ("minname", str(ministry)),
            ("rdate", str(day)),
            ("rmonth", str(month)),
            ("ryear", str(year)),
            ("__CALLBACKID", "__Page"),
            ("__CALLBACKPARAM", callback_param),
        ])
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.search_url,
            "Origin": "https://archive.pib.gov.in",
        }
        
        logging.info(f"Performing callback search for {day}-{month}-{year}...")
        response = self.session.post(self.search_url, data=payload, headers=headers, verify=False)
        response.raise_for_status()
        
        return self._parse_callback_response(response.text)

    def _parse_callback_response(self, text):
        """
        ASP.NET Callbacks return: "0|1|<HTML_RESULTS>|" or "1|<HTML_RESULTS>|"
        """
        # Remove common callback prefixes (0|1| or 1|)
        cleaned_text = re.sub(r'^[01]\|[01]?\|', '', text)
        if cleaned_text.startswith("1|"):
            cleaned_text = cleaned_text[2:]
            
        if cleaned_text.endswith("|"):
            cleaned_text = cleaned_text[:-1]
            
        return self._extract_relids(cleaned_text)

    def _extract_relids(self, html):
        """
        Extracts relid values from the raw HTML results.
        """
        relids = set()
        # Find Getrelease(XXXX) or relid=XXXX
        relids.update(re.findall(r'Getrelease\((\d+)', html))
        relids.update(re.findall(r'relid=(\d+)', html))
        
        unique_ids = sorted(list(relids))
        logging.info(f"Found {len(unique_ids)} releases.")
        return unique_ids

    def download_release(self, relid):
        """
        Downloads the full release text via the PrintRelease.aspx endpoint.
        """
        logging.info(f"Downloading release content for ID: {relid}")
        r = self.session.get(self.print_url, params={"relid": relid}, verify=False)
        r.raise_for_status()
        return r.text

if __name__ == "__main__":
    # Example usage for 1st January 2024
    scraper = PIBScraper()
    rel_ids = scraper.search(1, 1, 2024)
    
    if rel_ids:
        print(f"\nDiscovered Release IDs: {rel_ids}")
        # Test download of the first ID
        content = scraper.download_release(rel_ids[0])
        print(f"Content Length: {len(content)} characters")
        
        with open(f"pib_release_{rel_ids[0]}.html", "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Saved to pib_release_{rel_ids[0]}.html")
    else:
        print("\nNo results found for this date.")