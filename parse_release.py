import bs4
import json
import re

def extract_release_data(html_content, prid=None):
    soup = bs4.BeautifulSoup(html_content, 'html.parser')
    
    # 1. Ministry
    ministry = ""
    ministry_tag = soup.find(id="MinistryName")
    if ministry_tag:
        ministry = ministry_tag.get_text(strip=True)
    
    # 2. Title
    title = ""
    title_tag = soup.find(id="Titleh2")
    if title_tag:
        title = title_tag.get_text(strip=True)
        
    # 3. Date and Time
    date_text = ""
    timestamp = ""
    date_tag = soup.find(id="PrDateTime")
    if date_tag:
        date_text = date_tag.get_text(strip=True).replace("प्रविष्टि तिथि:", "").strip()
        # Simple extraction using regex for DD MMM YYYY H:MM(AM/PM)
        match = re.search(r'(\d{1,2}\s+[A-Z]{3}\s+\d{4}\s+\d{1,2}:\d{2}[AP]M)', date_text, re.IGNORECASE)
        if match:
             timestamp = match.group(1)
    
    # 4. Text Content
    content_div = soup.find('div', class_='innner-page-main-about-us-content-right-part')
    text_content = []
    if content_div:
        for p in content_div.find_all('p'):
            text = p.get_text(strip=True)
            if text and not text.startswith('******') and not text.startswith('Release ID') and not "PIB Delhi" in text:
                 text_content.append(text)
    
    full_text = "\n\n".join(text_content)
    
    # 5. Images
    images = []
    if content_div:
        for img in content_div.find_all('img'):
            src = img.get('src')
            if src and not any(x in src for x in ['socialmedianew', 'printer_icon']):
                 images.append(src)

    # 6. Language Links
    lang_div = soup.find('div', class_='ReleaseLang')
    lang_links = {}
    if lang_div:
        for a_tag in lang_div.find_all('a'):
            lang_name = a_tag.get_text(strip=True)
            href = a_tag.get('href')
            if href:
                lang_links[lang_name] = href

    # 7. Release ID from footer (heuristic)
    release_id_text = ""
    release_id_match = re.search(r'Release ID:\s*(\d+)', html_content)
    if release_id_match:
        release_id_text = release_id_match.group(0)

    # Structured Output
    return {
        "title": title,
        "text": full_text,
        "images": images,
        "metadata": {
            "prid": prid,
            "url": f"https://pib.gov.in/PressReleasePage.aspx?PRID={prid}" if prid else None,
            "ministry": ministry,
            "date_raw": date_text,
            "timestamp": timestamp,
            "release_id_text": release_id_text,
            "languages": lang_links
        }
    }

if __name__ == "__main__":
    # Test stub
    import sys
    test_file = "release_content.html"
    if os.path.exists(test_file):
        with open(test_file, "r", encoding="utf-8") as f:
            html = f.read()
        data = extract_release_data(html, prid="TEST")
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print(f"Please create {test_file} to test.")
