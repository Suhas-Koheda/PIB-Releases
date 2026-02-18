import bs4
import json
import re

def extract_release_data(html_content):
    soup = bs4.BeautifulSoup(html_content, 'html.parser')
    
    data = {}
    
    # 1. Ministry
    ministry_tag = soup.find(id="MinistryName")
    if ministry_tag:
        data['ministry'] = ministry_tag.get_text(strip=True)
    
    # 2. Title
    title_tag = soup.find(id="Titleh2")
    if title_tag:
        data['title'] = title_tag.get_text(strip=True)
        
    # 3. Date and Time
    date_tag = soup.find(id="PrDateTime")
    if date_tag:
        date_text = date_tag.get_text(strip=True)
        # Format: "18 FEB 2026 3:55PM by PIB Delhi" or "प्रविष्टि तिथि: 18 FEB 2026 3:55PM by PIB Delhi"
        # Let's try to extract the date and time part.
        # Removing "प्रविष्टि तिथि:" (Entry Date:) if present
        date_text = date_text.replace("प्रविष्टि तिथि:", "").strip()
        data['date_time_raw'] = date_text
        
        # Simple extraction using regex for DD MMM YYYY H:MM(AM/PM)
        # 18 FEB 2026 3:55PM
        match = re.search(r'(\d{1,2}\s+[A-Z]{3}\s+\d{4}\s+\d{1,2}:\d{2}[AP]M)', date_text, re.IGNORECASE)
        if match:
             data['timestamp'] = match.group(1)
    
    # 4. Text Content
    # The content seems to be in paragraphs with style="text-align:justify" 
    # OR generally inside 'innner-page-main-about-us-content-right-part' div but excluding title, header, date, etc.
    # tailored for this specific structure.
    
    content_div = soup.find('div', class_='innner-page-main-about-us-content-right-part')
    text_content = []
    if content_div:
        # We can extract all <p> tags. 
        # Note: Some <p> tags might be footer/header unrelated.
        # The main content usually follows the date and hydphotoUrl input.
        
        # Heuristic: Find paragraphs that are siblings of PrDateTime or hydphotoUrl?
        # Or just get all paragraphs inside content_div and filter out known short ones or metadata?
        
        for p in content_div.find_all('p'):
            text = p.get_text(strip=True)
            if text and not text.startswith('******') and not text.startswith('Release ID') and not "PIB Delhi" in text: # filtering basic footer junk
                 text_content.append(text)
    
    data['text'] = "\n\n".join(text_content)
    
    # 5. Images
    # Images can be in 'img' tags inside content_div.
    # Exclude layout images like 'printer_icon.png', social media icons, etc.
    # High probable content images often have full paths or look like photo/2026/...
    
    images = []
    if content_div:
        for img in content_div.find_all('img'):
            src = img.get('src')
            if src:
                 if 'socialmedianew' in src or 'printer_icon' in src:
                     continue
                 images.append(src)
    data['images'] = images

    # 6. Language Links
    # <div class="ReleaseLang"> ... <a href='...' > English </a> ... </div>
    lang_div = soup.find('div', class_='ReleaseLang')
    lang_links = {}
    if lang_div:
        for a_tag in lang_div.find_all('a'):
            lang_name = a_tag.get_text(strip=True)
            href = a_tag.get('href')
            if href:
                lang_links[lang_name] = href
    data['languages'] = lang_links

    return data

if __name__ == "__main__":
    with open("release_content.html", "r", encoding="utf-8") as f:
        html = f.read()
    
    data = extract_release_data(html)
    print(json.dumps(data, indent=2, ensure_ascii=False))
