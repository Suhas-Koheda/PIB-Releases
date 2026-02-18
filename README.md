
# PIB Press Release Scraper

This project scrapes and parses press releases from the Press Information Bureau (PIB), Government of India. It extracts structured data including ministry names, titles, timestamps, full text, and image links.

## Project Structure

└── PIB/
    ├── README.md
    ├── .gitignore
    ├── parse_release.py      # Core parsing logic using BeautifulSoup
    ├── process_releases.py   # Multi-threaded scraper and processing script
    └── extracted_data.json   # Output file containing structured JSON data

## Dashboard Information

We are querying the **PIB Regular Release Dashboard** (`AllRelease.aspx`). 
- **Type**: Regular/Live Dashboard.
- **Functionality**: This is the primary portal for current press releases. While it allows historical navigation by date, it is the standard interface used for daily releases, as opposed to the specialized Archive/Advanced Search interface which is often less reliable for direct scraping.

## Setup and Usage

1. **Install Dependencies**:
   ```bash
   pip install requests beautifulsoup4
   ```

2. **Run the Scraper**:
   To scrape all releases from the provided source:
   ```bash
   python3 process_releases.py
   ```
   To limit the number of releases for testing:
   ```bash
   python3 process_releases.py 20
   ```

## Key Features

- **Multi-threaded Downloading**: Uses `ThreadPoolExecutor` for high-performance extraction.
- **Language Detection & Switching**: Automatically detects non-English releases and follows links to their English counterparts.
- **Automatic Cleanup**: Temporary HTML files are removed after processing to keep the workspace clean.
- **Structured Output**: Saves final data in a clean, indented JSON format.
