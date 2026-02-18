
# PIB Press Release Scraper

This project scrapes and parses press releases from the Press Information Bureau (PIB), Government of India. It extracts structured data including ministry names, titles, timestamps, full text, and image links.

## Project Structure

```text
└── PIB/
    ├── README.md
    ├── .gitignore
    ├── parse_release.py      # Core parsing logic using BeautifulSoup
    ├── process_releases.py   # High-performance targeted scraper
    └── extracted_data.json   # Output file containing structured JSON data
```

## Dashboard Information

We are querying the **PIB Delhi Dashboard** (`AllRelease.aspx?reg=3`). 
- **Type**: Live Dashboard filtered for the Delhi region.
- **Precision**: By targeting the Delhi region (`reg=3`), the scraper focuses on the primary English press releases, avoiding thousands of redundant regional language duplicates.

## Setup and Usage

1. **Install Dependencies**:
   ```bash
   pip install requests beautifulsoup4
   ```

2. **Run the Scraper**:

   - **Today's releases (Auto-detect)**:
     ```bash
     python3 process_releases.py
     ```

   - **Specific historical date**:
     ```bash
     # Usage: python3 process_releases.py [limit] [day] [month] [year]
     # Example for Feb 18, 2026:
     python3 process_releases.py 0 18 2 2026
     ```

   - **Test the first N items**:
     ```bash
     python3 process_releases.py 10
     ```

## Key Features

- **English-Targeted Discovery**: The scraper intelligently parses the dashboard to find the English version of every release at the discovery stage, minimizing the need for secondary network requests.
- **Regional Filtering**: Specifically targets PIB Delhi (`reg=3`) to ensure high data quality and relevance, matching the view seen by most researchers.
- **In-Memory Discovery**: No clutter. All discovery and PRID extraction happens in memory without creating intermediate HTML files.
- **Automatic Date Detection**: Automatically targets the current date if no date arguments are provided.
- **Multi-threaded Performance**: Uses `ThreadPoolExecutor` with 10 workers to process hundreds of releases in parallel, fetching complete content, metadata, and image links in seconds.
- **Robust Language Logic**: Uses heuristic detection to ensure final data is English, even following cross-language links if a non-English PRID is accidentally encountered.
- **Clean JSON Output**: Generates structured, indented JSON ready for use in data science or LLM applications.
