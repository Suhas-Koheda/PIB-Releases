import uuid
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)

class PIBDateTask:
    def __init__(self, from_date, to_date):
        self.id = str(uuid.uuid4())
        self.from_date = from_date
        self.to_date = to_date

    def __str__(self):
        return f"PIBDateTask(id={self.id}, from={self.from_date}, to={self.to_date})"


class PIBDownloader:
    def __init__(self, task: PIBDateTask):
        self.task = task
        self.session = requests.Session()
        self.base_url = "https://archive.pib.gov.in/archive2/"
        self.search_url = self.base_url + "AdvSearch.aspx"

        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": self.search_url
        }

    def init_session(self):
        response = self.session.get(self.search_url, headers=self.headers)
        print(response.text)
        response.raise_for_status()
        return response.text

    def extract_viewstate(self, html):
        soup = BeautifulSoup(html, "html.parser")
        viewstate = soup.find("input", {"id": "__VIEWSTATE"})
        return viewstate["value"] if viewstate else ""

    def build_callback_payload(self, viewstate, current_date):
        day = str(current_date.day)
        month = str(current_date.month)
        year = str(current_date.year)

        callback_string = "|".join([
            "1",                # search mode
            "",                 # search text empty
            day,                # from day
            month,              # from month
            year,               # from year
            day,                # to day
            month,              # to month
            year,               # to year
            "0",                # ministry = all
            "2",                # searchtype = full text
            "1"                 # calledfromvalue
        ])

        payload = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": viewstate,
            "__VIEWSTATEGENERATOR": "",
            "__CALLBACKID": "__Page",
            "__CALLBACKPARAM": callback_string
        }

        return payload

    def search_day(self, current_date):
        html = self.init_session()
        viewstate = self.extract_viewstate(html)

        payload = self.build_callback_payload(viewstate, current_date)

        response = self.session.post(
            self.search_url,
            headers=self.headers,
            data=payload
        )
        print(response.text)
        response.raise_for_status()
        return response.text

    def parse_results(self, response_text):
        parts = response_text.split("|")

        if len(parts) < 3:
            return []

        html_content = parts[1]

        soup = BeautifulSoup(html_content, "html.parser")
        links = soup.find_all("a", href=True)

        relids = []

        for link in links:
            href = link["href"]
            if "relid=" in href:
                relid = href.split("relid=")[-1]
                relids.append(relid)

        return list(set(relids))

    def download_release(self, relid):
        url = f"https://archive.pib.gov.in/newsite/PrintRelease.aspx?relid={relid}"
        response = self.session.get(url, headers=self.headers)
        response.raise_for_status()
        return response.text

    def download(self):
        logging.info(f"Processing {self.task}")

        start = datetime.strptime(self.task.from_date, "%d-%m-%Y")
        end = datetime.strptime(self.task.to_date, "%d-%m-%Y")

        current = start

        while current <= end:
            logging.info(f"Searching {current.strftime('%d-%m-%Y')}")

            response_text = self.search_day(current)
            relids = self.parse_results(response_text)

            logging.info(f"Found {len(relids)} releases")

            for relid in relids:
                try:
                    content = self.download_release(relid)
                    print(f"Downloaded relid {relid}")
                except Exception as e:
                    logging.error(f"Error downloading {relid}: {e}")

            current += timedelta(days=1)


if __name__ == "__main__":
    task = PIBDateTask("01-01-2024", "05-01-2024")
    downloader = PIBDownloader(task)
    downloader.download()
