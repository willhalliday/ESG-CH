#!/usr/bin/env python3
"""
Bulk Downloader for Companies House Data (Monthly Only)

This script automates the download of:
 1) The latest basic company data snapshot (CSV) from Companies House (https://download.companieshouse.gov.uk/en_monthlyaccountsdata.html)
 2) Monthly accounts data ZIP files for the previous 12 months.
 

It runs as-is. Currently it saves to a local environment path (lines 29-31) - what will need to be reconfigured is saving the files to, for instance, an Azure Storage Account.
"""

##########################################################

import os
import re
import sys
import time
import logging
from urllib.parse import urljoin
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# Configuration
BASE_URL = "https://download.companieshouse.gov.uk/en_monthlyaccountsdata.html"
REGISTER_URL = "https://download.companieshouse.gov.uk/en_output.html"
DOWNLOAD_DIR = Path("downloads")
BASIC_DIR = DOWNLOAD_DIR / "basic"
MONTHLY_DIR = DOWNLOAD_DIR / "monthly"
RETRY_LIMIT = 3
RETRY_BACKOFF = 5  # seconds

# Month name to number mapping for sorting
MONTH_MAP = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'May': 5, 'June': 6, 'July': 7, 'August': 8,
    'September': 9, 'October': 10, 'November': 11, 'December': 12
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def ensure_dirs():
    BASIC_DIR.mkdir(parents=True, exist_ok=True)
    MONTHLY_DIR.mkdir(parents=True, exist_ok=True)


def fetch_page(url):
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {url}: {e}")
            time.sleep(RETRY_BACKOFF)
    logging.error(f"Failed to fetch page after {RETRY_LIMIT} attempts: {url}")
    sys.exit(1)


def parse_basic_snapshot_link(html):
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(r"BasicCompanyDataAsOneFile-\d{4}-\d{2}-\d{2}\.zip$")
    link = soup.find("a", href=pattern)
    if link:
        return urljoin("https://download.companieshouse.gov.uk/", link["href"])
    logging.error("Could not find basic snapshot link")
    sys.exit(1)


def parse_monthly_links(html, months=12):
    soup = BeautifulSoup(html, "html.parser")
    pattern = re.compile(r"Accounts_Monthly_Data-([A-Za-z]+?)(\d{4})\.zip$")
    links_info = []
    for a in soup.find_all("a", href=pattern):
        href = a["href"]
        m = pattern.search(href)
        month_str, year_str = m.groups()
        if month_str not in MONTH_MAP:
            continue
        year = int(year_str)
        month = MONTH_MAP[month_str]
        url = urljoin(BASE_URL, href)
        links_info.append((year, month, url))
    links_info.sort(key=lambda x: (x[0], x[1]))
    selected = links_info[-months:]
    return [url for (_, _, url) in selected]


def download_file(url, dest_path):
    if dest_path.exists():
        logging.info(f"Skipping existing file: {dest_path.name}")
        return

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = requests.get(url, stream=True, timeout=60)
            resp.raise_for_status()
            total = int(resp.headers.get('content-length', 0))
            with open(dest_path, 'wb') as f, tqdm(
                total=total, unit='B', unit_scale=True, desc=dest_path.name
            ) as bar:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
            return
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed for {url}: {e}")
            time.sleep(RETRY_BACKOFF)
    logging.error(f"Failed to download file after {RETRY_LIMIT} attempts: {url}")


def main():
    ensure_dirs()

    # 1) Download basic company data snapshot if not present
    existing = list(BASIC_DIR.glob("BasicCompanyDataAsOneFile-*.zip"))
    if existing:
        logging.info(f"Basic snapshot already exists ({existing[0].name}); skipping download.")
    else:
        logging.info("Fetching register snapshot page...")
        register_html = fetch_page(REGISTER_URL)
        basic_link = parse_basic_snapshot_link(register_html)
        basic_dest = BASIC_DIR / os.path.basename(basic_link)
        logging.info(f"Downloading basic snapshot: {basic_dest.name}")
        download_file(basic_link, basic_dest)

    # 2) Download last 12 months of accounts archives
    logging.info("Fetching monthly archives page...")
    monthly_html = fetch_page(BASE_URL)
    monthly_links = parse_monthly_links(monthly_html, months=12)
    for link in monthly_links:
        dest = MONTHLY_DIR / os.path.basename(link)
        logging.info(f"Downloading monthly archive: {dest.name}")
        download_file(link, dest)

    logging.info("All downloads completed.")


if __name__ == "__main__":
    main()
