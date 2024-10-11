#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import re
import os
from tqdm import tqdm
import argparse

def get_gaia_file_urls(base_url):
    response = requests.get(base_url)
    if response.status_code != 200:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=re.compile(r'\.csv\.gz$'))
    file_urls = [base_url + link['href'] for link in links]
    return file_urls

def download_file(url, directory):
    local_filename = os.path.join(directory, url.split('/')[-1])
    if os.path.exists(local_filename):
        print(f"File {local_filename} already exists. Skipping.")
        return

    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))

    with open(local_filename, 'wb') as file, tqdm(
        desc=local_filename,
        total=total_size,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as progress_bar:
        for data in response.iter_content(chunk_size=1024):
            size = file.write(data)
            progress_bar.update(size)

def main():
    parser = argparse.ArgumentParser(description="Download Gaia DR2 source files")
    parser.add_argument("--all", action="store_true", help="Download all files")
    parser.add_argument("--num", type=int, help="Number of files to download")
    parser.add_argument("--dir", default=".", help="Directory to save the files")
    args = parser.parse_args()

    base_url = "https://cdn.gea.esac.esa.int/Gaia/gdr2/gaia_source/csv/"
    file_urls = get_gaia_file_urls(base_url)

    if not file_urls:
        print("No files found to download.")
        return

    print(f"Total number of files available: {len(file_urls)}")

    if args.all:
        num_files = len(file_urls)
    elif args.num:
        num_files = min(args.num, len(file_urls))
    else:
        num_files = 1  # Default to downloading one file if no option is specified

    if not os.path.exists(args.dir):
        os.makedirs(args.dir)

    print(f"Downloading {num_files} files to {args.dir}")

    for url in tqdm(file_urls[:num_files], desc="Overall Progress"):
        download_file(url, args.dir)

    print("Download completed.")

if __name__ == "__main__":
    main()
