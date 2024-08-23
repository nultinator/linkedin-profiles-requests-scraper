import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def crawl_profiles(name, location, retries=3):
    first_name = name.split()[0]
    last_name = name.split()[1]
    url = f"https://www.linkedin.com/pub/dir?firstName={first_name}&lastName={last_name}&trk=people-guest_people-search-bar_search-submit"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed request, Status Code {response.status_code}")

            
                
            soup = BeautifulSoup(response.text, "html.parser")
            profile_cards = soup.find_all("div", class_="base-search-card__info")
            for card in profile_cards:
                href = card.parent.get("href").split("?")[0]
                name = href.split("/")[-1].split("?")[0]
                display_name = card.find("h3", class_="base-search-card__title").text
                location = card.find("p", class_="people-search-card__location").text
                companies = "n/a"
                has_companies = card.find("span", class_="entity-list-meta__entities-list")
                if has_companies:
                    companies = has_companies.text

                search_data = {
                    "name": name,
                    "display_name": display_name,
                    "url": href,
                    "location": location,
                    "companies": companies
                }
                print(search_data)
            
            logger.info(f"Successfully parsed data from: {url}")
            success = True        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries+=1
    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


def start_crawl(profile_list, location, retries=3):
    for name in profile_list:
        crawl_profiles(name, location, retries=retries)



if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["bill gates", "elon musk"]

    ## Job Processes
    filename = "profile-crawl.csv"
    start_crawl(keyword_list, LOCATION, retries=MAX_RETRIES)
    logger.info(f"Crawl complete.")