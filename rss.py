import os
import json
import feedparser
import requests
from urllib.parse import urlparse
from lxml import html
from utils import slugify
from db_client import get_db_client
from dotenv import load_dotenv

load_dotenv()
RSS_FILE = os.getenv("RSS_FILE")
PODNEWS_TOP50 = os.getenv("PODNEWS_TOP50")


def update_rss_file():
    """get rss_url for each podcast
    do request on rss_url and save response to file
    overwrites existing file"""
    with get_db_client() as db:
        podcasts = db.get_podcasts()

    rss_podcasts = {}
    for podcast in podcasts:
        r = requests.get(podcast['rss_url'])
        rss = r.text
        rss_podcasts[podcast['title']] = {}
        rss_podcasts[podcast['title']]['rssUrl'] = podcast['rss_url']
        rss_podcasts[podcast['title']]['rss'] = rss

    with open(RSS_FILE, 'w') as f:
        json.dump(rss_podcasts, f, indent=4)


def get_unscraped_episodes():
    episodes_from_rss = read_episodes_info_from_rss()
    rss_ids = [e['unique_id'] for e in episodes_from_rss]
    with get_db_client() as db:
        existing_ids = db.get_existing_ids(rss_ids)
        unscraped_episodes = [
            e for e in episodes_from_rss if e['unique_id'] not in existing_ids]
    unscraped_episodes = [
        e for e in unscraped_episodes if is_valid_url(e['downloadUrl'])]
    # TODO why are any even invalid? what should i do with those?
    return unscraped_episodes


def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def read_episodes_info_from_rss():
    """reads episodes into a list of dicts and creates a unique id based on podcast name and guid that is slugified
    so that it is compatible with filesystems"""
    episodes = []
    with open(RSS_FILE, 'r') as f:
        rss_podcasts = json.load(f)
    for podcast_title in rss_podcasts:
        feed = feedparser.parse(rss_podcasts[podcast_title]['rss'])
        for entry in feed.entries:
            episode = {
                'podcast_title': podcast_title,
                'title': entry.get('title', ''),
                'pubDate': entry.get('published', ''),
                'description': entry.get('description', ''),
                'downloadUrl': entry.enclosures[0].href if 'enclosures' in entry and entry.enclosures else '',
                'guid': entry.get('guid', '')
            }
            # TODO: should i generate guid if blank?
            episode['unique_id'] = slugify(
                podcast_title + '_' + episode['guid'])
            episodes.append(episode)
    return episodes


def get_podnews_top_50_podcasts(output_filepath):
    """scrape web page to identify top50 podcasts. only needs to be done once"""
    r = requests.get(PODNEWS_TOP50)
    tree = html.fromstring(r.content)
    podcast_elements = tree.xpath('//ul/div[@class="artblock"]')
    podcasts = {}
    for podcast_element in podcast_elements:
        podcast_title = podcast_element.xpath('.//div/a/cite')[0].text
        podcast_author = podcast_element.xpath(
            './/b[contains(text(), "From")]/following-sibling::text()')[0]
        podcast_hosted_by = podcast_element.xpath(
            './/b[contains(text(), "Hosted by")]/following-sibling::text()')
        if podcast_hosted_by:
            podcast_hosted_by = podcast_hosted_by[0]
        else:
            podcast_hosted_by = []
        podcast_url = podcast_element.xpath('.//div/a[cite]')[0].attrib['href']
        podcasts[podcast_url] = {
            'title': podcast_title,
            'author': podcast_author,
            'hostedBy': podcast_hosted_by
        }
    with open(output_filepath, 'w') as f:
        json.dump(podcasts, f, indent=4)
