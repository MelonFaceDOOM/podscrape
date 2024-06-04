import os
import json
import feedparser
from utils import slugify
from config import RSS_FILE, PODNEWS_TOP50
from db_client import get_client
 

""" TODO: add updating rss features. visit rss url. Check guid in entries and add these to existing entries
if they don't exist and then save in dict in json file.
"""

def get_unscraped_episodes():
    client = get_client()
    episodes_from_rss = read_episode_info_from_rss()
    db_id_list = client.get_id_list()
    unscraped_episodes = [e for e in episodes_from_rss if e['unique_id'] not in db_id_list]
    return unscraped_episodes
    

def read_episode_info_from_rss():
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
            episode['unique_id'] = slugify(podcast_title + '_' + episode['guid'])
            episodes.append(episode)
    return episodes
    
    
def get_podnews_top_50_podcasts(output_filepath):
    r = requests.get(PODNEWS_TOP50)
    tree = html.fromstring(r.content)
    podcast_elements = tree.xpath('//ul/div[@class="artblock"]')
    podcasts = {}
    for podcast_element in podcast_elements:
        podcast_title = podcast_element.xpath('.//div/a/cite')[0].text
        podcast_author = podcast_element.xpath('.//b[contains(text(), "From")]/following-sibling::text()')[0]
        podcast_hosted_by = podcast_element.xpath('.//b[contains(text(), "Hosted by")]/following-sibling::text()')
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
