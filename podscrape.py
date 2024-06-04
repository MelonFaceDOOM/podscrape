import argparse
from rss import get_unscraped_episodes, get_podnews_top_50_podcasts
from save import download_episodes_and_save_remotely, download_episodes_and_save_locally
from db_client import get_client

def get_podnews_top_50(output_filepath):
    get_podnews_top_50_podcasts(output_filepath)
    
def scrape_episodes_from_rss_and_save_remotely():
    unscraped_episodes = get_unscraped_episodes()
    unscraped_episodes = unscraped_episodes[:50]
    download_episodes_and_save_remotely(unscraped_episodes)
    
def scrape_episodes_from_rss_and_save_locally():
    unscraped_episodes = get_unscraped_episodes()
    download_episodes_and_save_locally(unscraped_episodes)
    
def db_ep_count():
    client = get_client()
    print(client.ep_count())
    
def main():
    parser = argparse.ArgumentParser(description='PodScrape: A tool for scraping podcast episodes.')
    subparsers = parser.add_subparsers(dest='command')

    parser_top_50 = subparsers.add_parser('get_top_50', help='Get RSS files associated with the Podnews Top 50 podcasts')
    parser_top_50.add_argument('output_filepath', type=str, help='The file path to save the top 50 podcasts')
    parser_remote = subparsers.add_parser('scrape_remote', help='Scrape episodes from RSS feeds and save remotely')
    parser_local = subparsers.add_parser('scrape_local', help='Scrape episodes from RSS feeds and save locally')
    parser_ep_count = subparsers.add_parser('ep_count', help='Check how many episodes are in the database.')
    
    args = parser.parse_args()

    if args.command == 'get_top_50':
        get_podnews_top_50(args.output_filepath)
    elif args.command == 'scrape_remote':
        scrape_episodes_from_rss_and_save_remotely()
    elif args.command == 'scrape_local':
        scrape_episodes_from_rss_and_save_locally()
    elif args.command == 'ep_count':
        db_ep_count()
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
    