import argparse
from rss import get_unscraped_episodes, get_podnews_top_50_podcasts, update_rss_file
from scrape import download_episodes_and_save_remotely, download_episodes_and_save_locally
from db_client import get_db_client


def get_podnews_top_50(output_filepath):
    get_podnews_top_50_podcasts(output_filepath)


def scrape_episodes_from_rss_and_save_locally():
    unscraped_episodes = get_unscraped_episodes()
    print(f"scraping {len(unscraped_episodes)} podcast episodes...")
    download_episodes_and_save_locally(unscraped_episodes)


def scrape_episodes_from_rss_and_save_remotely():
    unscraped_episodes = get_unscraped_episodes()
    print(f"scraping {len(unscraped_episodes)} podcast episodes...")
    download_episodes_and_save_remotely(unscraped_episodes)


def db_ep_count():
    with get_db_client as db:
        print(db.ep_count())


def db_recent_count():
    with get_db_client as db:
        recent_episode_counts = db.recent_episode_counts()
    for row in recent_episode_counts:
        print(f"{row[0]}: {row[1]}")


def update_local():
    update_rss_file()
    scrape_episodes_from_rss_and_save_locally()


def update_remote():
    update_rss_file()
    scrape_episodes_from_rss_and_save_remotely()


def main():
    parser = argparse.ArgumentParser(
        description='PodScrape: A tool for scraping podcast episodes.')
    subparsers = parser.add_subparsers(dest='command')

    parser_top_50 = subparsers.add_parser(
        'get_top_50', help='Get RSS files associated with the Podnews Top 50 podcasts')
    parser_top_50.add_argument(
        'output_filepath', type=str, help='The file path to save the top 50 podcasts')
    parser_remote = subparsers.add_parser(
        'scrape_remote', help='Scrape episodes from RSS feeds and save remotely')
    parser_local = subparsers.add_parser(
        'scrape_local', help='Scrape episodes from RSS feeds and save locally')
    parser_ep_count = subparsers.add_parser(
        'count', help='Check how many episodes are in the database.')
    parser_recent = subparsers.add_parser(
        'recent', help='Check how many episodes were saved in the last week.')
    parser_update_local = subparsers.add_parser(
        'update_local', help='Update RSS feeds, download new episodes, save locally.')
    parser_update_remote = subparsers.add_parser(
        'update_remote', help='Update RSS feeds, download new episodes, save remotely.')

    args = parser.parse_args()

    if args.command == 'get_top_50':
        get_podnews_top_50(args.output_filepath)
    elif args.command == 'scrape_local':
        scrape_episodes_from_rss_and_save_locally()
    elif args.command == 'scrape_remote':
        scrape_episodes_from_rss_and_save_remotely()
    elif args.command == 'count':
        db_ep_count()
    elif args.command == 'recent':
        db_recent_count()
    elif args.command == 'update_local':
        update_local()
    elif args.command == 'update_remote':
        update_remote()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
