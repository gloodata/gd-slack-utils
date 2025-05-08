import os
import argparse

import meilisearch

MS_URL_KEY = "MS_URL"
MS_URL_DEFAULT = "http://127.0.0.1:7700"

MS_KEY_KEY = "MS_MASTER_KEY"
MS_KEY_DEFAULT = ""

MS_INDEX_KEY = "MS_INDEX_NAME"
MS_INDEX_DEFAULT = "SearchIndex"

MS_PRIMARY_KEY_KEY = "MS_PRIMARY_KEY"
MS_PRIMARY_KEY_DEFAULT = "id"


class MeiliIndexConfig:
    def __init__(self, url, key, index, primary_key):
        self.url = url
        self.key = key
        self.index = index
        self.primary_key = primary_key

    @classmethod
    def from_env(cls):
        url = os.environ.get(MS_URL_KEY, MS_URL_DEFAULT)
        key = os.environ.get(MS_KEY_KEY, MS_KEY_DEFAULT)
        index = os.environ.get(MS_INDEX_KEY, MS_INDEX_DEFAULT)
        primary_key = os.environ.get(MS_PRIMARY_KEY_KEY, MS_PRIMARY_KEY_DEFAULT)

        return cls(url, key, index, primary_key)

    @classmethod
    def from_cli_args(cls, args):
        key = os.environ.get(MS_KEY_KEY, MS_KEY_DEFAULT)
        return cls(args.url, key, args.index, args.primary_key)

    def show(self):
        print("Meilisearch Configuration:")
        print(f"URL: {self.url}")
        print(f"Key: {'[SET]' if self.key else '[NOT SET]'}")
        print(f"Index: {self.index}")
        print(f"Primary Key: {self.primary_key}")

    def to_index(self, create=True):
        client = meilisearch.Client(self.url, self.key)

        if create:
            index = ensure_index_exists(client, self.index, self.primary_key)
        else:
            index = client.index(self.index)

        return client, index

def ensure_index_exists(client, index_uid, primary_key="id"):
    try:
        client.get_index(index_uid)
    except meilisearch.errors.MeilisearchApiError as err:
        if err.code == "index_not_found":
            client.create_index(index_uid, {"primaryKey": primary_key})
        else:
            raise err

    return client.index(index_uid)

def build_parser():
    parser = argparse.ArgumentParser(description='Meilisearch configuration utility')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Meilisearch subcommand
    meili_parser = subparsers.add_parser('meilisearch', help='Configure Meilisearch connection')
    meili_parser.add_argument('--url', default=MS_URL_DEFAULT,
                             help=f'Meilisearch URL (env: {MS_URL_KEY})')
    meili_parser.add_argument('--index', default=MS_INDEX_DEFAULT,
                             help=f'Index name (env: {MS_INDEX_KEY})')
    meili_parser.add_argument('--primary-key', default=MS_PRIMARY_KEY_DEFAULT,
                             help=f'Primary key field (env: {MS_PRIMARY_KEY_KEY})')

    # Meilisearch from env subcommand
    subparsers.add_parser('meilisearch-from-env', help='Configure Meilisearch using environment variables')

    return parser

def parse_args():
    parser = build_parser()
    return parser.parse_args()

def main():
    args = parse_args()
    if args.command == 'meilisearch':
        config = MeiliIndexConfig.from_cli_args(args)
        config.show()
    elif args.command == 'meilisearch-from-env':
        config = MeiliIndexConfig.from_env()
        config.show()
    else:
        print("No command specified. Use --help for usage information.")

if __name__ == '__main__':
    main()
