import os
import argparse

from archivereader import (
    RethreadAction,
    archive_channel_extractor,
    foc_history_channel_extractor,
    walk_archive,
    ARCHIVE_TYPES,
)
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

    def to_index(self):
        client = meilisearch.Client(self.url, self.key)
        ensure_index_exists(client, self.index, self.primary_key)
        index = client.index(self.index)

        return client, index

    def to_existing_index(self):
        client = meilisearch.Client(self.url, self.key)
        index = get_existing_index_opt(client, self.index)

        return client, index


def get_existing_index_opt(client, index_uid):
    try:
        return client.get_index(index_uid)
    except meilisearch.errors.MeilisearchApiError as err:
        if err.code == "index_not_found":
            return None
        else:
            raise err


def ensure_index_exists(client, index_uid, primary_key="id"):
    index = get_existing_index_opt(client, index_uid)
    if index is None:
        client.create_index(index_uid, {"primaryKey": primary_key})


def add_common_args(parser):
    parser.add_argument(
        "--archive-format",
        default="archive",
        choices=["archive", "foc-history"],
        help="Format of the Slack export (archive or foc-history)",
    )
    parser.add_argument(
        "--archive-base-path",
        default="slack_archive",
        help="Base path for the Slack export files",
    )


def build_parser():
    parser = argparse.ArgumentParser(description="Meilisearch configuration utility")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Meilisearch subcommand
    meili_parser = subparsers.add_parser(
        "meilisearch", help="Configure Meilisearch connection"
    )
    meili_parser.add_argument(
        "--url", default=MS_URL_DEFAULT, help=f"Meilisearch URL (env: {MS_URL_KEY})"
    )
    meili_parser.add_argument(
        "--index", default=MS_INDEX_DEFAULT, help=f"Index name (env: {MS_INDEX_KEY})"
    )
    meili_parser.add_argument(
        "--primary-key",
        default=MS_PRIMARY_KEY_DEFAULT,
        help=f"Primary key field (env: {MS_PRIMARY_KEY_KEY})",
    )
    add_common_args(meili_parser)

    # Meilisearch from env subcommand
    env_parser = subparsers.add_parser(
        "meilisearch-from-env", help="Configure Meilisearch using environment variables"
    )
    add_common_args(env_parser)

    # Meilisearch delete index from env subcommand
    subparsers.add_parser(
        "meilisearch-delete-index-from-env",
        help="Delete Meilisearch index using environment variables",
    )

    return parser


def parse_args():
    parser = build_parser()
    return parser.parse_args()


def main():
    args = parse_args()
    if args.command == "meilisearch":
        config = MeiliIndexConfig.from_cli_args(args)
    elif args.command == "meilisearch-from-env":
        config = MeiliIndexConfig.from_env()
    elif args.command == "meilisearch-delete-index-from-env":
        print("Running meilisearch-delete-index-from-env")
        config = MeiliIndexConfig.from_env()
        client, index = config.to_existing_index()
        if index:
            print("Deleting index", config.index)
            index.delete()
        else:
            print("Index doesn't exist", config.index)

        return
    else:
        print("No command specified. Use --help for usage information.")
        return

    client, index = config.to_index()
    channel_extractor, glob_pattern = ARCHIVE_TYPES[args.archive_format]

    action = SlackThreadImporter(client, index)
    action.channel_extractor = channel_extractor

    config.show()
    walk_archive(args.archive_base_path, glob_pattern, action)


class SlackThreadImporter(RethreadAction):
    def __init__(self, client, index):
        super().__init__()
        self.client = client
        self.index = index
        self.batch_size = 100

    def before_all(self, base_path, ctx):
        super().before_all(base_path, ctx)
        ctx.print_messages()
        print()
        print("Scanning archive")

    def after_all(self):
        super().after_all()

        print("Indexing documents")
        threads = self.get_sorted_messages_by_ts()
        for i in range(0, len(threads), self.batch_size):
            batch = threads[i : i + self.batch_size]
            docs = [thread_to_ms_doc(thread, self.ctx) for thread in batch]
            if docs:
                print(batch[0].message.dt)
                self.index.add_documents(docs)


def thread_to_ms_doc(thread, ctx):
    m = thread.message
    c = thread.channel
    return dict(
        id=m.ts, content=m.to_mdom(ctx).to_md(), channel_id=c.id, channel_name=c.name
    )


if __name__ == "__main__":
    main()
