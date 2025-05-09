ARCHIVE_PATH := "./slack_archive/"
ARCHIVE_FORMAT := "archive"

archive-action ACTION:
    uv run src/archivereader.py {{ACTION}} {{ARCHIVE_FORMAT}} {{ARCHIVE_PATH}}

archive-parse: (archive-action "parse")
archive-html: (archive-action "html")
archive-text: (archive-action "txt")
archive-md: (archive-action "md")
archive-threads-to-html: (archive-action "threads-to-html")
archive-threads-to-text: (archive-action "threads-to-txt")
archive-threads-to-md: (archive-action "threads-to-md")
archive-threads-to-links: (archive-action "threads-to-links")
archive-rethread: (archive-action "html")
archive-emojistats: (archive-action "emojistats")
archive-linkstats: (archive-action "linkstats")

archive-to-sqlite:
    rm -f slack.sqlite
    uv run src/archivereader.py to-sqlite {{ARCHIVE_FORMAT}} {{ARCHIVE_PATH}}

import-archive-meilisearch:
    uv run src/archiveimporter.py meilisearch-from-env --archive-format {{ARCHIVE_FORMAT}}

meilisearch-delete-index-from-env:
    uv run src/archiveimporter.py meilisearch-delete-index-from-env

gen-emoji-shortcodes:
    uv run tools/genemojicodes.py
