ARCHIVE_PATH := "./slack_archive/"
ARCHIVE_FORMAT := "archive"

archive-parse:
    uv run src/archivereader.py parse {{ARCHIVE_FORMAT}} {{ARCHIVE_PATH}}

archive-html:
    uv run src/archivereader.py html {{ARCHIVE_FORMAT}} {{ARCHIVE_PATH}}

archive-md:
    uv run src/archivereader.py md {{ARCHIVE_FORMAT}} {{ARCHIVE_PATH}}

archive-rethread:
    uv run src/archivereader.py rethread {{ARCHIVE_FORMAT}} {{ARCHIVE_PATH}}

archive-emojistats:
    uv run src/archivereader.py emojistats {{ARCHIVE_FORMAT}} {{ARCHIVE_PATH}}

archive-linkstats:
    uv run src/archivereader.py linkstats {{ARCHIVE_FORMAT}} {{ARCHIVE_PATH}}

import-archive-meilisearch:
    uv run src/archiveimporter.py meilisearch-from-env --archive-format {{ARCHIVE_FORMAT}}

meilisearch-delete-index-from-env:
    uv run src/archiveimporter.py meilisearch-delete-index-from-env

gen-emoji-shortcodes:
    uv run tools/genemojicodes.py
