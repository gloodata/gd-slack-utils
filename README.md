# GD Slack Utils

Tools for working with Slack data exports.

## Environment Variables

To use the MeiliSearch functionality, create a `.env` file in the root directory with the following content:

```
MS_URL=http://127.0.0.1:7700
MS_MASTER_KEY=
MS_INDEX_NAME=SearchIndex
MS_PRIMARY_KEY=id
```

- `MS_URL`: MeiliSearch server URL. Defaults to http://127.0.0.1:7700
- `MS_MASTER_KEY`: MeiliSearch master key. Should be set to a secure value
- `MS_INDEX_NAME`: Name of the MeiliSearch index. Defaults to "SearchIndex"
- `MS_PRIMARY_KEY`: Primary key field for documents. Defaults to "id"

These environment variables can also be overridden using command line arguments. Run `python src/archiveimporter.py --help` for more information.