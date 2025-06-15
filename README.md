# Gloodata Slack Utils

Tools for working with Slack data exports and live API data.

## Tools

### Archive Importer (`src/archiveimporter.py`)
Processes Slack data exports and imports them into MeiliSearch.

### API Importer (`src/apiimporter.py`)
Fetches live data from Slack using the Slack SDK. Supports fetching channels, users, and conversations.

#### Usage Examples:
```bash
# Fetch all channels
uv run src/apiimporter.py fetch-channels --output channels.json

# Fetch all users
uv run src/apiimporter.py fetch-users --output users.json

# Fetch conversations from specific channels
uv run src/apiimporter.py fetch-conversations --channels general,random --from-date 2024-01-01 --to-date 2024-01-07
```

## Environment Variables

### MeiliSearch Configuration
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

These environment variables can also be overridden using command line arguments. Run `uv run src/archiveimporter.py --help` for more information.

### Slack API Configuration
To use the API importer, you need to set up a Slack Bot Token:

```
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
```

#### Required Slack App Permissions:
- `channels:read` - Read public channel information
- `users:read` - Read user information
- `channels:history` - Read public channel messages
- `groups:history` - Read private channel messages (if needed)

#### Setting up a Slack App:
1. Go to https://api.slack.com/apps
2. Create a new app or select an existing one
3. Go to "OAuth & Permissions" and add the required scopes
4. Install the app to your workspace
5. Copy the "Bot User OAuth Token" and set it as `SLACK_TOKEN`
