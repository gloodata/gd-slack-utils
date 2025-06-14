#!/usr/bin/env python3
"""
Slack API Importer Module

This module provides command-line tools for fetching data from Slack using the Slack SDK.
It supports fetching channels, users, and conversations with flexible output options.

Usage:
    python src/apiimporter.py fetch-channels --output channels.json
    python src/apiimporter.py fetch-users --output users.json
    python src/apiimporter.py fetch-conversations --channels general,random --from-date 2024-01-01 --to-date 2024-01-08

Requirements:
    - SLACK_TOKEN environment variable must be set
    - Bot must have appropriate OAuth scopes (channels:read, users:read, channels:history, groups:history)
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import time
import urllib.request
import urllib.error
from pathlib import Path

try:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
except ImportError:
    print("Error: slack-sdk is required")
    sys.exit(1)


class SlackAPIImporter:
    """Main class for importing data from Slack API."""

    def __init__(self, token: Optional[str] = None):
        """Initialize the Slack API client."""
        self.token = token or os.getenv("SLACK_TOKEN")
        if not self.token:
            raise ValueError("SLACK_TOKEN environment variable is required")

        self.client = WebClient(token=self.token)
        self._test_connection()

    def _test_connection(self) -> None:
        """Test the Slack API connection."""
        try:
            response = self.client.auth_test()
            print(f"✓ Connected to Slack workspace: {response['team']}")
            print(f"✓ Bot user: {response['user']}")
        except SlackApiError as e:
            raise ConnectionError(
                f"Failed to connect to Slack API: {e.response['error']}"
            )

    def _handle_rate_limit(self, retry_after: int) -> None:
        """Handle rate limiting with exponential backoff."""
        print(f"Rate limited. Waiting {retry_after} seconds...")
        time.sleep(retry_after)

    def _paginate_api_call(self, method: str, **kwargs) -> List[Dict[str, Any]]:
        """Handle paginated API calls."""
        results = []
        cursor = None

        while True:
            try:
                if cursor:
                    kwargs["cursor"] = cursor

                response = getattr(self.client, method)(**kwargs)

                # Handle different response structures
                if "channels" in response:
                    results.extend(response["channels"])
                elif "members" in response:
                    results.extend(response["members"])
                elif "messages" in response:
                    results.extend(response["messages"])

                # Check for next cursor
                if response.get("response_metadata", {}).get("next_cursor"):
                    cursor = response["response_metadata"]["next_cursor"]
                else:
                    break

            except SlackApiError as e:
                if e.response["error"] == "ratelimited":
                    retry_after = int(e.response.get("retry_after", 1))
                    self._handle_rate_limit(retry_after)
                    continue
                else:
                    raise e

        return results

    def fetch_channels(self, output_path: str = "channels.json") -> None:
        """Fetch all channels and save to JSON file."""
        print("Fetching channels...")

        try:
            # Fetch public channels
            public_channels = self._paginate_api_call(
                "conversations_list", types="public_channel"
            )

            # Fetch private channels
            private_channels = self._paginate_api_call(
                "conversations_list", types="private_channel"
            )

            all_channels = public_channels + private_channels

            # Enrich channel data with full API responses
            output_data = []
            for channel in all_channels:
                try:
                    # Get additional channel info
                    info_response = self.client.conversations_info(
                        channel=channel["id"]
                    )
                    # Store the full channel info response
                    output_data.append(info_response["channel"])

                except SlackApiError as e:
                    print(
                        f"Warning: Could not fetch info for channel {channel['name']}: {
                            e
                        }"
                    )
                    # Use basic channel info from list response
                    output_data.append(channel)

            # Save to file
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)

            print(f"✓ Fetched {len(output_data)
                               } channels and saved to {output_path}")

        except SlackApiError as e:
            print(f"Error fetching channels: {e.response['error']}")
            sys.exit(1)

    def fetch_users(self, output_path: str = "users.json") -> None:
        """Fetch all users and save to JSON file."""
        print("Fetching users...")

        try:
            output_data = self._paginate_api_call("users_list")

            # Save to file
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)

            print(f"✓ Fetched {len(output_data)
                               } users and saved to {output_path}")

        except SlackApiError as e:
            print(f"Error fetching users: {e.response['error']}")
            sys.exit(1)

    def _get_channel_id_by_name(self, channel_name: str) -> Optional[str]:
        """Get channel ID by name."""
        try:
            # Try public channels first
            response = self.client.conversations_list(types="public_channel")
            for channel in response["channels"]:
                if channel["name"] == channel_name:
                    return channel["id"]

            # Try private channels
            response = self.client.conversations_list(types="private_channel")
            for channel in response["channels"]:
                if channel["name"] == channel_name:
                    return channel["id"]

            return None

        except SlackApiError:
            return None

    def _fetch_channel_messages(
        self, channel_id: str, channel_name: str, from_ts: float, to_ts: float
    ) -> Dict[str, Any]:
        """Fetch messages from a specific channel within date range."""
        print(f"  Fetching messages from #{channel_name}...")

        messages = []
        cursor = None

        while True:
            try:
                kwargs = {
                    "channel": channel_id,
                    "oldest": str(from_ts),
                    "latest": str(to_ts),
                    "limit": 100,
                }

                if cursor:
                    kwargs["cursor"] = cursor

                response = self.client.conversations_history(**kwargs)
                batch_messages = response.get("messages", [])

                # Store raw messages and fetch replies
                for message in batch_messages:
                    # Store the raw message
                    raw_message = message.copy()

                    # Fetch thread replies if this is a parent message
                    if message.get("reply_count", 0) > 0:
                        try:
                            replies_response = self.client.conversations_replies(
                                channel=channel_id, ts=message["ts"]
                            )
                            # Store the full replies response (including parent message)
                            raw_message["thread_replies"] = replies_response.get(
                                "messages", []
                            )

                        except SlackApiError as e:
                            print(
                                f"    Warning: Could not fetch replies for message {
                                    message['ts']
                                }: {e}"
                            )
                            raw_message["thread_replies"] = []

                    messages.append(raw_message)

                # Check for next cursor
                if response.get("response_metadata", {}).get("next_cursor"):
                    cursor = response["response_metadata"]["next_cursor"]
                else:
                    break

            except SlackApiError as e:
                if e.response["error"] == "ratelimited":
                    retry_after = int(e.response.get("retry_after", 1))
                    self._handle_rate_limit(retry_after)
                    continue
                else:
                    print(f"    Error fetching messages from #{
                          channel_name}: {e}")
                    break

        return {
            "channel_id": channel_id,
            "channel_name": channel_name,
            "messages": messages,
        }

    def fetch_conversations(
        self,
        channels: List[str],
        from_date: str,
        to_date: str,
        output_path: str = "conversations.json",
    ) -> None:
        """Fetch conversations from specified channels within date range."""
        print(f"Fetching conversations from {len(channels)} channels...")

        # Parse dates
        try:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            to_dt = datetime.strptime(
                to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            # Add one day to to_date to include the entire day
            to_dt = to_dt + timedelta(days=1)
        except ValueError as e:
            print(f"Error parsing dates: {e}")
            print("Date format should be YYYY-MM-DD")
            sys.exit(1)

        from_ts = from_dt.timestamp()
        to_ts = to_dt.timestamp()

        conversations = []
        successful_channels = []
        failed_channels = []

        for channel_name in channels:
            channel_id = self._get_channel_id_by_name(channel_name)

            if not channel_id:
                print(f"  ✗ Channel '{
                      channel_name}' not found or not accessible")
                failed_channels.append(channel_name)
                continue

            try:
                channel_data = self._fetch_channel_messages(
                    channel_id, channel_name, from_ts, to_ts
                )
                conversations.append(channel_data)
                successful_channels.append(channel_name)
                print(f"    ✓ Fetched {
                      len(channel_data['messages'])} messages")

            except Exception as e:
                print(f"  ✗ Failed to fetch from #{channel_name}: {e}")
                failed_channels.append(channel_name)

        output_data = conversations

        # Save to file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(f"✓ Fetched conversations from {
              len(successful_channels)} channels and saved to {output_path}")
        if failed_channels:
            print(f"✗ Failed to fetch from {len(failed_channels)} channels: {
                  ', '.join(failed_channels)}")

    def _download_file(self, file_url: str, file_path: str, headers: Dict[str, str]) -> bool:
        """Download a file from URL to local path with authentication headers."""
        try:
            request = urllib.request.Request(file_url, headers=headers)
            with urllib.request.urlopen(request) as response:
                with open(file_path, 'wb') as f:
                    f.write(response.read())
            return True
        except Exception as e:
            print(f"    Error downloading file: {e}")
            return False

    def _fetch_files_in_date_range(self, from_ts: float, to_ts: float) -> List[Dict[str, Any]]:
        """Fetch all files within the specified date range."""
        print("  Fetching files list...")

        all_files = []
        page = 1

        while True:
            try:
                response = self.client.files_list(
                    ts_from=str(int(from_ts)),
                    ts_to=str(int(to_ts)),
                    count=100,
                    page=page
                )

                files = response.get("files", [])
                if not files:
                    break

                all_files.extend(files)

                # Check if there are more pages
                paging = response.get("paging", {})
                if page >= paging.get("pages", 1):
                    break

                page += 1

            except SlackApiError as e:
                if e.response["error"] == "ratelimited":
                    retry_after = int(e.response.get("retry_after", 1))
                    self._handle_rate_limit(retry_after)
                    continue
                else:
                    print(f"    Error fetching files: {e}")
                    break

        return all_files

    def _filter_files_by_channels(self, files: List[Dict[str, Any]], channel_ids: List[str]) -> List[Dict[str, Any]]:
        """Filter files to only include those from specified channels."""
        filtered_files = []

        for file in files:
            # Check if file is in any of the specified channels
            file_channels = file.get("channels", [])
            file_groups = file.get("groups", [])
            file_ims = file.get("ims", [])

            # Combine all channel-like locations
            all_file_locations = file_channels + file_groups + file_ims

            if any(channel_id in all_file_locations for channel_id in channel_ids):
                filtered_files.append(file)

        return filtered_files

    def fetch_attachments(
        self,
        channels: List[str],
        from_date: str,
        to_date: str,
        output_path: str = "attachments",
    ) -> None:
        """Fetch file attachments from specified channels within date range."""
        print(f"Fetching attachments from {len(channels)} channels...")

        # Parse dates
        try:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            to_dt = datetime.strptime(
                to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            to_dt = to_dt + timedelta(days=1)
        except ValueError as e:
            print(f"Error parsing dates: {e}")
            print("Date format should be YYYY-MM-DD")
            sys.exit(1)

        from_ts = from_dt.timestamp()
        to_ts = to_dt.timestamp()

        # Create output directory if it doesn't exist
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get channel IDs for the specified channel names
        channel_ids = []
        channel_name_to_id = {}
        failed_channels = []

        for channel_name in channels:
            channel_id = self._get_channel_id_by_name(channel_name)
            if channel_id:
                channel_ids.append(channel_id)
                channel_name_to_id[channel_id] = channel_name
            else:
                print(f"  ✗ Channel '{
                      channel_name}' not found or not accessible")
                failed_channels.append(channel_name)

        if not channel_ids:
            print("✗ No accessible channels found")
            return

        # Fetch all files in the date range
        all_files = self._fetch_files_in_date_range(from_ts, to_ts)

        if not all_files:
            print("  No files found in the specified date range")
            return

        # Filter files to only include those from specified channels
        channel_files = self._filter_files_by_channels(all_files, channel_ids)

        if not channel_files:
            print("  No files found in the specified channels and date range")
            return

        print(f"  Found {len(channel_files)} files to download")

        # Prepare authentication headers for file downloads
        headers = {
            'Authorization': f'Bearer {self.token}',
            'User-Agent': 'Slack API Importer'
        }

        # Download files
        downloaded_count = 0
        failed_downloads = []
        files_metadata = []

        for file_info in channel_files:
            file_id = file_info.get("id")
            file_name = file_info.get("name", f"file_{file_id}")
            file_url = file_info.get(
                "url_private_download") or file_info.get("url_private")

            if not file_url:
                print(f"    ✗ No download URL found for file: {file_name}")
                failed_downloads.append(file_name)
                continue

            # Determine which channel this file belongs to (use the first one if multiple)
            file_channels = file_info.get("channels", [])
            file_groups = file_info.get("groups", [])
            file_ims = file_info.get("ims", [])

            channel_folder = "unknown"
            for channel_id in file_channels + file_groups + file_ims:
                if channel_id in channel_name_to_id:
                    channel_folder = channel_name_to_id[channel_id]
                    break

            # Create channel subdirectory
            channel_dir = output_dir / channel_folder
            channel_dir.mkdir(exist_ok=True)

            # Create safe filename
            safe_filename = "".join(
                c for c in file_name if c.isalnum() or c in "._- ").strip()
            if not safe_filename:
                safe_filename = f"file_{file_id}"

            # Add timestamp prefix to avoid conflicts
            timestamp = datetime.fromtimestamp(file_info.get(
                "timestamp", 0)).strftime("%Y%m%d_%H%M%S")
            final_filename = f"{timestamp}_{safe_filename}"

            file_path = channel_dir / final_filename

            print(f"    Downloading: {
                  file_name} -> {channel_folder}/{final_filename}")

            if self._download_file(file_url, str(file_path), headers):
                downloaded_count += 1

                # Store metadata
                files_metadata.append({
                    "file_id": file_id,
                    "original_name": file_name,
                    "downloaded_name": final_filename,
                    "channel": channel_folder,
                    "local_path": str(file_path.relative_to(output_dir)),
                    "file_info": file_info
                })
            else:
                failed_downloads.append(file_name)

        # Save metadata file
        metadata_file = output_dir / "files_metadata.json"
        with open(metadata_file, "w", encoding="utf-8") as f:
            json.dump(files_metadata, f, indent=2, ensure_ascii=False)

        # Print summary
        print(f"✓ Downloaded {downloaded_count} files to {output_path}")
        print(f"✓ Metadata saved to {metadata_file}")

        if failed_downloads:
            print(f"✗ Failed to download {len(failed_downloads)} files: {
                  ', '.join(failed_downloads[:5])}")
            if len(failed_downloads) > 5:
                print(f"    ... and {len(failed_downloads) - 5} more")

        if failed_channels:
            print(f"✗ Could not access {len(failed_channels)} channels: {
                  ', '.join(failed_channels)}")


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        description="Slack API Importer - Fetch data from Slack using the API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s fetch-channels --output my_channels.json
  %(prog)s fetch-users --output team_members.json
  %(prog)s fetch-conversations --channels general,random --from-date 2024-01-01 --to-date 2024-01-07
  %(prog)s fetch-attachments --channels general,random --from-date 2024-01-01 --to-date 2024-01-07 --output ./downloads
        """,
    )

    subparsers = parser.add_subparsers(
        dest="command", help="Available commands")

    # fetch-channels subcommand
    channels_parser = subparsers.add_parser(
        "fetch-channels", help="Fetch all channels")
    channels_parser.add_argument(
        "--output",
        "-o",
        default="channels.json",
        help="Output file path (default: channels.json)",
    )

    # fetch-users subcommand
    users_parser = subparsers.add_parser("fetch-users", help="Fetch all users")
    users_parser.add_argument(
        "--output",
        "-o",
        default="users.json",
        help="Output file path (default: users.json)",
    )

    # fetch-conversations subcommand
    conv_parser = subparsers.add_parser(
        "fetch-conversations", help="Fetch conversations from specified channels"
    )
    conv_parser.add_argument(
        "--channels", "-c", required=True, help="Comma-separated list of channel names"
    )
    conv_parser.add_argument(
        "--from-date",
        default=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        help="Start date in YYYY-MM-DD format (default: 7 days ago)",
    )
    conv_parser.add_argument(
        "--to-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format (default: today)",
    )
    conv_parser.add_argument(
        "--output",
        "-o",
        default="conversations.json",
        help="Output file path (default: conversations.json)",
    )

    # fetch-attachments subcommand
    attach_parser = subparsers.add_parser(
        "fetch-attachments", help="Fetch file attachments from specified channels"
    )
    attach_parser.add_argument(
        "--channels", "-c", required=True, help="Comma-separated list of channel names"
    )
    attach_parser.add_argument(
        "--from-date",
        default=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
        help="Start date in YYYY-MM-DD format (default: 7 days ago)",
    )
    attach_parser.add_argument(
        "--to-date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format (default: today)",
    )
    attach_parser.add_argument(
        "--output",
        "-o",
        default="attachments",
        help="Output directory path (default: attachments)",
    )

    return parser


def main() -> None:
    """Main function."""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        importer = SlackAPIImporter()

        if args.command == "fetch-channels":
            importer.fetch_channels(args.output)

        elif args.command == "fetch-users":
            importer.fetch_users(args.output)

        elif args.command == "fetch-conversations":
            channels = [ch.strip() for ch in args.channels.split(",")]
            importer.fetch_conversations(
                channels, args.from_date, args.to_date, args.output
            )

        elif args.command == "fetch-attachments":
            channels = [ch.strip() for ch in args.channels.split(",")]
            importer.fetch_attachments(
                channels, args.from_date, args.to_date, args.output
            )

    except KeyboardInterrupt:
        print("\n✗ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
