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

            print(f"✓ Fetched {len(output_data)} channels and saved to {output_path}")

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

            print(f"✓ Fetched {len(output_data)} users and saved to {output_path}")

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
                    print(f"    Error fetching messages from #{channel_name}: {e}")
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
            to_dt = datetime.strptime(to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
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
                print(f"  ✗ Channel '{channel_name}' not found or not accessible")
                failed_channels.append(channel_name)
                continue

            try:
                channel_data = self._fetch_channel_messages(
                    channel_id, channel_name, from_ts, to_ts
                )
                conversations.append(channel_data)
                successful_channels.append(channel_name)
                print(f"    ✓ Fetched {len(channel_data['messages'])} messages")

            except Exception as e:
                print(f"  ✗ Failed to fetch from #{channel_name}: {e}")
                failed_channels.append(channel_name)

        output_data = conversations

        # Save to file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

        print(
            f"✓ Fetched conversations from {
                len(successful_channels)
            } channels and saved to {output_path}"
        )
        if failed_channels:
            print(
                f"✗ Failed to fetch from {len(failed_channels)} channels: {
                    ', '.join(failed_channels)
                }"
            )


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
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # fetch-channels subcommand
    channels_parser = subparsers.add_parser("fetch-channels", help="Fetch all channels")
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

    except KeyboardInterrupt:
        print("\n✗ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
