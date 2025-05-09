import re
import sys
import json
import sqlite3
import argparse

from enum import Enum
from datetime import datetime
from typing import Self
from pathlib import Path
from dataclasses import dataclass

from urltitle import URLTitleReader, URLTitleError


import mdom


class Channels:
    def __init__(self, by_name, by_id):
        self.by_name = by_name
        self.by_id = by_id

    def from_name(self, name):
        return self.by_name.get(name)

    def from_id(self, user_id):
        return self.by_id.get(user_id)


class Users:
    def __init__(self, by_name, by_id):
        self.by_name = by_name
        self.by_id = by_id

    def add_slackbot(self):
        user = Bot("USLACKBOT", "slackbot", {})
        self.by_name[user.name] = user
        self.by_id[user.id] = user

    def from_name(self, name):
        return self.by_name.get(name)

    def from_id(self, user_id):
        return self.by_id.get(user_id)


class Context:
    def __init__(self, users: Users, channels: Channels, shortcode_to_emoji: dict):
        self.users = users
        self.channels = channels
        self.shortcode_to_emoji = shortcode_to_emoji
        self.msgs = []

    def warn(self, type_, data):
        self.msgs.append(dict(level="warn", type=type_, data=data))

    def error(self, type_, data):
        self.msgs.append(dict(level="error", type=type_, data=data))

    def print_messages(self):
        for msg in self.msgs:
            print(msg["level"], msg["type"], msg["data"], file=sys.stderr)

    def message_from_data(self, d):
        t = MessageType.from_subtype(d.get("subtype"))
        user_id = d.get("user")
        ts = d.get("ts")
        thread_ts = d.get("thread_ts")
        text = d.get("text")
        team = d.get("team")
        raw_blocks = d.get("blocks")
        reactions = [
            Reaction(v.get("name", "?"), v.get("count", 1))
            for v in d.get("reactions", [])
        ]

        if raw_blocks is None:
            raw_blocks = [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": blocks_from_text(text),
                        }
                    ],
                }
            ]

        blocks = [Block.from_data(block, self) for block in raw_blocks]

        if user_id is None:
            if t == MessageType.BOT_MESSAGE:
                bot_id = d.get("bot_id")
                username = d.get("username")
                user = Bot(bot_id, username, {})
            elif t == MessageType.FILE_COMMENT:
                user = AnonymousUser()
            else:
                # slack messages about free plan limits have a message field and no user, don't warn
                if d.get("is_hidden_by_limit") is None:
                    self.warn("user-id-is-none", dict(message=d))
                user = AnonymousUser()
        else:
            user = self.users.from_id(user_id)
            if not user:
                self.warn("user-id-not-found", dict(user_id=user_id))
                user = AnonymousUser()

        assert ts is not None
        assert text is not None

        return Message(t, user, ts, thread_ts, text, team, blocks, reactions)

    @classmethod
    def from_base_path(cls, base_path):
        with open("emoji_shortcodes.json", "r") as f:
            shortcode_to_emoji = json.load(f)

        users_path = Path(base_path).joinpath("users.json")
        channels_path = Path(base_path).joinpath("channels.json")

        channels = parse_channels(channels_path)
        users = parse_users(users_path)

        return cls(users, channels, shortcode_to_emoji)


class Channel:
    def __init__(self, id_: str, name: str, is_old_name: bool, info: dict):
        self.id = id_
        self.name = name
        self.info = info
        self.is_old_name = is_old_name

    def to_old_name(self):
        return Channel(self.id, self.name, True, self.info)

    def to_mdom(self, ctx: Context):
        return mdom.Ref("channel", self.id, self.name)


def parse_channels(path):
    by_name = {}
    by_id = {}
    with open(path, "r", encoding="utf-8") as handle:
        items = json.load(handle)
        for info in items:
            id_ = info.get("id")
            name = info.get("name")
            g = Channel(id_, name, False, info)
            by_name[name] = g
            by_id[id_] = g
            prev_names = info.get("previous_names", [])

            for name in prev_names:
                # some groups are their own previous names
                if name not in by_name:
                    by_name[name] = g.to_old_name()

    return Channels(by_name, by_id)


class User:
    def __init__(self, id_: str | None, name: str | None, info: dict):
        self.id = id_
        self.name = name
        self.info = info

    def to_mdom(self, ctx: Context):
        return mdom.Ref("user", self.id or "", self.name or "")


class Bot(User):
    def to_mdom(self, ctx: Context):
        return mdom.Ref("bot", self.id or "", self.name or "")


class AnonymousUser(User):
    def __init__(self):
        super().__init__(None, None, {})

    def to_mdom(self, ctx: Context):
        return mdom.Ref("nouser", "", "")


def walk_blocks_for_leaf_instance(blocks, cls, fn):
    for block in blocks:
        if isinstance(block, BlockRichText):
            for element in block.elements:
                if isinstance(element, RichTextList):
                    walk_blocks_for_leaf_instance(element.elements, cls, fn)
                else:
                    for sub_element in element.elements:
                        if isinstance(sub_element, cls):
                            fn(sub_element)


class MessageType(Enum):
    MESSAGE = "message"
    BOT_MESSAGE = "bot_message"
    CHANNEL_JOIN = "channel_join"
    CHANNEL_NAME = "channel_name"
    CHANNEL_PURPOSE = "channel_purpose"
    CHANNEL_TOPIC = "channel_topic"
    FILE_COMMENT = "file_comment"
    ME_MESSAGE = "me_message"
    REPLY_BROADCAST = "reply_broadcast"
    THREAD_BROADCAST = "thread_broadcast"
    TOMBSTONE = "tombstone"
    UNKNOWN = "unknown"

    @classmethod
    def from_subtype(cls, type_str: str | None):
        match type_str:
            case None:
                return cls.MESSAGE
            case "bot_message":
                return cls.BOT_MESSAGE
            case "channel_join":
                return cls.CHANNEL_JOIN
            case "channel_name":
                return cls.CHANNEL_NAME
            case "channel_purpose":
                return cls.CHANNEL_PURPOSE
            case "channel_topic":
                return cls.CHANNEL_TOPIC
            case "file_comment":
                return cls.FILE_COMMENT
            case "me_message":
                return cls.ME_MESSAGE
            case "reply_broadcast":
                return cls.REPLY_BROADCAST
            case "thread_broadcast":
                return cls.THREAD_BROADCAST
            case "tombstone":
                return cls.TOMBSTONE
            case _:
                return cls.UNKNOWN


@dataclass
class Reaction:
    name: str
    count: int = 1


class Message:
    def __init__(
        self,
        type_: MessageType,
        user: User,
        ts: str,
        thread_ts: str,
        text: str,
        team: str | None,
        blocks: list,
        reactions: list[Reaction],
    ):
        self.type = type_
        self.user = user
        self.ts = ts
        self.thread_ts = thread_ts
        self.dt = datetime.fromtimestamp(float(ts))
        self.text = text
        self.team = team
        self.blocks = blocks
        self.reactions = reactions

    def get_emojis(self):
        items = []
        walk_blocks_for_leaf_instance(
            self.blocks, RichTextSectionElementEmoji, lambda e: items.append(e.name)
        )
        return items

    def get_links(self) -> list["RichTextSectionElementLink"]:
        items = []
        walk_blocks_for_leaf_instance(
            self.blocks, RichTextSectionElementLink, lambda e: items.append(e)
        )
        return items

    def to_mdom(self, ctx: Context):
        childs = [
            mdom.Group(
                "msg-head",
                [mdom.Span("isodate", self.dt.isoformat()), self.user.to_mdom(ctx)],
            )
        ]

        for block in self.blocks:
            childs.append(block.to_mdom(ctx))

        return mdom.Block("message", childs)


class Thread:
    def __init__(self, message: Message, replies: list[Message], channel: Channel):
        self.message = message
        self.replies = replies
        self.channel = channel

    def get_links(self) -> list["RichTextSectionElementLink"]:
        replies_links = [link for r in self.replies for link in r.get_links()]
        root_links = self.message.get_links()
        return root_links + replies_links

    def to_mdom(self, ctx: Context):
        childs = [
            self.message.to_mdom(ctx),
            mdom.Block("replies", [reply.to_mdom(ctx) for reply in self.replies]),
        ]
        return mdom.Section("thread", childs)


RE_BETWEEN_LT_AND_GT = re.compile(r"<(.*?)>")
RE_EMOJI_SHORTCODE = re.compile(r"(?:\s|:|^):([a-zA-Z0-9_+-]+):(?:\s|:|$)")


def parse_raw_slack_text_field_to_json(text):
    parsed_elements = []

    def parse_element(element):
        if element.startswith("@"):
            return {"type": "user", "user_id": element[1:].split("|")[0]}
        elif element.startswith("#"):
            return {"type": "channel", "channel_id": element[1:].split("|")[0]}
        elif element.startswith("!"):
            return {"type": "broadcast", "range": element[1:]}
        elif "|" in element:
            url, display_text = element.split("|", 1)
            return {"type": "link", "url": url, "text": display_text}
        else:
            return {"type": "link", "url": element}

    last_end = 0
    for match in RE_BETWEEN_LT_AND_GT.finditer(text):
        if match.start() > last_end:
            parsed_elements.append(
                {"type": "text", "text": text[last_end : match.start()]}
            )
        parsed_elements.append(parse_element(match.group(1)))
        last_end = match.end()

    if last_end < len(text):
        parsed_elements.append({"type": "text", "text": text[last_end:]})

    return parsed_elements


def blocks_from_text(text):
    return parse_raw_slack_text_field_to_json(text)


class Block:
    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> type[Self]:
        match d.get("type"):
            case "rich_text":
                return BlockRichText.from_data(d, ctx)
            case "section":
                return BlockSection.from_data(d, ctx)
            case "context":
                return BlockContext.from_data(d, ctx)
            case "actions":
                return BlockActions.from_data(d, ctx)
            case _:
                ctx.warn("unknown-block-type", dict(block=d))
                return BlockUnknown(d)

    def to_mdom(self, ctx: Context) -> type[mdom.Node]:
        return mdom.Block("block", [])


class BlockUnknown(Block):
    def __init__(self, data):
        self.data = data


class RichTextSectionElement:
    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> type[Self]:
        match d.get("type"):
            case "link":
                return RichTextSectionElementLink.from_data(d, ctx)
            case "text":
                return RichTextSectionElementText.from_data(d, ctx)
            case "emoji":
                return RichTextSectionElementEmoji.from_data(d, ctx)
            case "channel":
                return RichTextSectionElementChannel.from_data(d, ctx)
            case "user":
                return RichTextSectionElementUser.from_data(d, ctx)
            case "broadcast":
                return RichTextSectionElementBroadcast.from_data(d, ctx)
            case "color":
                return RichTextSectionElementColor.from_data(d, ctx)
            case _:
                ctx.warn("unknown-rich-text-section-type", dict(section=d))
                return RichTextSectionElementUnknown.from_data(d, ctx)

    def to_mdom(self, ctx: Context) -> mdom.Node:
        return mdom.Span("RichTextSectionElement", "")


class RichTextElement:
    elements: list[type[RichTextSectionElement]]

    def __init__(self, elements: list[type[RichTextSectionElement]]):
        self.elements = elements

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> type[Self]:
        match d.get("type"):
            case "rich_text_section":
                return RichTextSection.from_data(d, ctx)
            case "rich_text_list":
                return RichTextList.from_data(d, ctx)
            case "rich_text_quote":
                return RichTextQuote.from_data(d, ctx)
            case "rich_text_preformatted":
                return RichTextPreformatted.from_data(d, ctx)
            case _:
                ctx.warn("unknown-rich-text-element-type", dict(element=d))
                return RichTextUnknown(d)


class RichTextSection(RichTextElement):
    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        elements = [
            RichTextSectionElement.from_data(e, ctx) for e in d.get("elements", [])
        ]
        return cls(elements)

    def to_mdom(self, ctx: Context) -> mdom.Node:
        childs = [e.to_mdom(ctx) for e in self.elements]
        return mdom.Paragraph("Section", childs)


class RichTextQuote(RichTextElement):
    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        elements = [
            RichTextSectionElement.from_data(e, ctx) for e in d.get("elements", [])
        ]
        return cls(elements)

    def to_mdom(self, ctx: Context):
        childs = [e.to_mdom(ctx) for e in self.elements]
        return mdom.Quote(childs)


class RichTextPreformatted(RichTextElement):
    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        elements = [
            RichTextSectionElement.from_data(e, ctx) for e in d.get("elements", [])
        ]
        return cls(elements)

    def to_mdom(self, ctx: Context):
        childs = [e.to_mdom(ctx) for e in self.elements]
        return mdom.Preformatted(childs)


class RichTextList(RichTextElement):
    def __init__(
        self,
        elements: list[type[RichTextSectionElement]],
        indent=0,
        offset=0,
        list_style="bullet",
    ):
        super().__init__(elements)
        self.indent = indent
        self.offset = offset
        self.list_style = list_style

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        indent = d.get("indent", 0)
        offset = d.get("offset", 0)
        list_style = d.get("style", "bullet")  # or ordered

        # NOTE: it's RichTextSection not RichTextSectionElement like others
        elements = [RichTextSection.from_data(e, ctx) for e in d.get("elements", [])]
        return cls(elements, indent, offset, list_style)

    def to_mdom(self, ctx: Context):
        childs = [e.to_mdom(ctx) for e in self.elements]
        if self.list_style == "bullet":
            return mdom.List(childs)

        return mdom.OrderedList(childs)


class Style:
    def __init__(
        self, italic=False, code=False, bold=False, strike=False, unlink=False
    ):
        self.italic = italic
        self.code = code
        self.bold = bold
        self.strike = strike
        self.unlink = unlink

    @classmethod
    def from_data(cls, d: dict) -> Self:
        return cls(**d)


class RichTextSectionElementLink(RichTextSectionElement):
    def __init__(self, url: str, text: str, style: Style):
        self.url = url
        self.text = text
        self.style = style

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        url = d["url"]
        text = d.get("text")
        style = Style.from_data(d.get("style", {}))

        if not text:
            text = url

        return cls(url, text, style)

    def __repr__(self):
        return f"Link({self.url}, {self.text})"

    def __str__(self):
        return f"<{self.url}|{self.text}>"

    def to_mdom(self, ctx: Context) -> mdom.Node:
        return mdom.Ref("link", self.url, self.text)


class RichTextSectionElementText(RichTextSectionElement):
    def __init__(self, text: str, style: Style):
        self.text = text
        self.style = style

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        def replace_match(match):
            shortcode = match.group(1)
            emoji = ctx.shortcode_to_emoji.get(shortcode)
            return emoji if emoji is not None else f":{shortcode}:"

        text = RE_EMOJI_SHORTCODE.sub(replace_match, d["text"])

        style = Style.from_data(d.get("style", {}))
        return cls(text, style)

    def to_mdom(self, ctx: Context):
        return mdom.Span("text", self.text)


class RichTextSectionElementEmoji(RichTextSectionElement):
    def __init__(self, name: str, unicode: str | None):
        self.name = name
        self.unicode = unicode

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        name = d["name"]
        unicode = d.get("unicode")

        return cls(name, unicode)

    def to_mdom(self, ctx: Context):
        text = ctx.shortcode_to_emoji.get(self.name)

        if text is None:
            text = f":{self.name}:"

        return mdom.Span("emoji", text)


class RichTextSectionElementChannel(RichTextSectionElement):
    def __init__(self, channel: Channel, style: Style):
        self.channel = channel
        self.style = style

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        channel_id = d["channel_id"]
        channel = ctx.channels.from_id(channel_id)
        if channel is None:
            ctx.warn("channel-id-not-found", dict(channel_id=channel_id))
            channel = Channel(channel_id, channel_id, False, d)

        style = Style.from_data(d.get("style", {}))
        return cls(channel, style)

    def to_mdom(self, ctx: Context):
        return mdom.Ref("channel", self.channel.id, self.channel.name)


class RichTextSectionElementUser(RichTextSectionElement):
    def __init__(self, user: User, style: Style):
        self.user = user
        self.style = style

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        user_id = d["user_id"]
        user = ctx.users.from_id(user_id)
        if user is None:
            ctx.warn("user-id-not-found", dict(user_id=user_id))
            user = User(user_id, user_id, d)

        style = Style.from_data(d.get("style", {}))
        return cls(user, style)

    def to_mdom(self, ctx: Context):
        return mdom.Ref("user", self.user.id or "", self.user.name or "")


class RichTextSectionElementBroadcast(RichTextSectionElement):
    def __init__(self, b_range: str):
        self.range = b_range

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        b_range = d["range"]  # seen everyone and channel
        return cls(b_range)

    def to_mdom(self, ctx: Context):
        return mdom.Block("broadcast", [])


class RichTextSectionElementColor(RichTextSectionElement):
    def __init__(self, value: str):
        self.value = value

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        value = d["value"]  # seen #rrggbb
        return cls(value)

    def to_mdom(self, ctx: Context):
        return mdom.Span("color", self.value)


class RichTextSectionElementUnknown(RichTextSectionElement):
    def __init__(self, data):
        self.data = data

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        return cls(d)

    def to_mdom(self, ctx: Context):
        return mdom.Span("element-unknown", "")


class RichTextUnknown(RichTextElement):
    def __init__(self, data):
        self.data = data

    def to_mdom(self, ctx: Context):
        return mdom.Span("unknown", "")


class BlockRichText(Block):
    def __init__(self, elements: list[type[RichTextElement]]):
        self.elements = elements

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        elements = [RichTextElement.from_data(e, ctx) for e in d.get("elements", [])]
        return cls(elements)

    def to_mdom(self, ctx: Context):
        childs = [e.to_mdom(ctx) for e in self.elements]
        return mdom.Block("rich_text", childs)


class BlockSection(Block):
    def __init__(self):
        pass

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        # poll stuff
        return cls()

    def to_mdom(self, ctx: Context):
        return mdom.Section("section", [])


class BlockContext(Block):
    def __init__(self):
        pass

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        # {'type': 'context', 'block_id': 'vfA', 'elements': [{'type':
        # 'mrkdwn', 'text': 'Created by <@UC...> with /poll', 'verbatim':
        # False}]}
        return cls()

    def to_mdom(self, ctx: Context):
        return mdom.Section("context", [])


class BlockActions(Block):
    def __init__(self, data):
        self.data = data

    @classmethod
    def from_data(cls, d: dict, ctx: Context) -> Self:
        # {'type': 'actions', 'block_id': 'add_option_to_poll&1...',
        # 'elements': [{'type': 'button', 'action_id': 'open_unified_modal',
        # 'text': {'type': 'plain_text', 'text': 'Open', 'emoji': True},
        # 'style': 'primary'}, {'type': 'button', 'action_id': 'add_option',
        # 'text': {'type': 'plain_text', 'text': 'Add option', 'emoji':
        # True}}]}
        return cls(d)

    def to_mdom(self, ctx: Context):
        return mdom.Section("actions", [])


def parse_users(path):
    by_name = {}
    by_id = {}
    with open(path, "r", encoding="utf-8") as handle:
        items = json.load(handle)
        for info in items:
            id_ = info.get("id")
            real_name = info.get("real_name")
            display_name = info.get("profile", {}).get("display_name")
            name = display_name or real_name or info.get("name") or id_
            user = User(id_, name, info)
            by_name[name] = user
            by_id[id_] = user

    users = Users(by_name, by_id)
    users.add_slackbot()
    return users


def natural_sort_paths(paths):
    def alphanum_key(path):
        return [int(text) if text.isdigit() else text.lower() for text in path.parts]

    return sorted(paths, key=alphanum_key)


def walk_archive(base_path, glob_pattern, action):
    ctx = Context.from_base_path(base_path)

    action.before_all(base_path, ctx)

    file_paths = natural_sort_paths(Path(base_path).glob(glob_pattern))

    for file_path in file_paths:
        # print("Processing", file_path)
        action.before_file(file_path)

        with open(file_path, "r", encoding="utf-8") as handle:
            msgs = json.load(handle)
            for msg in msgs:
                action.on_msg(msg)

        action.after_file()

    action.after_all()
    return ctx


def archive_channel_extractor(path, base_path):
    # assumes the file name is the first directory
    glob_path = path.relative_to(base_path)
    return glob_path.parts[0]


def foc_history_channel_extractor(path, base_path):
    # assumes the file name is the channel name
    glob_path = path.relative_to(base_path)
    return glob_path.stem


class BaseAction:
    cur_file: Path | None
    cur_channel: Channel

    def __init__(self, channel_extractor=archive_channel_extractor):
        self.channel_extractor = channel_extractor

        self.base_path = Path(".")
        self.ctx = Context(Users({}, {}), Channels({}, {}), {})

        self.cur_file = None
        self.cur_channel = Channel("?", "?", False, {})

    def before_all(self, base_path, ctx):
        self.base_path = base_path
        self.ctx = ctx

    def after_all(self):
        pass

    def before_file(self, file_path):
        self.cur_file = file_path
        channel_name = self.channel_extractor(file_path, self.base_path)
        self.cur_channel = self.ctx.channels.from_name(channel_name)

    def after_file(self):
        self.cur_file = None
        self.cur_channel = Channel("?", "?", False, {})

    def on_msg(self, msg):
        if msg.get("type") == "message":
            message = self.ctx.message_from_data(msg)
            self.on_message(message)
        else:
            self.on_unknown(msg)

    def on_message(self, msg):
        pass

    def on_unknown(self, m):
        # canvas thing
        if m.get("mimetype") != "application/vnd.slack-docs":
            self.ctx.warn("unknown-message-type", dict(message=m))


class ParseAction(BaseAction):
    pass


class ToHTMLAction(BaseAction):
    def on_message(self, msg):
        print(msg.to_mdom(self.ctx).to_html_str())
        print()


class ToMarkdownAction(BaseAction):
    def on_message(self, msg):
        print(msg.to_mdom(self.ctx).to_md())
        print()


class ToTextAction(BaseAction):
    def on_message(self, msg):
        print(msg.to_mdom(self.ctx).to_text())
        print()


class ToEmojiStatsAction(BaseAction):
    def __init__(self):
        super().__init__()
        self.emoji_count = {}

    def on_message(self, msg):
        for emoji in msg.get_emojis():
            if emoji in self.emoji_count:
                self.emoji_count[emoji] += 1
            else:
                self.emoji_count[emoji] = 1

    def after_all(self):
        for shortcode, count in sorted(
            self.emoji_count.items(), key=lambda x: x[1], reverse=True
        ):
            emoji = self.ctx.shortcode_to_emoji.get(shortcode, "?")
            print(f"{shortcode} ({emoji}): {count}")


class ToLinkStatsAction(BaseAction):
    def __init__(self):
        super().__init__()
        self.link_count = {}
        self.link_by_url = {}

    def on_message(self, msg):
        for link in msg.get_links():
            url = link.url
            if url in self.link_count:
                self.link_count[url] += 1
            else:
                self.link_count[url] = 1

            if url not in self.link_by_url:
                self.link_by_url[url] = link

    def after_all(self):
        reader = URLTitleReader(verify_ssl=False)
        items = sorted(self.link_count.items(), key=lambda x: x[1], reverse=True)
        for url, count in items:
            link = self.link_by_url[url]
            try:
                title = reader.title(url)
            except URLTitleError:
                title = link.text

            print(f"{url} ({link.text}): {count}\t{title}")


class RethreadAction(BaseAction):
    def __init__(self):
        super().__init__()
        self.threads = {}
        # edit a message that refers to a message that happened after the original?
        self.orphans = []

    def on_message(self, msg):
        ts = msg.ts
        thread_ts = msg.thread_ts

        if thread_ts is None or ts == thread_ts:
            if ts in self.threads:
                cur_thread = self.threads[ts]
                cur_thread.message = msg
                # usually this is an edited message
                # log if the user is different
                if msg.user.id != cur_thread.message.user.id:
                    self.ctx.warn("duplicated-thread-ts", dict(message=msg))
            else:
                self.threads[ts] = Thread(msg, [], self.cur_channel)
        else:
            if thread_ts not in self.threads:
                self.orphans.append(msg)
            else:
                self.threads[thread_ts].replies.append(msg)

    def process_orphans(self):
        for msg in self.orphans:
            thread = self.threads.get(msg.thread_ts)
            if thread:
                thread.replies.append(msg)
            else:
                self.ctx.warn("orphan-msg", dict(message=msg))

    def get_sorted_messages_by_ts(self):
        return sorted(self.threads.values(), key=lambda x: x.message.ts)

    def after_all(self):
        self.process_orphans()


class ToSQLite(RethreadAction):
    def __init__(
        self, db_path="slack.sqlite", store_txt=True, store_md=True, store_html=True
    ):
        super().__init__()
        self.batch_size = 200
        self.conn = sqlite3.connect(db_path)
        self.store_txt = store_txt
        self.store_md = store_md
        self.store_html = store_html
        self.link_info = LinkCollector()
        self.initialize_tables()

    def initialize_tables(self):
        cursor = self.conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user (
            id TEXT PRIMARY KEY,
            name TEXT
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel (
            id TEXT PRIMARY KEY,
            name TEXT
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS thread (
            ts TEXT PRIMARY KEY,
            date TEXT,
            channel_id TEXT,
            user_id TEXT,
            reply_count INTEGER,
            FOREIGN KEY (channel_id) REFERENCES channel (id),
            FOREIGN KEY (user_id) REFERENCES user (id)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS reaction (
            ts TEXT PRIMARY KEY,
            name TEXT,
            count INTEGER DEFAULT 1,
            FOREIGN KEY (ts) REFERENCES thread (ts)
        )
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS link (
            ts TEXT,
            url TEXT,
            text TEXT,
            count INTEGER DEFAULT 1,
            FOREIGN KEY (ts) REFERENCES thread (ts)
        )
        """)

        if self.store_txt:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS thread_txt (
                ts TEXT PRIMARY KEY,
                content TEXT,
                FOREIGN KEY (ts) REFERENCES thread (ts)
            )
            """)

        if self.store_md:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS thread_md (
                ts TEXT PRIMARY KEY,
                content TEXT,
                FOREIGN KEY (ts) REFERENCES thread (ts)
            )
            """)

        if self.store_html:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS thread_html (
                ts TEXT PRIMARY KEY,
                content TEXT,
                FOREIGN KEY (ts) REFERENCES thread (ts)
            )
            """)

        self.conn.commit()

    def insert_many(self, query, rows):
        cursor = self.conn.cursor()
        cursor.executemany(query, rows)
        self.conn.commit()

    def insert_users(self, users):
        self.insert_many(
            "INSERT OR REPLACE INTO user (id, name) VALUES (:id, :name)", users
        )

    def insert_channels(self, channels):
        self.insert_many(
            "INSERT OR REPLACE INTO channel (id, name) VALUES (:id, :name)", channels
        )

    def insert_reactions(self, reactions):
        self.insert_many(
            "INSERT OR REPLACE INTO reaction (ts, name, count) VALUES (:ts, :name, :count)",
            reactions,
        )

    def insert_links(self, links):
        self.insert_many(
            "INSERT OR REPLACE INTO link (ts, url, text, count) VALUES (:ts, :url, :text, :count)",
            links,
        )

    def insert_threads(self, threads):
        self.insert_many(
            "INSERT OR REPLACE INTO thread (ts, date, channel_id, user_id, reply_count) VALUES (:ts, :date, :channel_id, :user_id, :reply_count)",
            threads,
        )

    def insert_threads_txt(self, threads):
        self.insert_many(
            "INSERT OR REPLACE INTO thread_txt (ts, content) VALUES (:ts, :content)",
            threads,
        )

    def insert_threads_md(self, threads):
        self.insert_many(
            "INSERT OR REPLACE INTO thread_md (ts, content) VALUES (:ts, :content)",
            threads,
        )

    def insert_threads_html(self, threads):
        self.insert_many(
            "INSERT OR REPLACE INTO thread_html (ts, content) VALUES (:ts, :content)",
            threads,
        )

    def after_all(self):
        super().after_all()

        print("Inserting channels")
        self.insert_channels(
            [
                dict(id=item.id, name=item.name)
                for item in self.ctx.channels.by_id.values()
            ]
        )

        print("Inserting users")
        self.insert_users(
            [dict(id=item.id, name=item.name) for item in self.ctx.users.by_id.values()]
        )

        print("Inserting threads")
        threads = self.get_sorted_messages_by_ts()
        link_info = LinkCollector()
        for i in range(0, len(threads), self.batch_size):
            batch = threads[i : i + self.batch_size]
            rows = [self.thread_to_db_record(thread) for thread in batch]
            if rows:
                print(batch[0].message.dt)
                self.insert_threads(rows)

                reactions = []
                mdoms = []

                for t in batch:
                    ts = t.message.ts
                    node = t.message.to_mdom(self.ctx)
                    link_info.handle_thread(t)
                    mdoms.append((ts, node))
                    for r in t.message.reactions:
                        reactions.append(dict(ts=ts, name=r.name, count=r.count))

                self.insert_reactions(reactions)

                if self.store_txt:
                    rows = [
                        dict(ts=ts, content=node.to_text().strip())
                        for ts, node in mdoms
                    ]
                    self.insert_threads_txt(rows)

                if self.store_md:
                    rows = [
                        dict(ts=ts, content=node.to_md().strip()) for ts, node in mdoms
                    ]
                    self.insert_threads_md(rows)

                if self.store_html:
                    rows = [
                        dict(ts=ts, content=node.to_html_str()) for ts, node in mdoms
                    ]
                    self.insert_threads_html(rows)

            self.insert_links(
                [
                    dict(
                        ts=ts, url=info.link.url, text=info.link.text, count=info.count
                    )
                    for info in link_info.link_info.values()
                    for ts in info.refs
                ]
            )

    def thread_to_db_record(self, thread):
        m = thread.message
        u = m.user
        c = thread.channel
        date = m.dt.isoformat()

        return dict(
            ts=m.ts,
            date=date,
            channel_id=c.id,
            user_id=u.id,
            reply_count=len(thread.replies),
        )


class SortedThreadsAction(RethreadAction):
    def before_threads(self, threads):
        pass

    def after_threads(self, threads):
        pass

    def handle_threads(self, threads):
        self.before_threads(threads)

        for thread in threads:
            self.handle_thread(thread)

        self.after_threads(threads)

    def handle_thread(self, thread):
        pass

    def after_all(self):
        super().after_all()
        threads = self.get_sorted_messages_by_ts()
        self.handle_threads(threads)


class ThreadsToMarkdownAction(SortedThreadsAction):
    def handle_thread(self, thread):
        print(thread.to_mdom(self.ctx).to_md())
        print("-" * 50)


class ThreadsToHTMLAction(SortedThreadsAction):
    def before_threads(self, threads):
        print("<!doctype html>")
        print('<html><head><meta charset="utf-8"></head><body>')

    def after_threads(self, threads):
        print("</body></html>")

    def handle_thread(self, thread):
        print(thread.to_mdom(self.ctx).to_html_str())


class ThreadsToTextAction(SortedThreadsAction):
    def handle_thread(self, thread):
        print(thread.to_mdom(self.ctx).to_text().strip())
        print("-" * 50)


@dataclass
class LinkInfo:
    link: RichTextSectionElementLink
    count: int
    refs: list[str]


class LinkCollector:
    def __init__(self):
        self.link_info = {}

    def handle_thread(self, thread):
        for link in thread.get_links():
            url = link.url
            info = self.link_info.get(url)
            if info:
                info.count += 1
                info.refs.append(thread.message.ts)
            else:
                self.link_info[url] = LinkInfo(link, 1, [thread.message.ts])

    def get_links_sorted_by_count(self, reverse=False):
        return sorted(
            self.link_info.values(), key=lambda info: info.count, reverse=reverse
        )

    def get_links_sorted_by_url(self):
        return sorted(self.link_info.values(), key=lambda info: info.link.url)


class ThreadsToLinksAction(SortedThreadsAction):
    def __init__(self):
        super().__init__()
        self.link_info = LinkCollector()

    def handle_thread(self, thread):
        self.link_info.handle_thread(thread)

    def after_threads(self, threads):
        links = self.link_info.get_links_sorted_by_count(reverse=True)
        for info in links:
            print(info.link.url, info.count, info.refs)


ACTIONS = {
    "parse": ParseAction,
    "html": ToHTMLAction,
    "md": ToMarkdownAction,
    "txt": ToTextAction,
    "threads-to-html": ThreadsToHTMLAction,
    "threads-to-md": ThreadsToMarkdownAction,
    "threads-to-txt": ThreadsToTextAction,
    "threads-to-links": ThreadsToLinksAction,
    "emojistats": ToEmojiStatsAction,
    "linkstats": ToLinkStatsAction,
    "rethread": RethreadAction,
    "to-sqlite": ToSQLite,
}


def parse_args():
    parser = argparse.ArgumentParser(description="Process slack archive files.")
    parser.add_argument("action", help="Action to do on each entry")
    parser.add_argument(
        "dir_tree_type",
        help="Type of directory tree",
        choices=["archive", "foc-history"],
    )
    parser.add_argument("base_path", help="Root path for glob pattern")

    return parser.parse_args()


ARCHIVE_TYPES = {
    "archive": (archive_channel_extractor, "*/*.json"),
    "foc-history": (foc_history_channel_extractor, "*/*/*/*.json"),
}


def main():
    args = parse_args()
    action = ACTIONS[args.action]()

    channel_extractor, glob_pattern = ARCHIVE_TYPES[args.dir_tree_type]
    action.channel_extractor = channel_extractor

    ctx = walk_archive(args.base_path, glob_pattern, action)
    ctx.print_messages()


if __name__ == "__main__":
    main()
