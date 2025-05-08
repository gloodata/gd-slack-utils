from xml.etree import ElementTree as ET

from markdownify import markdownify


def node(tag, childs, attrib=None):
    if attrib is None:
        attrib = {}

    n = ET.Element(tag, attrib)
    if isinstance(childs, str):
        lines = childs.split("\n")
        if len(lines) > 1:
            for i, line in enumerate(lines):
                if i > 0:
                    n.append(node("br", []))

                n.append(node("span", line))
        else:
            n.text = childs
    elif isinstance(childs, list):
        for child in childs:
            n.append(child)
    else:
        n.append(childs)

    return n


def node_to_str(n):
    return ET.tostring(n, encoding="utf-8", method="html")


class Node:
    def to_html(self):
        return node("span", "")

    def to_html_str(self):
        return node_to_str(self.to_html())

    def to_md(self):
        return markdownify(self.to_html_str())

    def to_text(self) -> str:
        return "?"


class Ref(Node):
    def __init__(self, type_: str, target: str, text: str):
        self.type = type_
        self.target = target
        self.text = text

    def to_html(self):
        if self.type == "link":
            return node("a", self.text, {"href": self.target})

        return node(
            "a",
            self.text,
            {"class": self.type, "href": f"#{self.type}?key={self.target}"},
        )

    def to_text(self):
        if self.type == "user" or self.type == "bot":
            return f"@{self.text}"
        elif self.type == "nouser":
            return "@?"
        elif self.type == "channel":
            return f"#{self.text}"
        else:
            return f"[{self.text}]({self.target})"


class Section(Node):
    def __init__(self, type_: str, childs):
        self.type = type_
        self.childs = childs

    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("section", childs, {"class": self.type})

    def to_text(self):
        return "\n\n".join(child.to_text() for child in self.childs)


class Block(Node):
    def __init__(self, type_: str, childs):
        self.type = type_
        self.childs = childs

    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("div", childs, {"class": self.type})

    def to_text(self):
        return "\n\n".join(child.to_text() for child in self.childs)


class Paragraph(Block):
    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("p", childs, {"class": self.type})

    def to_text(self):
        return "\n\n".join(child.to_text() for child in self.childs)


class Preformatted(Block):
    def __init__(self, childs):
        super().__init__("preformatted", childs)

    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("pre", childs, {})

    def to_text(self):
        return "\n".join(child.to_text() for child in self.childs)


class Quote(Block):
    def __init__(self, childs):
        super().__init__("quote", childs)

    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("blockquote", childs, {})

    def to_text(self):
        return "\n".join(child.to_text() for child in self.childs)


class Group(Block):
    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        # interleave spaces between elements
        for i in range(1, len(childs), 2):
            childs.insert(i, node("span", " "))

        return node("div", childs, {"class": self.type})

    def to_text(self):
        return " ".join(child.to_text() for child in self.childs)


class List(Node):
    def __init__(self, childs):
        self.childs = childs

    def to_html(self):
        childs = [node("li", child.to_html()) for child in self.childs]
        return node("ul", childs)

    def to_text(self):
        return "\n* ".join(child.to_text() for child in self.childs)


class OrderedList(List):
    def to_html(self):
        childs = [node("li", child.to_html()) for child in self.childs]
        return node("ol", childs)

    def to_text(self):
        return "\n- ".join(child.to_text() for child in self.childs)


class Span(Node):
    def __init__(self, type_: str, text: str):
        self.type = type_
        self.text = text

    def to_html(self):
        if self.type == "isodate":
            text = self.text.replace("T", " ").split(".")[0]
            return node("time", text, {"datetime": self.text})

        attrs = {"class": self.type} if self.type else {}
        return node("span", self.text, attrs)

    def to_text(self):
        if self.type == "isodate":
            return self.text.replace("T", " ").split(".")[0]

        return self.text
