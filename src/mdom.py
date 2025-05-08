from xml.etree import ElementTree as ET

from markdownify import markdownify


def node(tag, childs, attrib=None):
    if attrib is None:
        attrib = {}

    n = ET.Element(tag, attrib)
    if isinstance(childs, str):
        n.text = childs
    elif isinstance(childs, Node):
        n.append(childs)
    elif isinstance(childs, list):
        for child in childs:
            n.append(child)

    return n


def node_to_str(n):
    return ET.tostring(n, encoding="utf-8", method="html")


class Node:
    def to_html(self):
        return None

    def to_html_str(self):
        return node_to_str(self.to_html())

    def to_md(self):
        return markdownify(self.to_html_str())


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


class Section(Node):
    def __init__(self, type_: str, childs):
        self.type = type_
        self.childs = childs

    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("section", childs, {"class": self.type})


class Block(Node):
    def __init__(self, type_: str, childs):
        self.type = type_
        self.childs = childs

    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("div", childs, {"class": self.type})

class Paragraph(Block):
    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("p", childs, {"class": self.type})

class Preformatted(Block):
    def __init__(self, childs):
        super().__init__("preformatted", childs)

    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("pre", childs, {})


class Quote(Block):
    def __init__(self, childs):
        super().__init__("quote", childs)

    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        return node("blockquote", childs, {})


class Group(Block):
    def to_html(self):
        childs = [child.to_html() for child in self.childs]
        # interleave spaces between elements
        for i in range(1, len(childs), 2):
            childs.insert(i, node("span", " "))

        return node("div", childs, {"class": self.type})


class List(Node):
    def __init__(self, childs):
        self.childs = childs

    def to_html(self):
        childs = [node("li", child.to_html()) for child in self.childs]
        return node("ul", childs)


class OrderedList(List):
    def to_html(self):
        childs = [node("li", child.to_html()) for child in self.childs]
        return node("ol", childs)


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
