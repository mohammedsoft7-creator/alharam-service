from html.parser import HTMLParser
from pathlib import Path


class ApplicationMarkup(HTMLParser):
    void = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link',
            'meta', 'param', 'source', 'track', 'wbr'}

    def __init__(self):
        super().__init__()
        self.stack = []
        self.ids = set()

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        identifier = attrs.get('id')
        if identifier:
            assert identifier not in self.ids, f'Duplicate ID: {identifier}'
            self.ids.add(identifier)
        if identifier == 'content':
            assert self.stack[-1][1] == 'main', 'Content must be outside the sticky header'
        if 'page' in attrs.get('class', '').split():
            assert self.stack[-1][1] == 'content', f'Page is nested incorrectly: {identifier}'
        if 'modal-overlay' in attrs.get('class', '').split():
            assert self.stack[-1][0] == 'body', 'Dialogs must be outside the page layout'
        if tag not in self.void:
            self.stack.append((tag, identifier))

    def handle_endtag(self, tag):
        assert self.stack and self.stack[-1][0] == tag, f'Unbalanced {tag} at {self.getpos()}'
        self.stack.pop()


root = Path(__file__).resolve().parent.parent
parser = ApplicationMarkup()
parser.feed((root / 'index.html').read_text(encoding='utf-8'))
assert not parser.stack, f'Unclosed tags: {parser.stack}'
print(f'HTML verified: balanced tags, {len(parser.ids)} unique IDs, correct pages and dialog parents.')
