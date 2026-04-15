#!/usr/bin/env python3
from pathlib import Path
import argparse
import re
import requests
from bs4 import BeautifulSoup
import html2text


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Download Zig language reference as plain text.')
    parser.add_argument('version', nargs='?', default='master', help='Zig documentation version slug, e.g. master or 0.15.2')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    version = args.version.strip('/')
    url = f'https://ziglang.org/documentation/{version}/'
    out = Path(f'docs/zig-{version}-language-reference.txt')

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    main = soup.find(id='contents')
    if main is None:
        raise SystemExit('Could not find #contents in Zig language reference HTML')

    converter = html2text.HTML2Text()
    converter.body_width = 0
    converter.ignore_images = True
    converter.ignore_emphasis = False
    converter.inline_links = True
    converter.protect_links = True

    text = converter.handle(str(main))
    text = text.replace('Â§', '§')
    text = re.sub(r' +\n', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text).strip() + '\n'

    header = (
        f'Zig Language Reference {version}\n'
        f'Source: {url}\n\n'
    )

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(header + text, encoding='utf-8')
    print(f'wrote {out}')
    print(f'lines: {(header + text).count(chr(10))}')
    print(f'bytes: {out.stat().st_size}')


if __name__ == '__main__':
    main()
