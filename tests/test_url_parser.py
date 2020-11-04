#!/usr/bin/env python3

import re
import yaml
from url_parser import url_parser


def test_url_parser(tmpdir):

    URLS_FILE = tmpdir / 'urls.yaml'

    urls_data = [
        'http://python.org',
        'https://www.google.com',
        {
            'url': 'https://www.instagram.com'
        },
        {
            'url': 'https://aiven.io',
            'regex': r'data\s+pipelines'
        },
        {
            'non-url': 'http://notfound.com'
        },
        {
            'url': 'https://twitter.com',
            'delay': 3600
        }
    ]

    URLS_FILE.write(yaml.dump(urls_data))

    items = list(url_parser(URLS_FILE))
    assert items[0] == {'url': 'http://python.org'}
    assert items[1] == {'url': 'https://www.google.com'}
    assert items[2] == {'url': 'https://www.instagram.com'}
    assert items[3] == {'url': 'https://aiven.io', 'regex': re.compile(r'data\s+pipelines')}
    assert items[4] == {'url': 'https://twitter.com', 'delay': 3600}
    assert len(items) == 5


def test_url_parser_file_not_found(tmpdir):

    URLS_FILE = tmpdir / 'urls.yaml'
    items = list(url_parser(URLS_FILE))
    assert len(items) == 0


def test_url_parser_yaml_error(tmpdir):

    URLS_FILE = tmpdir / 'urls.yaml'
    URLS_FILE.write('}{')
    items = list(url_parser(URLS_FILE))
    assert len(items) == 0


def test_url_parser_yaml_not_list(tmpdir):

    URLS_FILE = tmpdir / 'urls.yaml'
    URLS_FILE.write('k: v')
    items = list(url_parser(URLS_FILE))
    assert len(items) == 0
