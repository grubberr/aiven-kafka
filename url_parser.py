#!/usr/bin/env python3

import os
import re
import logging
import yaml
import jsonschema

logger = logging.getLogger(os.path.basename(__file__))

json_schema = {
    'type': 'object',
    'properties': {
        'url': {'type': 'string'},
        'delay': {'type': 'integer'},
        'regex': {'type': 'string'},
    },
    'required': ['url'],
    'additionalProperties': True
}


def validate(item: dict):
    " validate and transform url item "

    try:
        jsonschema.validate(item, json_schema)
    except jsonschema.exceptions.ValidationError as e:
        logger.exception(e)
        return

    if 'regex' in item:

        try:
            item['regex'] = re.compile(item['regex'])
        except re.error as e:
            logger.exception(e)
            return

    return item

def url_parser(filename):
    " generate url items for scheduler "

    try:
        fp = open(filename)
    except FileNotFoundError as e:
        logger.exception(e)
        return

    try:
        data = yaml.safe_load(fp)
    except yaml.error.YAMLError as e:
        logger.exception(e)
        return

    if not isinstance(data, list):
        logger.error('yaml parsing error: not list')
        return

    for item in data:
        if isinstance(item, str):
            yield {'url': item}
        elif isinstance(item, dict):
            item = validate(item)
            if item:
                yield item
