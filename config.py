import json
import logging
import os

from tornado.options import define, options


def load_setting(filename):
    try:
        with open(filename) as js:
            settings = json.load(js)
    except FileNotFoundError:
        settings = None

    for key in settings:
        if isinstance(settings[key], dict):
            for subkey in settings[key]:
                define(f'{key}_{subkey}', default=settings[key][subkey])
        else:
            define(key, default=settings[key])

    if options['debug_mode']:
        options.logging_level = logging.DEBUG


define('base', default=os.path.dirname(__file__))
define('exchange_file', default='rates.json')
define('address_key', default='dc0bc747-273e-4281-9d0c-6308af027108')
define('logging_level', default=logging.INFO)
define('logging_format', default="%(asctime)s %(message)s")
