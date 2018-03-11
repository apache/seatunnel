# encoding: utf-8

import json
import httplib
from urlparse import urlparse

from alert_util import match_alert


class Webhook(object):

    def __init__(self):
        self.name = "webhook"

    @staticmethod
    def send_alert(config, level, subject, objects, content):
        config = config['webhook']
        url = config['url']
        params = {
            'subject': subject,
            'objects': objects,
            'content': content
        }

        headers = {
            'content-type': 'application/json;charset=UTF-8',
            'Accept': 'text/plain'
        }

        if match_alert(config['routes'], level):
            url_info = urlparse(url)

            port = 80 if url_info.port is None else url_info.port
            http_client = httplib.HTTPConnection(url_info.hostname, port, timeout=5)

            http_client.request("POST", url_info.path, json.dumps(params), headers)

    @staticmethod
    def check_config(config):
        config = config['webhook']
        if 'url' not in config or 'routes' not in config:
            return False
        else:
            return True
