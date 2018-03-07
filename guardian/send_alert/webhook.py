# encoding: utf-8

import json
import httplib
from urlparse import urlparse

from alert_util import _match_alert

def send_alert(config, level, subject, objects, content):
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

    if _match_alert(config['routes'], level):
        url_info = urlparse(url)

        port = 80 if url_info.port is None else url_info.port
        http_client = httplib.HTTPConnection(url_info.hostname, port, timeout=5)

        http_client.request("POST", url_info.path, json.dumps(params), headers)
