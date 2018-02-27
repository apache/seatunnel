# encoding: utf-8

import smtplib
from email.mime.text import MIMEText
from email.header import Header

import json
import httplib
from urlparse import urlparse


def _match_alert(routes, level):
    if level in routes['match']['level']:
        return True
    else:
        return False


class GuardianAlert(object):
    def __init__(self, alert_config):
        self.alert_config = alert_config
        self.check_config()

    def send_alert(self, level, subject, objects, content):
        for method in self.alert_config:
            if method == 'email':
                send_mail_alert(self.alert_config[method], level, subject, objects, content)
            elif method == 'webhook':
                send_webhook_alert(self.alert_config[method], level, subject, objects, content)
            else:
                raise UnsupportedAlertMethod()

    def check_config(self):
        pass


def send_mail_alert(config, level, subject, objects, content):
    sender = config['sender']
    receivers = config['receivers']
    smtpserver = config['smtp_server']
    username = config['auth_username']
    password = config['auth_password']

    if _match_alert(config['routes'], level):

        message = MIMEText(content, 'text', 'utf-8')
        message['Subject'] = Header(subject + objects, 'utf-8')

        smtp = smtplib.SMTP()
        smtp.connect(smtpserver)
        smtp.login(username, password)
        smtp.sendmail(sender, receivers, message.as_string())


def send_webhook_alert(config, level, subject, objects, content):
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


class AlertException(Exception):
    pass


class UnsupportedAlertMethod(AlertException):
    pass
