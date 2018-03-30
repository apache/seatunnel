# encoding: utf-8

import smtplib
from email.mime.text import MIMEText
from email.header import Header

from alert_util import match_alert


class Emails(object):

    def __init__(self):
        self.name = 'emails'

    @staticmethod
    def send_alert(config, level, subject, objects, content):
        config = config['emails']
        sender = config['sender']
        receivers = config['receivers']
        smtpserver = config['smtp_server']
        username = config['auth_username']
        password = config['auth_password']

        if match_alert(config['routes'], level):

            message = MIMEText(content, 'text', 'utf-8')
            message['Subject'] = Header(subject + objects, 'utf-8')

            smtp = smtplib.SMTP()
            smtp.connect(smtpserver)
            smtp.login(username, password)
            smtp.sendmail(sender, receivers, message.as_string())

    @staticmethod
    def check_config(config):
        config = config['emails']
        arg_list = ['sender', 'receivers', 'smtp_server', 'auth_username',
                    'auth_password', 'routes']

        for arg in arg_list:
            if arg not in config:
                return False

        return True
