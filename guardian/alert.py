# encoding: utf-8

import smtplib
from email.mime.text import MIMEText
from email.header import Header

class GuardianAlert():
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

    message = MIMEText(content, 'text', 'utf-8')
    message['Subject'] = Header(subject + objects, 'utf-8')

    smtp = smtplib.SMTP()
    smtp.connect(smtpserver)
    smtp.login(username, password)
    smtp.sendmail(sender, receivers, message.as_string())


def send_webhook_alert(config, level, subject, objects, content):
    pass

class AlertException(Exception):
    pass

class UnsupportedAlertMethod(AlertException):
    pass
