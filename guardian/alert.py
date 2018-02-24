# encoding: utf-8

class GuardianAlert():
    def __init__(self, alert_config):
        self.alert_config = alert_config
        self.check_config()

    def send_alert(self, level, subject, objects, content):
        pass

    def check_config(self):
        pass

class AlertException(Exception):
    pass
