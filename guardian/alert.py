# encoding: utf-8

import send_alert


class GuardianAlert(object):

    def __init__(self, alert_config):
        self.alert_config = alert_config
        self.alerts = self.create_alert()
        self.check_config()

    def create_alert(self):
        alerts = []
        for method in self.alert_config:
            alerts.append(getattr(send_alert, method))

        return alerts

    def send_alert(self, level, subject, objects, content):
        for alert in self.alerts:
            alert.send_alert(level, subject, objects, content)

    def check_config(self):
        for alert in self.alerts:
            alert.check_confgi()


class AlertException(Exception):
    pass


class UnsupportedAlertMethod(AlertException):
    pass
