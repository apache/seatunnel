# encoding: utf-8

from send_alert import alert_manager


class GuardianAlert(object):

    def __init__(self, alert_config):
        self.alert_config = alert_config
        self.alerts = self.create_alert()
        self.check_config()

    def create_alert(self):
        alerts = []
        for method in self.alert_config:
            alerts.append(getattr(alert_manager, method)())

        return alerts

    def send_alert(self, level, subject, objects, content):
        for alert in self.alerts:
            alert.send_alert(self.alert_config, level, subject, objects, content)

    def check_config(self):
        for alert_impl in self.alerts:
            if not alert_impl.check_config(self.alert_config):
                raise IncorrectConfig("Incorrect Config: " + alert_impl.name)


class AlertException(Exception):
    pass


class UnsupportedAlertMethod(AlertException):
    pass


class IncorrectConfig(AlertException):
    pass
