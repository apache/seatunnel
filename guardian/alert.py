# encoding: utf-8

class GuardianAlert(object):
    pass

class DefaultAlertImpl(GuardianAlert):

    def internal(self):
        pass

    def fatal(self):
        pass

    def error(self):
        pass

    def warning(self):
        pass