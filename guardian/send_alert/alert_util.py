# encoding: utf-8


def match_alert(routes, level):
    if level in routes['match']['level']:
        return True
    else:
        return False
