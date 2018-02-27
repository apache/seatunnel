# coding:utf-8

import sys
from datetime import datetime
import streaming_utils

def _log_debug(msg):
        _logging('DEBUG', msg)

def _log_info(msg):
    _logging('INFO', msg)

def _log_error(msg):
    _logging('ERROR', msg)

def _logging(log_level, msg):
    print datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "[{level}]".format(level=log_level), msg.encode('utf-8')
    sys.stdout.flush() # flush stdout buffer.

def check(apps, config, alert_client):

    for app in apps:
        _log_debug("check spark streaming:" + repr(app))

        _check_impl(app, config, alert_client)


def _alert_invalid_config(alert_client):
    subject = 'Guardian'
    objects = '配置'
    content = '监控配置错误'

    alert_client.send_alert('INTERNAL', subject, objects, content)


def _check_impl(app, config, alert_client):

    app_config = config['app']
    if 'check_options' not in app_config or not isinstance(app_config['check_options'], dict) or len(app_config['check_options']) == 0:
        return

    check_options = app_config['check_options']
    if 'alert_level' not in check_options or ('max_delayed_batch_num' not in check_options and 'max_delayed_time' not in check_options):
        _alert_invalid_config(alert_client)
        return

    active_rm = config['yarn']['active_rm']

    try:
        batch_stats = streaming_utils.streaming_batch_stats(active_rm, app['id'], status='RUNNING')
        delayed_batch_number = streaming_utils.streaming_batch_delay(batch_stats)
        seconds_delayed = streaming_utils.streaming_time_delay(batch_stats)
        log_content = 'checked spark streaming, appname: %s, delayed_batch_number: %d, seconds_delayed: %d' % (app['name'], delayed_batch_number, seconds_delayed)
        _log_debug(log_content)
    except streaming_utils.StreamingUtilsError as e:
        _log_error("failed to get streaming batch delay stats, caught exception" + repr(e))
        subject = 'Guardian'
        objects = app['name']
        content = "无法获取到流式处理延迟统计"
        alert_client.send_alert('FATAL', subject, objects, content)
        return

    content = ""
    send_alert = False
    if 'max_delayed_batch_num' in check_options and delayed_batch_number > int(check_options['max_delayed_batch_num']):
        send_alert = True
        content = "流式处理延迟batch个数[%d], 超过阈值[%d], " % (delayed_batch_number, int(check_options['max_delayed_batch_num']))

    if 'max_delayed_time' in check_options and seconds_delayed > int(check_options['max_delayed_time']):
        send_alert = True
        content += ", 流式处理延迟时间[%d]秒, 超过阈值[%d]秒" % (seconds_delayed, int(check_options['max_delayed_time']))

    if send_alert:
        subject = 'Guardian'
        objects = app['name']
        if check_options['alert_level'] ==  'FATAL':
            alert_client.send_alert('FATAL', subject, objects, content)
        elif check_options['alert_level'] == 'ERROR':
            alert_client.send_alert('ERROR', subject, objects, content)
        elif check_options['alert_level'] == 'WARNING':
            alert_client.send_alert('WARNING', subject, objects, content)
