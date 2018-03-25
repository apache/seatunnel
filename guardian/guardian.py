# coding:utf-8

import json
import sys
import subprocess
import time
from datetime import datetime

from flask_apscheduler import APScheduler

import requests

import config_api
from alert import GuardianAlert, AlertException
# TODO:
# from contacts import contacts


import spark_checker

# TODO:
# start application concurrently


def _log_debug(msg):
    _logging('DEBUG', msg)


def _log_info(msg):
    _logging('INFO', msg)


def _log_error(msg):
    _logging('ERROR', msg)


def _logging(log_level, msg):
    print datetime.now().strftime('%Y-%m-%d %H:%M:%S'), "[{level}]".format(level=log_level), msg.encode('utf-8')
    sys.stdout.flush()  # flush stdout buffer.


def set_config_default(config):
    if 'node_name' not in config:
        config['node_name'] = u'my_guardian'


def get_args_check(args):

    f = open(args, 'r')
    try:
        config = json.load(f)
    except ValueError as e:
        _log_error(repr(e))
        raise ValueError('Config file is not a valid json')

    set_config_default(config)

    config = _get_active_app_config(config)

    return config


def command_check(args):

    _log_info("Starting to check applications")

    while True:

        config = get_args_check(args)
        oi_alert = GuardianAlert(config["alert_manager"])
        check_impl(config, oi_alert)
        time.sleep(config['check_interval'])


class GuardianError(Exception):
    pass


class NoAvailableYarnRM(GuardianError):
    pass


class NoActiveYarnRM(GuardianError):
    pass


class CannotGetClusterApps(GuardianError):
    pass


def _get_active_app_config(config):
    apps = config['apps']
    del_list = []
    for i in range(len(apps)):
        app = apps[i]
        if 'active' in app.keys() and app['active'] is False:
            del_list.insert(0, i)

    for i in del_list:
        del apps[i]

    return config


def _get_yarn_active_rm(hosts, timeout=10):
    """Find active yarn resource manager.
    """

    active_rm = None
    available_hosts = len(hosts)
    for host in hosts:
        url = 'http://{host}/ws/v1/cluster/info'.format(host=host)
        try:
            resp = requests.get(url, timeout=timeout)
        except requests.exceptions.ConnectTimeout as e:
            available_hosts -= 1
            continue

        if resp.status_code != 200:
            available_hosts -= 1
            continue

        cluster_info = resp.json()

        if cluster_info['clusterInfo']['haState'].lower() == "active":
            active_rm = host
            break

    if available_hosts == 0:
        raise NoAvailableYarnRM

    if active_rm is None:
        raise NoActiveYarnRM

    _log_debug('Picked up Yarn active resource manager:' + active_rm)

    return active_rm


def _request_yarn(hosts, timeout=10):

    active_rm = _get_yarn_active_rm(hosts)

    url = 'http://{host}/ws/v1/cluster/apps?states=accepted,running'.format(host=active_rm)
    resp = requests.get(url, timeout=timeout)
    if resp.status_code != 200:
        raise CannotGetClusterApps()

    stats = resp.json()

    if len(stats.keys()) == 0:
        raise ValueError('Cannot not get yarn application stats')

    return stats, active_rm


def check_impl(args, oi_alert):

    yarn_active_rm = None
    retry = 0
    while retry < 3:
        try:
            j, yarn_active_rm = _request_yarn(args['yarn']['api_hosts'])
            break

        except (ValueError, NoAvailableYarnRM, NoActiveYarnRM):

            retry += 1

    if retry >= 3:
        _log_error("Failed to send request to yarn resource manager, host config:" +
                   ', '.join(args['yarn']['api_hosts']))
        subject = 'Guardian'
        objects = 'Yarn RM'
        content = 'Failed to send request to yarn resource manager.'
        try:
            oi_alert.send_alert("ERROR", subject, objects, content)
        except AlertException as e:
            _log_error('failed to send alert, caught exception: ' + repr(e))

        return

    running_apps = j['apps']['app']

    app_map = {}
    for app in running_apps:
        key = app['name']
        if key not in app_map:
            app_map[key] = []

        app_map[key].append(app)

    not_running_apps = []
    for app_config in args['apps']:
        app_name = app_config['app_name']

        apps = None
        try:
            apps = app_map[app_name]
        except KeyError:
            apps = []

        actual_app_num = len(apps)
        expected_app_num = 1
        try:
            expected_app_num = app_config['app_num']
        except KeyError:
            pass

        if actual_app_num < expected_app_num:
            not_running_apps.append(app_name)
            continue

        # app is running but not in expected number
        if actual_app_num > expected_app_num:
            subject = 'Guardian'
            objects = app_name
            content = 'Unexpected running app number, expected/actual: {expected}/{actual}'.format(expected=app_config['app_num'], actual=len(apps))
            try:
                oi_alert.send_alert("ERROR", subject, objects, content)
            except AlertException as e:
                _log_error('failed to send alert, caught exception: ' + repr(e))

            continue

        # specific type of checker has been set
        if 'check_type' in app_config and 'check_options' in app_config:

            config = {
                'app': app_config,
                'yarn': {
                    'active_rm': yarn_active_rm
                },
                'node_name': args['node_name'],
            }

            if app_config['check_type'] == 'spark':
                spark_checker.check(apps, config, oi_alert)

    if len(not_running_apps) == 0:
        _log_info("There is no application need to be started.")
        return

    alert_not_running_apps(not_running_apps, args['apps'], oi_alert)


def alert_not_running_apps(app_names, app_configs, oi_alert):

    for app_name in app_names:

        subject = 'Guardian'
        objects = app_name
        content = 'App is not running or less than expected number of running instance, will restart.'
        try:
            oi_alert.send_alert("ERROR", subject, objects, content)
        except AlertException as e:
            _log_error('failed to send alert, caught exception: ' + repr(e))

        app_info = filter(lambda x: x['app_name'] == app_name, app_configs)
        raw_cmd = app_info[0]['start_cmd']
        cmd = raw_cmd.split()  # split by whitespace to comand and arguments for popen

        retry = 0
        while retry < 3:
            try:
                p = subprocess.Popen(cmd)
            except OSError:
                # probably os cannot find the start command.
                _log_error("Invalid start command: " + raw_cmd)
                retry += 1
                continue

            output, err = p.communicate()

            if err is not None:
                print err
                retry += 1
                continue
            else:
                print output
                break

        if retry >= 3:

            _log_info("Alert sms after failed 3 times.")
            subject = 'Guardian'
            objects = app_name
            content = 'Failed to start yarn app after 3 times.'
            try:
                oi_alert.send_alert("ERROR", subject, objects, content)
            except AlertException as e:
                _log_error('failed to send alert, caught exception: ' + repr(e))

    _log_info("Finished checking applications")


def get_args_inspect(args):
    """
    args:
        filter: only support "app_name"
        value: only support regular expression
    """

    if len(args) != 3:
        raise ValueError("Invalid argument number")

    f = open(args[0], 'r')
    try:
        config = json.load(f)
    except ValueError as e:
        _log_error(repr(e))
        raise ValueError('Config file is not a valid json')

    if args[1] != 'app_name':
        raise ValueError("Invalid Filter, only support \"app_name\"")

    args_map = {
        'config': config,
        'filter': args[1],
        'value': args[2],
    }

    return args_map


def command_inspect(args):

    _log_info("Starting to inspect applications")

    import re

    def match(s):

        pattern = args['value']
        m = re.search(pattern, s)
        return True if m is not None else False

    j, active_rm = _request_yarn(args['config']['yarn']['api_hosts'])

    running_apps = j['apps']['app']

    running_app_names = map(lambda x: x['name'], running_apps)
    running_app_names = filter(match, running_app_names)
    running_app_names = set(running_app_names)

    config = args['config']

    configured_apps = map(lambda x: x['app_name'], config['apps'])
    configured_apps = set(configured_apps)

    for app_name in running_app_names - configured_apps:
        config['apps'].append({
            'app_name': app_name,
            'start_cmd': 'TODO',
            'app_num': 1
        })

    print json.dumps(config, indent=4)

    _log_info("Finished inspecting applications, please check config.")


if __name__ == '__main__':

    import ntpath

    executable = ntpath.basename(sys.argv[0])
    if len(sys.argv[1:]) < 1:
        print "usage:", executable, "<command> <command_args>"
        print "  commands:"
        print "    - check <config_file>"
        print "      example:", executable, "check ./config.json"
        print ""
        print "    - inspect <config_file> <filter> <value>"
        print "          * filter: app_name"
        print "          * value: any regular expression"
        print "      example:", executable, "inspect ./config.json app_name waterdrop_"
        print ""
        sys.exit(-1)

    command = sys.argv[1]

    try:

        if command == 'check':
            if len(sys.argv[2:]) != 1:
                raise ValueError('Invalid argument number')

            class CheckConfig(object):
                JOBS = [
                    {
                        'id': 'job1',
                        'func': 'guardian:command_check',
                        'args': [sys.argv[2]]
                    }
                ]

            # running with flask
            app = config_api.app
            app.config['config_name'] = sys.argv[2]
            app.config.from_object(CheckConfig())
            scheduler = APScheduler()
            scheduler.init_app(app)
            scheduler.start()
            app.run()

        elif command == 'inspect':
            config = get_args_inspect(sys.argv[2:])
            command_inspect(config)

        else:
            raise ValueError("Unsupported Command:" + command)

    except KeyboardInterrupt as e:
        _log_info("Exiting. Bye")
        sys.exit(0)
