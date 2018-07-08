#!/usr/bin/env python

import datetime
import re
import pytz
import requests
from tzlocal import get_localzone

MAX_RETRY = 5

class StreamingUtilsError(Exception):
    pass

def streaming_batch_stats(master_url, application_id, status=None, timeout=20):

    stats_url = 'http://' + master_url + '/proxy/' + application_id +\
                '/api/v1/applications/' + application_id + '/jobs'

    if status is not None:
        stats_url += "?status=" + status

    retry = 0
    stats_json = None
    while retry < MAX_RETRY:
        retry += 1

        try:
            resp = requests.get(stats_url, timeout=timeout)
        except requests.exceptions.ConnectTimeout:
            continue
        except requests.exceptions.ReadTimeout:
            continue

        if resp.status_code != 200:
            continue

        try:
            stats_json = resp.json()
        except ValueError as e:
            stats_json = None

        break

    if stats_json is None:
        raise StreamingUtilsError("cannot get streaming batch utils from yarn")

    batch_regex = re.compile(r'.*id=(\d+).*batch time (\d\d:\d\d:\d\d).*')

    batch_stats = {}
    for job in stats_json:

        if not 'description' in job:
            continue # job needs a batch start time

        job_stats = {}

        description = job['description']
        matches = batch_regex.match(description).groups()
        batch_id = int(matches[0])
        batch_time = datetime.datetime.fromtimestamp(batch_id / 1000)
        batch_time = batch_time.replace(tzinfo=get_localzone())

        if not batch_id in batch_stats:
            batch_stats[batch_id] = {
                'batchStartTime': batch_time, # Scheduled Batch Start Time
                'status': job['status'],
                'jobs': [] # Job Details
            }

        if job['status'] == 'RUNNING':
            batch_stats[batch_id]['status'] = job['status']

        job_stats = {
            'status': job['status'],
        }
        if 'submissionTime' in job:
            job_stats['submissionTime'] = datetime.datetime.strptime(job['submissionTime'], '%Y-%m-%dT%H:%M:%S.%f%Z')
            job_stats['submissionTime'] = job_stats['submissionTime'].replace(tzinfo=pytz.UTC)
            # print job['submissionTime'], job_stats['submissionTime'].astimezone(get_localzone())

        if 'completionTime' in job:
            job_stats['completionTime'] = datetime.datetime.strptime(job['completionTime'], '%Y-%m-%dT%H:%M:%S.%f%Z')
            job_stats['completionTime'] = job_stats['completionTime'].replace(tzinfo=pytz.UTC)
            # print job['completionTime'], job_stats['completionTime'].astimezone(get_localzone())

        batch_stats[batch_id]['jobs'].append(job_stats)

    return batch_stats

def streaming_duration(batch_stats):
    """get spark streaming duration in seconds
    return None if there is only 0 or 1 batch, so duration cannot be calculated
    """
    if batch_stats is None:
        raise StreamingUtilsError("invalid batch stats")

    if len(batch_stats) < 2:
        return None

    batch_list = sorted(batch_stats.iteritems())
    return (batch_list[1][1]['batchStartTime'] - batch_list[0][1]['batchStartTime']).seconds

def streaming_batch_delay(batch_stats):
    """return number of batches delayed by now
    """
    if batch_stats is None:
        raise StreamingUtilsError("invalid batch stats")

    running_batches = streaming_running_batch(batch_stats)
    if len(running_batches) == 0:
        return 0

    now = datetime.datetime.now()
    now = now.replace(tzinfo=get_localzone())

    batch_duration = streaming_duration(batch_stats)
    if batch_duration is None:
        return 0

    running_batches = sorted(running_batches)

    delay = (now - running_batches[-1]['batchStartTime']).seconds / batch_duration
    return delay

def streaming_time_delay(batch_stats):
    """get time delay in seconds of running batch by now
    """
    if batch_stats is None:
        raise StreamingUtilsError("invalid batch stats")

    running_batches = streaming_running_batch(batch_stats)
    if len(running_batches) == 0:
        return 0

    batch_duration = streaming_duration(batch_stats)
    if batch_duration is None:
        return 0

    now = datetime.datetime.now()
    now = now.replace(tzinfo=get_localzone())

    if now - datetime.timedelta(seconds=batch_duration) <= running_batches[0]['batchStartTime']:
        return 0

    delay = now - datetime.timedelta(seconds=batch_duration) - running_batches[0]['batchStartTime']
    print "now:", now, "batch_duration:", batch_duration, "batchStartTime:", running_batches[0]['batchStartTime'], "delay:", delay
    return delay.seconds


def streaming_running_batch(batch_stats):
    """get running batch list of given batch stats, order by 'batchStartTime' ascending
    """
    running_batches = filter(lambda x: x['status'] == 'RUNNING', batch_stats.values())
    if len(running_batches) > 1:
        running_batches = sorted(running_batches)
    return running_batches

def main(master_url, application_id, status):
    batch_stats = streaming_batch_stats(master_url, application_id, status)
    batch_list = sorted(batch_stats.iteritems())

    batch_duration = streaming_duration(batch_stats)
    if batch_duration is None:
        print "Spark Streaming Batch Duration: too few batches to calculate duration",
    else:
        print "Spark Streaming Batch Duration:", batch_duration, "seconds"

    for item in batch_list:

        stats = item[1]
        jobs = stats['jobs']
        jobs = sorted(jobs, key=lambda x: x['submissionTime'])

        scheduling_delay = jobs[0]['submissionTime'] - stats['batchStartTime']

        processing_time = None
        if stats['status'] != 'RUNNING':
            processing_time = jobs[-1]['completionTime'] - jobs[0]['submissionTime']

        print 'Batch: %10s -- %20s -- %20s -- %20s' % (stats['status'], stats['batchStartTime'], processing_time, scheduling_delay)

    print "Pick up running batches: "
    running_batches = streaming_running_batch(batch_stats)
    for stats in running_batches:
        print 'Batch: %10s -- %20s -- %20s -- %20s' % (stats['status'], stats['batchStartTime'], processing_time, scheduling_delay, )

    print "~" * 30

    num_delayed_batches = streaming_batch_delay(batch_stats)
    time_delayed = streaming_time_delay(batch_stats)
    print "Detect batch scheduling delay,"
    print "\tNumber batch delay:", num_delayed_batches
    print "\tTime delayed:", time_delayed


"""
Get Spark Streaming microbatch statistics:
    - Batch start time
    - Scheduling delay (in seconds) for each microbatch
    - Processing time (in seconds) for each microbatch

Tested on Spark 2.0.0 running on YARN 2.7.2.

Time deltas are naive, do not run close to midnight (yet!).

Example usage:

    python get_spark_streaming_batch_statistics.py \
            --master ec2-52-40-144-150.us-west-2.compute.amazonaws.com \
            --applicationId application_1469205272660_0006

    Output (batch start time, processing time, scheduling delay):

        18:36:55 3.991 3783.837
        18:36:56 4.001 3786.832
        18:36:57 3.949 3789.862
        ...
"""
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--master', help='YARN ResourceManager URL', required=True)
    parser.add_argument('--applicationId', help='YARN application ID', required=True)
    parser.add_argument('--status', help='Spark Job Status[RUNNING, SUCCEEDED]', required=False)

    args = vars(parser.parse_args())

    master_url = args['master']
    application_id = args['applicationId']
    status = args['status']

    try:
        main(master_url, application_id, status)
    except StreamingUtilsError as e:
        print "Failure, caught exception:", repr(e)
    except KeyboardInterrupt as e:
        print "Exiting, bye !"

