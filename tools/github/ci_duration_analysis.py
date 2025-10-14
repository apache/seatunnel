#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# !/usr/bin/python
import json
import math
import sys
from datetime import datetime
import os
import requests
from tabulate import tabulate


def fetch_jobs_from_api(api_url, token=None):
    try:
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()
        return parse_jobs_from_data(response.json())
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"API request failed: {str(e)}")


def _parse_single_job(job):
    job_info = {
        'job_id': job.get('id'),
        'name': job.get('name'),
        'status': job.get('status'),
        'conclusion': job.get('conclusion'),
    }

    # Calculate the total time consumed
    try:
        start_time = datetime.fromisoformat(job['started_at'].replace('Z', '+00:00'))
        end_time = datetime.fromisoformat(job['completed_at'].replace('Z', '+00:00'))
        duration_seconds = (end_time - start_time).total_seconds()
        job_info['started_at'] = start_time.strftime("%Y-%m-%d %H:%M:%S")
        job_info['total_duration_seconds'] = round(duration_seconds, 2)
        job_info['total_duration_minutes'] = round(duration_seconds / 60, 2)
    except (KeyError, ValueError) as e:
        job_info['total_duration_seconds'] = None
        job_info['total_duration_minutes'] = None
        print(f"Warning: Failed to parse the time of task {job.get('id')} - {str(e)}")

    # Parsing step information
    job_info['steps'] = []
    for step in job.get('steps', []):
        try:
            step_start = datetime.fromisoformat(step['started_at'].replace('Z', '+00:00'))
            step_end = datetime.fromisoformat(step['completed_at'].replace('Z', '+00:00'))
            step_duration = (step_end - step_start).total_seconds()

            job_info['steps'].append({
                'name': step.get('name'),
                'number': step.get('number'),
                'status': step.get('status'),
                'conclusion': step.get('conclusion'),
                'duration_seconds': round(step_duration, 2)
            })
        except (KeyError, ValueError) as e:
            print(f"Warning: Failed to parse time for step {step.get('name')} - {str(e)}")

    return job_info


def _parse_jobs_batch(data):
    if 'jobs' not in data:
        raise KeyError("The data does not contain the 'jobs' field.")
    jobs = data['jobs']
    jobs.sort(
        key=lambda x: datetime.fromisoformat(x["started_at"].replace("Z", "+00:00"))
    )
    return [_parse_single_job(job) for job in jobs]


def parse_jobs_from_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not exist: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing error: {str(e)}")

    return _parse_jobs_batch(data)


def parse_jobs_from_data(data):
    return _parse_jobs_batch(data)


def print_job_summary(parsed_jobs):
    success_jobs = [job for job in parsed_jobs if job['conclusion'] == 'success']
    print(f"A total of {len(parsed_jobs)} tasks were parsed, among which {len(success_jobs)} tasks were successful (success)\n")

    job_table_data = []
    headers = ["Sequence Number", "Job Name", "Job ID", "CONCLUSION", "STARTED_AT",
               "Total Duration (Seconds)", "Total Duration (Minutes)", "Expected"]

    for i, job in enumerate(success_jobs, 1):
        remainder = job['total_duration_minutes'] % 20
        cost = (math.ceil(job['total_duration_minutes'] / 20) + 1) * 20 if remainder >= 10 else \
            (math.ceil(job['total_duration_minutes'] / 20)) * 20
        job_table_data.append([
            i, job['name'], job['job_id'], job['conclusion'],job['started_at'],
            job['total_duration_seconds'], job['total_duration_minutes'], cost
        ])

    print("Overview of the Task:")
    print(tabulate(job_table_data, headers=headers, tablefmt="pipe"))
    print()

    for i, job in enumerate(success_jobs, 1):
        print(f"\nTask {i}: The 3 steps with the longest duration for {job['name']}:")
        if job['steps']:
            sorted_steps = sorted(job['steps'], key=lambda x: x['duration_seconds'], reverse=True)
            top_steps = sorted_steps[:3]
            step_table_data = [
                [s['name'], s['number'], s['status'], s['conclusion'], s['duration_seconds']]
                for s in top_steps
            ]
            print(tabulate(step_table_data, headers=["Step name", "No", "Status", "Conclusion", "Total Duration (seconds)"], tablefmt="pipe"))
        else:
            print("There is no step information for this task.")
        print("\n" + "-" * 80)


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("Usage: python ci_duration_analysis.py <owner> <run_id> [token]")
        print("Usage: <owner> is a required parameter, example: hawk9821")
        print("Usage: <run_id> is a required parameter, example: 18013073919")
        print("Usage: [token] is a non-mandatory parameter. Token acquisition method: GitHub -> Setting -> Developer Settings -> Personal access tokens -> Tokens (classic)")
        sys.exit(1)
    owner = sys.argv[1]
    run_id = sys.argv[2]
    api_url = f'https://api.github.com/repos/{owner}/seatunnel/actions/runs/{run_id}/jobs?page=1&per_page=100'
    token = sys.argv[3] if len(sys.argv) == 4 else None

    try:
        jobs_data = fetch_jobs_from_api(api_url, token)
        print_job_summary(jobs_data)

    except Exception as e:
        print(f"Processing failed: {str(e)}")