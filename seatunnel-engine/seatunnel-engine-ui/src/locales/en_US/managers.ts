/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export default {
  managers: 'Managers',
  address: 'Address',
  cpu: 'Process CPU',
  heap: 'Heap used / max',
  physical: 'Physical memory',
  gc: 'GC count (minor / major)',
  threads: 'Threads',
  slots: 'Slots',
  details: 'Details',
  refresh: 'Refresh',
  refresh_hint: 'Refreshes every 30 seconds after the previous request completes.',
  monitor_unavailable: 'System monitoring is unavailable. Previous values have been cleared.',
  resource_unavailable:
    'Worker resources are unavailable. This does not mean the cluster has no workers.',
  snapshot_hint:
    'Monitoring and worker resources are independent snapshots. Resource values come from the latest heartbeat; response time is not heartbeat freshness.',
  collected_at: 'Resource response time',
  dynamic_used: 'Dynamic — {used} used (no fixed capacity)',
  fixed_slots: '{used} / {total} used, {free} free',
  cpu_resources: 'CPU cores (available / total)',
  heap_resources: 'Heap resources (available / total)',
  cpu_usage: 'Heartbeat CPU usage',
  memory_usage: 'Heartbeat memory usage',
  running_jobs: 'Running job count',
  tags: 'Tags',
  resources: 'Worker resources',
  monitoring: 'System monitoring',
  resource_missing: 'No resource snapshot is available for this worker.',
  monitor_missing: 'No system monitoring sample is available for this worker.'
}
