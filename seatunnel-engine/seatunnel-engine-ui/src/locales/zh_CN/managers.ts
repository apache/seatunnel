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
  managers: '节点管理',
  address: '地址',
  cpu: '进程 CPU',
  heap: '堆内存已用 / 上限',
  physical: '物理内存',
  gc: 'GC 次数（Minor / Major）',
  threads: '线程数',
  slots: '槽位',
  details: '详情',
  refresh: '刷新',
  refresh_hint: '上次请求完成后每 30 秒刷新一次。',
  monitor_unavailable: '系统监控暂不可用，已清除之前的数值。',
  resource_unavailable: 'Worker 资源暂不可用，并不代表集群没有 Worker。',
  snapshot_hint:
    '系统监控与 Worker 资源为独立快照。资源值来自最近一次心跳；响应时间不表示心跳新鲜度。',
  collected_at: '资源响应时间',
  dynamic_used: '动态 — 已用 {used}（无固定容量）',
  fixed_slots: '已用 {used} / {total}，空闲 {free}',
  cpu_resources: 'CPU 核心（可用 / 总计）',
  heap_resources: '堆内存资源（可用 / 总计）',
  cpu_usage: '心跳 CPU 使用率',
  memory_usage: '心跳内存使用率',
  running_jobs: '运行中作业数',
  tags: '标签',
  resources: 'Worker 资源',
  monitoring: '系统监控',
  resource_missing: '该 Worker 暂无资源快照。',
  monitor_missing: '该 Worker 暂无系统监控采样。'
}
