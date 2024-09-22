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

export interface Monitor {
  processors: string
  'physical.memory.total': string
  'physical.memory.free': string
  'swap.space.total': string
  'swap.space.free': string
  'heap.memory.used': string
  'heap.memory.free': string
  'heap.memory.total': string
  'heap.memory.max': string
  'heap.memory.used/total': string
  'heap.memory.used/max': string
  'minor.gc.count': string
  'minor.gc.time': string
  'major.gc.count': string
  'major.gc.time': string
  'load.process': string
  'load.system': string
  'load.systemAverage': string
  'thread.count': string
  'thread.peakCount': string
  'cluster.timeDiff': string
  'event.q.size': string
  'executor.q.async.size': string
  'executor.q.client.size': string
  'executor.q.client.query.size': string
  'executor.q.client.blocking.size': string
  'executor.q.query.size': string
  'executor.q.scheduled.size': string
  'executor.q.io.size': string
  'executor.q.system.size': string
  'executor.q.operations.size': string
  'executor.q.priorityOperation.size': string
  'operations.completed.count': string
  'executor.q.mapLoad.size': string
  'executor.q.mapLoadAllKeys.size': string
  'executor.q.cluster.size': string
  'executor.q.response.size': string
  'operations.running.count': string
  'operations.pending.invocations.percentage': string
  'operations.pending.invocations.count': string
  'proxy.count': string
  'clientEndpoint.count': string
  'connection.active.count': string
  'client.connection.count': string
  'connection.count': string
}
