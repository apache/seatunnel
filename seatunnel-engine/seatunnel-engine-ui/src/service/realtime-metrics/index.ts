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

import { get } from '@/service/service'

export interface RealtimeEdgePoint {
  ts: number
  emitBlockedNs: number
  bpRatio: number
  queueSize: number
  queueCapacity: number
  queueFillRatio: number
}

export interface RealtimeEdgesResponse {
  bucketMs: number
  fromMs: number
  toMs: number
  edges: Array<{
    queueId: number
    targetVertexId?: number
    points: RealtimeEdgePoint[]
  }>
}

export interface RealtimeVertexPoint {
  ts: number
  subtaskCount: number

  sourceReadNs: number
  sourceIdleNs: number
  sourceReadRatio: number
  sourceIdleRatio: number

  transformProcessNs: number
  transformRecordsIn: number
  transformRecordsOut: number
  transformBusyRatio: number
  transformProcessNsPerRecord: number

  sinkWriteNs: number
  sinkRecordsIn: number
  sinkPrepareCommitNs: number
  sinkCommitNs: number
  sinkAbortNs: number
  sinkBusyRatio: number
  sinkWriteNsPerRecord: number
}

export interface RealtimeVerticesResponse {
  bucketMs: number
  fromMs: number
  toMs: number
  vertices: Array<{
    vertexId: number
    points: RealtimeVertexPoint[]
  }>
}

export const getRealtimeJobEdges = (jobId: string, windowMs: number) =>
  get<RealtimeEdgesResponse>(`/metrics/realtime/jobs/${jobId}/edges`, { windowMs })

export const getRealtimeJobVertices = (jobId: string, windowMs: number) =>
  get<RealtimeVerticesResponse>(`/metrics/realtime/jobs/${jobId}/vertices`, { windowMs })

export const RealtimeMetricsService = {
  getRealtimeJobEdges,
  getRealtimeJobVertices
}
