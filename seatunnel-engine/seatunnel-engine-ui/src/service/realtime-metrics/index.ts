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

/** Overview poll interval. Pinning must not add extra REST traffic. */
export const REALTIME_POLL_INTERVAL_MS = 2000
/** Default query window (3 minutes). */
export const REALTIME_WINDOW_MS_DEFAULT = 3 * 60 * 1000
/** Hard cap for the query window (10 minutes). */
export const REALTIME_WINDOW_MS_MAX = 10 * 60 * 1000

export const effectiveRealtimeWindowMs = (windowMs = REALTIME_WINDOW_MS_DEFAULT) =>
  Math.min(windowMs, REALTIME_WINDOW_MS_MAX)

/**
 * Shared job-level fetch used by Job Detail Overview and follow-up
 * observability views. Callers own the poll loop; this helper does not start one.
 */
export const fetchJobRealtimeMetrics = async (
  jobId: string,
  windowMs = REALTIME_WINDOW_MS_DEFAULT
) => {
  const effectiveWindowMs = effectiveRealtimeWindowMs(windowMs)
  const [edges, vertices] = await Promise.all([
    getRealtimeJobEdges(jobId, effectiveWindowMs),
    getRealtimeJobVertices(jobId, effectiveWindowMs)
  ])
  return { edges, vertices, windowMs: effectiveWindowMs }
}

export const describeRealtimeFetchError = (error: unknown): string => {
  const status = (error as { response?: { status?: number } })?.response?.status
  if (status === 503) {
    return 'Realtime metrics disabled on master'
  }
  if (status === 404) {
    return 'Realtime metrics job not found'
  }
  if (status === 401 || status === 403) {
    return 'Realtime metrics unauthorized'
  }
  return 'Failed to fetch realtime metrics'
}

export const getRealtimeJobEdges = (jobId: string, windowMs: number) =>
  get<RealtimeEdgesResponse>(`/metrics/realtime/jobs/${jobId}/edges`, { windowMs })

export const getRealtimeJobVertices = (jobId: string, windowMs: number) =>
  get<RealtimeVerticesResponse>(`/metrics/realtime/jobs/${jobId}/vertices`, { windowMs })

export const RealtimeMetricsService = {
  getRealtimeJobEdges,
  getRealtimeJobVertices,
  fetchJobRealtimeMetrics
}
