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

import type { Edge, Vertex } from '@/service/job/types'
import type { RealtimeEdgePoint, RealtimeVertexPoint } from '@/service/realtime-metrics'
import type { CheckpointInfo, PipelineCheckpointOverview } from '@/service/checkpoint/types'

export interface RuntimeVertexRow {
  vertexId: number
  vertexName: string
  vertexType: string
  busyRatio: number
}

export interface RuntimeEdgeRow {
  edgeId: string
  pipelineId: string
  from: string
  to: string
  bpRatio: number
  queueFillRatio: number
  queueSize: number
  queueCapacity: number
}

const RUNTIME_SUMMARY_ROW_LIMIT = 5

// The summary supplements the DAG only above these limits; it does not replace rendering.
export const isLargeGraph = (vertexCount: number, edgeCount: number) =>
  vertexCount > 80 || edgeCount > 120

const runtimeVertexRatio = (vertex: Vertex, latest?: RealtimeVertexPoint) => {
  if (!latest) return 0
  if (vertex.type === 'source') return latest.sourceReadRatio
  if (vertex.type === 'transform') return latest.transformBusyRatio
  if (vertex.type === 'sink') return latest.sinkBusyRatio
  return 0
}

// Each operator type reports busyness in a different field of the same metric point.
export const getTopBusyVertices = (
  vertices: Vertex[],
  stats: Record<number, RealtimeVertexPoint>
): RuntimeVertexRow[] =>
  vertices
    .map((vertex) => ({
      vertexId: vertex.vertexId,
      vertexName: vertex.vertexName,
      vertexType: vertex.type,
      busyRatio: runtimeVertexRatio(vertex, stats[vertex.vertexId])
    }))
    .filter((row) => row.busyRatio > 0)
    .sort((a, b) => b.busyRatio - a.busyRatio)
    .slice(0, RUNTIME_SUMMARY_ROW_LIMIT)

// Queue metrics describe downstream inputs, not individual physical upstream edges.
export const getTopBlockedInputs = (
  pipelineEdges: Record<string, Edge[]>,
  vertices: Vertex[],
  stats: Record<number, RealtimeEdgePoint>
): RuntimeEdgeRow[] => {
  const names: Record<string, string> = {}
  vertices.forEach((vertex) => {
    names[String(vertex.vertexId)] = vertex.vertexName
  })
  return Object.entries(pipelineEdges)
    .flatMap(([pipelineId, edges]) =>
      edges.map((edge) => {
        const metrics = stats[Number(edge.targetVertexId)]
        return {
          edgeId: `edge-${pipelineId}-${edge.inputVertexId}-${edge.targetVertexId}`,
          pipelineId,
          from: names[edge.inputVertexId] || edge.inputVertexId,
          to: names[edge.targetVertexId] || edge.targetVertexId,
          bpRatio: metrics?.bpRatio ?? 0,
          queueFillRatio: metrics?.queueFillRatio ?? 0,
          queueSize: metrics?.queueSize ?? 0,
          queueCapacity: metrics?.queueCapacity ?? 0
        }
      })
    )
    .filter((row) => row.bpRatio > 0 || row.queueFillRatio > 0)
    .sort((a, b) => Math.max(b.bpRatio, b.queueFillRatio) - Math.max(a.bpRatio, a.queueFillRatio))
    .slice(0, RUNTIME_SUMMARY_ROW_LIMIT)
}

// Counts are per pipeline, so the job overview must aggregate rather than select one pipeline.
export const sumCheckpointCounts = (pipelines: PipelineCheckpointOverview[]) =>
  pipelines.reduce(
    (counts, pipeline) => {
      counts.triggered += pipeline.counts?.triggered || 0
      counts.completed += pipeline.counts?.completed || 0
      counts.failed += pipeline.counts?.failed || 0
      counts.inProgress += pipeline.counts?.inProgress || 0
      counts.restored += pipeline.counts?.restored || 0
      return counts
    },
    { triggered: 0, completed: 0, failed: 0, inProgress: 0, restored: 0 }
  )

// The REST API returns an empty object for a checkpoint that has not occurred yet.
const hasCheckpoint = (
  checkpoint?: CheckpointInfo
): checkpoint is CheckpointInfo & { checkpointId: number } =>
  checkpoint?.checkpointId !== undefined && checkpoint.checkpointId !== null

// Failed or unfinished events may only have a trigger timestamp; IDs do not order events.
export const getLatestCheckpoint = (
  pipelines: PipelineCheckpointOverview[],
  key: 'latestCompleted' | 'latestFailed' | 'latestSavepoint'
) =>
  pipelines
    .map((pipeline) => pipeline[key])
    .filter(hasCheckpoint)
    .sort(
      (a, b) =>
        (b.completedTimestamp || b.triggerTimestamp || 0) -
        (a.completedTimestamp || a.triggerTimestamp || 0)
    )[0]

export const formatDurationMs = (value: number | null | undefined, unavailable: string) => {
  if (value === undefined || value === null) return unavailable
  if (value < 1000) return `${value} ms`
  return `${(value / 1000).toFixed(2)} s`
}

export const formatCheckpoint = (checkpoint: CheckpointInfo | undefined, unavailable: string) => {
  if (!hasCheckpoint(checkpoint)) return unavailable
  const status = checkpoint.status || checkpoint.checkpointType || ''
  return `#${checkpoint.checkpointId} ${status} ${formatDurationMs(checkpoint.durationMillis, unavailable)}`
}
