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

import { describe, expect, test } from 'vitest'
import type { Vertex } from '@/service/job/types'
import type { PipelineCheckpointOverview } from '@/service/checkpoint/types'
import type { RealtimeEdgePoint, RealtimeVertexPoint } from '@/service/realtime-metrics'
import {
  isLargeGraph,
  getTopBusyVertices,
  getTopBlockedInputs,
  sumCheckpointCounts,
  getLatestCheckpoint,
  formatDurationMs,
  formatCheckpoint
} from '@/views/jobs/detail-runtime-summary'

const vertex = (vertexId: number, type: Vertex['type'] = 'source'): Vertex => ({
  vertexId,
  vertexName: `vertex-${vertexId}`,
  type,
  tablePaths: []
})

const vertexPoint = (overrides: Partial<RealtimeVertexPoint> = {}): RealtimeVertexPoint => ({
  ts: 1000,
  subtaskCount: 1,
  sourceReadNs: 0,
  sourceIdleNs: 0,
  sourceReadRatio: 0,
  sourceIdleRatio: 0,
  transformProcessNs: 0,
  transformRecordsIn: 0,
  transformRecordsOut: 0,
  transformBusyRatio: 0,
  transformProcessNsPerRecord: 0,
  sinkWriteNs: 0,
  sinkRecordsIn: 0,
  sinkPrepareCommitNs: 0,
  sinkCommitNs: 0,
  sinkAbortNs: 0,
  sinkBusyRatio: 0,
  sinkWriteNsPerRecord: 0,
  ...overrides
})

const edgePoint = (bpRatio: number, queueFillRatio: number): RealtimeEdgePoint => ({
  ts: 1000,
  emitBlockedNs: 0,
  bpRatio,
  queueFillRatio,
  queueSize: 12,
  queueCapacity: 20
})

const pipeline = (
  overrides: Partial<PipelineCheckpointOverview> = {}
): PipelineCheckpointOverview => ({
  pipelineId: 1,
  counts: { triggered: 0, completed: 0, failed: 0, inProgress: 0, restored: 0 },
  latestCompleted: {},
  latestFailed: {},
  latestSavepoint: {},
  inProgress: [],
  history: [],
  ...overrides
})

describe('runtime diagnosis summary', () => {
  test.each([
    [80, 120, false],
    [81, 0, true],
    [0, 121, true],
    [0, 0, false]
  ] as const)(
    'uses strict graph thresholds for %i vertices and %i edges',
    (vertices, edges, expected) => {
      expect(isLargeGraph(vertices, edges)).toBe(expected)
    }
  )

  test('ranks the five busiest vertices using the metric for each operator type', () => {
    const vertices = [
      vertex(1),
      vertex(2, 'transform'),
      vertex(3, 'sink'),
      vertex(4),
      vertex(5),
      vertex(6),
      vertex(7),
      vertex(8)
    ]
    const rows = getTopBusyVertices(vertices, {
      1: vertexPoint({ sourceReadRatio: 0.1, sinkBusyRatio: 1 }),
      2: vertexPoint({ sourceReadRatio: 0.01, transformBusyRatio: 0.9 }),
      3: vertexPoint({ sourceReadRatio: 1, sinkBusyRatio: 0.6 }),
      4: vertexPoint(),
      5: vertexPoint({ sourceReadRatio: 0.8 }),
      6: vertexPoint({ sourceReadRatio: 0.5 }),
      7: vertexPoint({ sourceReadRatio: 0.7 })
    })
    expect(rows.map((row) => [row.vertexId, row.busyRatio])).toEqual([
      [2, 0.9],
      [5, 0.8],
      [7, 0.7],
      [3, 0.6],
      [6, 0.5]
    ])
    expect(rows[0]).toMatchObject({ vertexName: 'vertex-2', vertexType: 'transform' })
    expect(vertices.map((v) => v.vertexId)).toEqual([1, 2, 3, 4, 5, 6, 7, 8])
  })

  test('omits vertices and inputs with no positive measurements', () => {
    expect(getTopBusyVertices([vertex(1), vertex(2)], { 1: vertexPoint() })).toEqual([])
    expect(
      getTopBlockedInputs({ '1': [{ inputVertexId: '1', targetVertexId: '2' }] }, [], {})
    ).toEqual([])
    expect(getTopBusyVertices([], {})).toEqual([])
    expect(getTopBlockedInputs({}, [], {})).toEqual([])
  })

  test('ranks inputs by the greater of wait and fill ratios, retaining stable ties', () => {
    const rows = getTopBlockedInputs(
      {
        '10': [2, 3, 4, 5, 6, 7, 8, 9].map((target) => ({
          inputVertexId: '1',
          targetVertexId: String(target)
        }))
      },
      [vertex(1), vertex(3)],
      {
        2: edgePoint(0.2, 0.1),
        3: edgePoint(0.1, 0.95),
        4: edgePoint(0.9, 0.2),
        5: edgePoint(0, 0),
        6: edgePoint(0.6, 0.3),
        7: edgePoint(0.3, 0.6),
        8: edgePoint(0.1, 0.1)
      }
    )
    expect(rows.map((row) => row.to)).toEqual(['vertex-3', '4', '6', '7', '2'])
    expect(rows[0]).toEqual({
      edgeId: 'edge-10-1-3',
      pipelineId: '10',
      from: 'vertex-1',
      to: 'vertex-3',
      bpRatio: 0.1,
      queueFillRatio: 0.95,
      queueSize: 12,
      queueCapacity: 20
    })
  })

  test('sums all checkpoint counters across pipelines and handles an empty overview', () => {
    expect(
      sumCheckpointCounts([
        pipeline({ counts: { triggered: 8, completed: 5, failed: 1, inProgress: 2, restored: 3 } }),
        pipeline({ counts: { triggered: 4, completed: 2, failed: 2, inProgress: 0, restored: 1 } })
      ])
    ).toEqual({ triggered: 12, completed: 7, failed: 3, inProgress: 2, restored: 4 })
    expect(sumCheckpointCounts([])).toEqual({
      triggered: 0,
      completed: 0,
      failed: 0,
      inProgress: 0,
      restored: 0
    })
  })

  test.each(['latestCompleted', 'latestFailed', 'latestSavepoint'] as const)(
    'selects %s by completion time with trigger-time fallback and ignores empty objects',
    (key) => {
      const pipelines = [
        pipeline({ [key]: { checkpointId: 99, triggerTimestamp: 400 } }),
        pipeline(),
        pipeline({ [key]: { checkpointId: 2, completedTimestamp: 500, triggerTimestamp: 100 } }),
        pipeline({ [key]: { checkpointId: 100, triggerTimestamp: 300 } }),
        pipeline({ [key]: { checkpointId: 101 } })
      ]
      expect(getLatestCheckpoint(pipelines, key)?.checkpointId).toBe(2)
      expect(getLatestCheckpoint([pipelines[0], pipelines[3]], key)?.checkpointId).toBe(99)
      expect(getLatestCheckpoint([pipeline()], key)).toBeUndefined()
      expect(getLatestCheckpoint([], key)).toBeUndefined()
    }
  )

  test.each([
    [undefined, 'unavailable'],
    [null, 'unavailable'],
    [0, '0 ms'],
    [999, '999 ms'],
    [1000, '1.00 s'],
    [1234, '1.23 s']
  ] as const)('formats duration %s without losing zero or the unit boundary', (value, expected) => {
    expect(formatDurationMs(value, 'unavailable')).toBe(expected)
  })

  test('formats empty checkpoints and preserves zero IDs, status and type fallback', () => {
    expect(formatCheckpoint({}, 'unavailable')).toBe('unavailable')
    expect(formatCheckpoint(undefined, 'unavailable')).toBe('unavailable')
    expect(
      formatCheckpoint(
        { checkpointId: 0, status: 'COMPLETED', checkpointType: 'CHECKPOINT', durationMillis: 0 },
        'unavailable'
      )
    ).toBe('#0 COMPLETED 0 ms')
    expect(
      formatCheckpoint(
        { checkpointId: 2, checkpointType: 'SAVEPOINT', durationMillis: 1000 },
        'unavailable'
      )
    ).toBe('#2 SAVEPOINT 1.00 s')
    expect(formatCheckpoint({ checkpointId: 3, status: 'FAILED' }, 'unavailable')).toBe(
      '#3 FAILED unavailable'
    )
  })
})
