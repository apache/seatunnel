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
import {
  buildSeriesFromVertexPoints,
  decodeTargetVertexId,
  resolvePinnedSeries,
  shortOperatorLabel,
  vertexPinFields,
  vertexSeriesId
} from '@/views/jobs/detail-live-metrics'
import {
  formatMetricValue,
  groupSeriesByUnit,
  inferMetricUnit
} from '@/components/live-metrics-chart/group'
import {
  describeRealtimeFetchError,
  effectiveRealtimeWindowMs,
  REALTIME_WINDOW_MS_DEFAULT,
  REALTIME_WINDOW_MS_MAX
} from '@/service/realtime-metrics'
import type { RealtimeVertexPoint, RealtimeVerticesResponse } from '@/service/realtime-metrics'

describe('detail live metrics helpers', () => {
  test('decodes negative queue ids to target vertex ids', () => {
    expect(decodeTargetVertexId(7)).toBe(7)
    expect(decodeTargetVertexId(-2)).toBe(1)
    expect(decodeTargetVertexId(-10)).toBe(5)
  })

  test('builds a vertex series sorted by ts and mapped to ms', () => {
    const field = vertexPinFields('transform').find(
      (item) => item.field === 'transformProcessNsPerRecord'
    )
    expect(field).toBeDefined()
    expect(field?.unit).toBe('duration')
    const series = buildSeriesFromVertexPoints(
      12,
      'Transform[0]',
      field!,
      [
        { ts: 2000, transformProcessNsPerRecord: 4_000_000 } as RealtimeVertexPoint,
        { ts: 1000, transformProcessNsPerRecord: 2_000_000 } as RealtimeVertexPoint
      ],
      10
    )
    expect(series.id).toBe(vertexSeriesId(12, 'transformProcessNsPerRecord'))
    expect(series.points).toEqual([
      { ts: 1000, value: 2 },
      { ts: 2000, value: 4 }
    ])
  })

  test('resolves pinned vertex series from the shared realtime response', () => {
    const vertices: RealtimeVerticesResponse = {
      bucketMs: 5000,
      fromMs: 0,
      toMs: 5000,
      vertices: [
        {
          vertexId: 1,
          points: [{ ts: 1000, sourceReadRatio: 0.4 } as RealtimeVertexPoint]
        }
      ]
    }
    const series = resolvePinnedSeries(
      [
        {
          id: 'vertex:1:sourceReadRatio',
          name: 'Source[0] · Source Read Ratio',
          kind: 'vertex',
          targetId: 1,
          field: 'sourceReadRatio'
        }
      ],
      vertices,
      undefined,
      { 1: 'Source[0]' },
      10
    )
    expect(series).toHaveLength(1)
    expect(series[0].name).toBe('Source[0] · Source Read Ratio')
    expect(series[0].unit).toBe('ratio')
    expect(series[0].points).toEqual([{ ts: 1000, value: 0.4 }])
  })
})

describe('metric display grouping', () => {
  test('shortens vertex names to operator identifiers', () => {
    expect(shortOperatorLabel('pipeline-1 [Source[0]-FakeSource]')).toBe('Source[0]')
    expect(shortOperatorLabel('plain-name')).toBe('plain-name')
  })

  test('splits mixed units so ratios are not flattened by counts', () => {
    expect(inferMetricUnit('sourceReadRatio')).toBe('ratio')
    expect(inferMetricUnit('sinkWriteNsPerRecord')).toBe('duration')
    expect(inferMetricUnit('sinkRecordsIn')).toBe('count')
    const groups = groupSeriesByUnit([
      { id: 'a', name: 'read', unit: 'ratio', points: [{ ts: 1, value: 0.4 }] },
      { id: 'b', name: 'in', unit: 'count', points: [{ ts: 1, value: 80 }] }
    ])
    expect(groups.map((g) => g.unit)).toEqual(['ratio', 'count'])
    expect(formatMetricValue('ratio', 0.452)).toBe('45.2%')
    expect(formatMetricValue('duration', 1.888)).toBe('1.89 ms')
  })
})

describe('shared realtime fetch helpers', () => {
  test('caps the query window at 10 minutes', () => {
    expect(effectiveRealtimeWindowMs()).toBe(REALTIME_WINDOW_MS_DEFAULT)
    expect(effectiveRealtimeWindowMs(20 * 60 * 1000)).toBe(REALTIME_WINDOW_MS_MAX)
  })

  test('maps fetch errors without leaking response bodies', () => {
    expect(describeRealtimeFetchError({ response: { status: 503 } })).toBe(
      'Realtime metrics disabled on master'
    )
    expect(describeRealtimeFetchError({ response: { status: 404 } })).toBe(
      'Realtime metrics job not found'
    )
    expect(describeRealtimeFetchError(new Error('network'))).toBe(
      'Failed to fetch realtime metrics'
    )
  })
})
