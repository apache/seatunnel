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
  collectVertexMetrics,
  extractVertexIdentifier,
  readVertexMetricValue
} from '@/views/jobs/detail-metrics'
import type { Vertex } from '@/service/job/types'

describe('detail metrics helpers', () => {
  const sourceVertex = {
    vertexId: 1,
    type: 'source',
    vertexName: 'pipeline-1 [Source[0]]',
    tablePaths: ['fake.user_table']
  } as Vertex

  const sinkVertex = {
    vertexId: 2,
    type: 'sink',
    vertexName: 'pipeline-1 [Sink[1]]',
    tablePaths: ['fake.user_table']
  } as Vertex

  test('extracts the vertex identifier from the vertex name', () => {
    expect(extractVertexIdentifier(sourceVertex.vertexName)).toBe('Source[0]')
    expect(extractVertexIdentifier(sinkVertex.vertexName)).toBe('Sink[1]')
  })

  test('prefers 2.3.13 prefixed metric keys over raw table names', () => {
    const metricMap = {
      'Source[0].fake.user_table': '10',
      'Source[1].fake.user_table': '20',
      'fake.user_table': '999'
    }

    expect(readVertexMetricValue(metricMap, sourceVertex, 'fake.user_table')).toBe(10)
  })

  test('falls back to raw table names when prefixed keys are unavailable', () => {
    const metricMap = {
      'fake.user_table': '15'
    }

    expect(readVertexMetricValue(metricMap, sourceVertex, 'fake.user_table')).toBe(15)
  })

  test('collects only metrics that belong to the focused vertex', () => {
    const metricMap = {
      'Sink[0].fake.user_table': '5',
      'Sink[1].fake.user_table': '8'
    }

    expect(collectVertexMetrics('TableSinkWriteCount', metricMap, sinkVertex)).toEqual({
      'TableSinkWriteCount.fake.user_table': '8'
    })
  })
})
