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

import { beforeEach, describe, expect, it } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { LIVE_METRICS_PIN_LIMIT, useLiveMetricsPinStore } from '@/store/live-metrics-pin'

const samplePin = (index: number) => ({
  id: `vertex:${index}:sourceReadRatio`,
  name: `Source ${index} · Source Read Ratio`,
  kind: 'vertex' as const,
  targetId: index,
  field: 'sourceReadRatio'
})

describe('live metrics pin store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('pins a series and unpins it', () => {
    const store = useLiveMetricsPinStore()
    store.ensureJob('job-1')
    expect(store.pin(samplePin(1))).toBe('ok')
    expect(store.pins).toHaveLength(1)
    expect(store.isPinned('vertex:1:sourceReadRatio')).toBe(true)
    expect(store.pin(samplePin(1))).toBe('exists')
    expect(store.toggle(samplePin(1))).toBe('removed')
    expect(store.pins).toHaveLength(0)
  })

  it('rejects pins beyond the documented limit', () => {
    const store = useLiveMetricsPinStore()
    store.ensureJob('job-1')
    for (let i = 1; i <= LIVE_METRICS_PIN_LIMIT; i++) {
      expect(store.pin(samplePin(i))).toBe('ok')
    }
    expect(store.pin(samplePin(LIVE_METRICS_PIN_LIMIT + 1))).toBe('limit')
    expect(store.pins).toHaveLength(LIVE_METRICS_PIN_LIMIT)
    expect(store.remaining).toBe(0)
  })

  it('clears pins when switching jobs or leaving the page', () => {
    const store = useLiveMetricsPinStore()
    store.ensureJob('job-1')
    store.pin(samplePin(1))
    store.ensureJob('job-2')
    expect(store.pins).toHaveLength(0)
    store.pin(samplePin(2))
    store.clear()
    expect(store.pins).toHaveLength(0)
    expect(store.jobId).toBeNull()
  })
})
