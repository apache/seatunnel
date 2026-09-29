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

import { defineStore } from 'pinia'

export const LIVE_METRICS_PIN_LIMIT = 6

export interface PinnedMetricRef {
  id: string
  name: string
  /** vertex | edge */
  kind: 'vertex' | 'edge'
  /** vertexId or targetVertexId for edge */
  targetId: number
  /** field key on the realtime point */
  field: string
}

interface LiveMetricsPinState {
  jobId: string | null
  pins: PinnedMetricRef[]
}

export const useLiveMetricsPinStore = defineStore({
  id: 'live-metrics-pin',
  state: (): LiveMetricsPinState => ({
    jobId: null,
    pins: []
  }),
  getters: {
    pinIds(state): Set<string> {
      return new Set(state.pins.map((p) => p.id))
    },
    isPinned:
      (state) =>
      (id: string): boolean =>
        state.pins.some((p) => p.id === id),
    remaining(state): number {
      return Math.max(0, LIVE_METRICS_PIN_LIMIT - state.pins.length)
    }
  },
  actions: {
    /**
     * Bind pins to the current Job Detail visit. Switching job clears pins.
     */
    ensureJob(jobId: string) {
      if (this.jobId !== jobId) {
        this.jobId = jobId
        this.pins = []
      }
    },
    clear() {
      this.pins = []
      this.jobId = null
    },
    /**
     * @returns true if pinned; false if already present or limit reached
     */
    pin(ref: PinnedMetricRef): 'ok' | 'exists' | 'limit' {
      if (this.pins.some((p) => p.id === ref.id)) return 'exists'
      if (this.pins.length >= LIVE_METRICS_PIN_LIMIT) return 'limit'
      this.pins.push(ref)
      return 'ok'
    },
    unpin(id: string) {
      this.pins = this.pins.filter((p) => p.id !== id)
    },
    toggle(ref: PinnedMetricRef): 'ok' | 'exists' | 'limit' | 'removed' {
      if (this.pins.some((p) => p.id === ref.id)) {
        this.unpin(ref.id)
        return 'removed'
      }
      return this.pin(ref)
    }
  }
})
