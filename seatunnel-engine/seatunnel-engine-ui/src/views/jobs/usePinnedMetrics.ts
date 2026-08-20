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

import { ref, computed } from 'vue'
import type {
  RealtimeVertexPoint,
  RealtimeEdgePoint,
  RealtimeVerticesResponse,
  RealtimeEdgesResponse
} from '@/service/realtime-metrics'

export interface PinnedMetric {
  /** Unique identifier for this pin */
  id: string
  /** Label shown in the pin chip and chart legend */
  label: string
  /** 'vertex' or 'edge' */
  sourceType: 'vertex' | 'edge'
  /** Vertex or edge target vertex ID */
  sourceId: number
  /** The metric key on the point object (e.g., 'sourceReadRatio', 'bpRatio') */
  metricKey: string
  /** Human-readable metric name */
  metricLabel: string
  /** Vertex type for determining which metrics are available */
  vertexType?: 'source' | 'transform' | 'sink'
}

const MAX_PINS = 6

export function usePinnedMetrics() {
  const pins = ref<PinnedMetric[]>([])

  const pinLimitReached = computed(() => pins.value.length >= MAX_PINS)

  const pinCount = computed(() => pins.value.length)

  const addPin = (pin: PinnedMetric): boolean => {
    if (pinLimitReached.value) return false
    const exists = pins.value.some(
      (p) => p.sourceType === pin.sourceType && p.sourceId === pin.sourceId && p.metricKey === pin.metricKey
    )
    if (exists) return false
    pins.value.push(pin)
    return true
  }

  const removePin = (id: string) => {
    pins.value = pins.value.filter((p) => p.id !== id)
  }

  const clearPins = () => {
    pins.value = []
  }

  const hasPins = computed(() => pins.value.length > 0)

  /**
   * Extract time-series data for all pinned metrics from the current realtime data snapshots.
   * Returns series data suitable for ECharts.
   */
  const getPinnedSeriesData = (
    vertices: RealtimeVerticesResponse | undefined,
    edges: RealtimeEdgesResponse | undefined,
    vertexNameMap: Map<number, string>
  ) => {
    if (!hasPins.value || (!vertices && !edges)) return []

    const seriesMap = new Map<string, { name: string; data: [number, number][] }>()

    pins.value.forEach((pin) => {
      if (pin.sourceType === 'vertex' && vertices) {
        const vertex = vertices.vertices?.find((v) => v.vertexId === pin.sourceId)
        if (vertex?.points) {
          const data = vertex.points
            .map((p) => [p.ts, (p as any)[pin.metricKey] as number] as [number, number])
            .filter(([, v]) => typeof v === 'number' && Number.isFinite(v))
            .sort((a, b) => a[0] - b[0])
          if (data.length > 0) {
            seriesMap.set(pin.id, { name: pin.label, data })
          }
        }
      } else if (pin.sourceType === 'edge' && edges) {
        const decodeTargetVertexId = (queueId: number) => {
          if (queueId >= 0) return queueId
          const abs = Math.abs(queueId)
          if (!abs) return undefined
          if (abs % 2 === 0) return abs / 2
          return (abs - 1) / 2
        }
        const edge = edges.edges?.find(
          (e) => (e.targetVertexId ?? decodeTargetVertexId(e.queueId)) === pin.sourceId
        )
        if (edge?.points) {
          const data = edge.points
            .map((p) => [p.ts, (p as any)[pin.metricKey] as number] as [number, number])
            .filter(([, v]) => typeof v === 'number' && Number.isFinite(v))
            .sort((a, b) => a[0] - b[0])
          if (data.length > 0) {
            seriesMap.set(pin.id, { name: pin.label, data })
          }
        }
      }
    })

    return Array.from(seriesMap.values())
  }

  return {
    pins,
    pinLimitReached,
    pinCount,
    addPin,
    removePin,
    clearPins,
    hasPins,
    getPinnedSeriesData,
    MAX_PINS
  }
}