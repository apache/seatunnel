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

import type { LiveMetricSeries, MetricUnit } from '@/components/live-metrics-chart/types'
import { inferMetricUnit } from '@/components/live-metrics-chart/group'
import type { PinnedMetricRef } from '@/store/live-metrics-pin'
import type {
  RealtimeEdgePoint,
  RealtimeEdgesResponse,
  RealtimeVertexPoint,
  RealtimeVerticesResponse
} from '@/service/realtime-metrics'
import { extractVertexIdentifier } from './detail-metrics'

export type Translate = (key: string) => string

export type PinableField = {
  field: string
  label: string
  unit: MetricUnit
  /** Transform point value for charting (e.g. ns -> ms) */
  mapValue?: (v: number) => number
}

const identityTranslate: Translate = (key) => key
const ratio = (v: number) => v
const nsToMs = (v: number) => v / 1_000_000

export const shortOperatorLabel = (name?: string): string => {
  if (!name) return ''
  return extractVertexIdentifier(name) || name
}

export const vertexPinFields = (
  type?: string,
  t: Translate = identityTranslate
): PinableField[] => {
  if (type === 'source') {
    return [
      {
        field: 'sourceReadRatio',
        label: t('detail.observability.sourceReadRatio'),
        unit: 'ratio',
        mapValue: ratio
      },
      {
        field: 'sourceIdleRatio',
        label: t('detail.observability.sourceIdleRatio'),
        unit: 'ratio',
        mapValue: ratio
      }
    ]
  }
  if (type === 'transform') {
    return [
      {
        field: 'transformBusyRatio',
        label: t('detail.observability.transformBusyRatio'),
        unit: 'ratio',
        mapValue: ratio
      },
      {
        field: 'transformProcessNsPerRecord',
        label: t('detail.observability.processMsPerRecord'),
        unit: 'duration',
        mapValue: nsToMs
      },
      { field: 'transformRecordsIn', label: t('detail.observability.recordsIn'), unit: 'count' },
      { field: 'transformRecordsOut', label: t('detail.observability.recordsOut'), unit: 'count' }
    ]
  }
  if (type === 'sink') {
    return [
      {
        field: 'sinkBusyRatio',
        label: t('detail.observability.sinkBusyRatio'),
        unit: 'ratio',
        mapValue: ratio
      },
      {
        field: 'sinkWriteNsPerRecord',
        label: t('detail.observability.writeMsPerRecord'),
        unit: 'duration',
        mapValue: nsToMs
      },
      { field: 'sinkRecordsIn', label: t('detail.observability.recordsIn'), unit: 'count' }
    ]
  }
  return []
}

export const edgePinFields = (t: Translate = identityTranslate): PinableField[] => [
  { field: 'bpRatio', label: t('detail.observability.bpRatio'), unit: 'ratio', mapValue: ratio },
  {
    field: 'queueFillRatio',
    label: t('detail.observability.queueFillRatio'),
    unit: 'ratio',
    mapValue: ratio
  }
]

export const vertexSeriesId = (vertexId: number, field: string) => `vertex:${vertexId}:${field}`
export const edgeSeriesId = (targetVertexId: number, field: string) =>
  `edge:${targetVertexId}:${field}`

const readPointField = (point: Record<string, unknown>, field: string): number | undefined => {
  const v = point[field]
  return typeof v === 'number' && Number.isFinite(v) ? v : undefined
}

export const buildSeriesFromVertexPoints = (
  vertexId: number,
  vertexName: string,
  field: PinableField,
  points: RealtimeVertexPoint[],
  limit: number
): LiveMetricSeries => {
  const mapValue = field.mapValue || ((v: number) => v)
  const shortName = `${shortOperatorLabel(vertexName)} · ${field.label}`
  return {
    id: vertexSeriesId(vertexId, field.field),
    name: shortName,
    fullName: `${vertexName} · ${field.label}`,
    unit: field.unit,
    points: [...points]
      .slice(-limit)
      .sort((a, b) => a.ts - b.ts)
      .map((p) => {
        const raw = readPointField(p as unknown as Record<string, unknown>, field.field)
        return raw === undefined ? null : { ts: p.ts, value: mapValue(raw) }
      })
      .filter((p): p is { ts: number; value: number } => p !== null)
  }
}

export const buildSeriesFromEdgePoints = (
  targetVertexId: number,
  edgeLabel: string,
  field: PinableField,
  points: RealtimeEdgePoint[],
  limit: number
): LiveMetricSeries => {
  const mapValue = field.mapValue || ((v: number) => v)
  const shortName = `${edgeLabel} · ${field.label}`
  return {
    id: edgeSeriesId(targetVertexId, field.field),
    name: shortName,
    fullName: shortName,
    unit: field.unit,
    points: [...points]
      .slice(-limit)
      .sort((a, b) => a.ts - b.ts)
      .map((p) => {
        const raw = readPointField(p as unknown as Record<string, unknown>, field.field)
        return raw === undefined ? null : { ts: p.ts, value: mapValue(raw) }
      })
      .filter((p): p is { ts: number; value: number } => p !== null)
  }
}

export const decodeTargetVertexId = (queueId: number) => {
  if (queueId >= 0) return queueId
  const abs = Math.abs(queueId)
  if (!abs) return undefined
  if (abs % 2 === 0) return abs / 2
  return (abs - 1) / 2
}

export const resolvePinnedSeries = (
  pins: PinnedMetricRef[],
  vertices: RealtimeVerticesResponse | undefined,
  edges: RealtimeEdgesResponse | undefined,
  vertexNameById: Record<number, string>,
  limit: number
): LiveMetricSeries[] => {
  return pins
    .map((pin) => {
      if (pin.kind === 'vertex') {
        const points = vertices?.vertices?.find((v) => v.vertexId === pin.targetId)?.points || []
        const field: PinableField = {
          field: pin.field,
          label: pin.name.split(' · ').slice(1).join(' · ') || pin.field,
          unit: inferMetricUnit(pin.field),
          mapValue: pin.field.endsWith('NsPerRecord') ? nsToMs : undefined
        }
        return buildSeriesFromVertexPoints(
          pin.targetId,
          vertexNameById[pin.targetId] || String(pin.targetId),
          field,
          points,
          limit
        )
      }
      const points =
        edges?.edges?.find(
          (e) => (e.targetVertexId ?? decodeTargetVertexId(e.queueId)) === pin.targetId
        )?.points || []
      const field: PinableField = {
        field: pin.field,
        label: pin.name.split(' · ').slice(1).join(' · ') || pin.field,
        unit: inferMetricUnit(pin.field)
      }
      return buildSeriesFromEdgePoints(
        pin.targetId,
        pin.name.split(' · ')[0] || 'edge',
        field,
        points,
        limit
      )
    })
    .map((s, idx) => ({
      ...s,
      name: pins[idx]?.name || s.name
    }))
}
