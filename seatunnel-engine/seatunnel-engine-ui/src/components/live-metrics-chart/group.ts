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

import type { LiveMetricSeries, MetricUnit } from './types'

export const METRIC_UNIT_ORDER: MetricUnit[] = ['ratio', 'duration', 'count']

export const inferMetricUnit = (field: string): MetricUnit => {
  if (field.endsWith('NsPerRecord') || field.toLowerCase().includes('msperrecord')) {
    return 'duration'
  }
  if (field.endsWith('Ratio') || field === 'bpRatio') {
    return 'ratio'
  }
  return 'count'
}

export const groupSeriesByUnit = (
  series: LiveMetricSeries[]
): Array<{ unit: MetricUnit; series: LiveMetricSeries[] }> => {
  const buckets: Record<MetricUnit, LiveMetricSeries[]> = {
    ratio: [],
    duration: [],
    count: []
  }
  series.forEach((item) => {
    buckets[item.unit || 'count'].push(item)
  })
  return METRIC_UNIT_ORDER.filter((unit) => buckets[unit].length).map((unit) => ({
    unit,
    series: buckets[unit]
  }))
}

export const formatMetricValue = (unit: MetricUnit | undefined, value: number): string => {
  if (!Number.isFinite(value)) return '-'
  if (unit === 'ratio') {
    return `${(value * 100).toFixed(1)}%`
  }
  if (unit === 'duration') {
    return `${value.toFixed(2)} ms`
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(1)
}
