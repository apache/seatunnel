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

import { computed, defineComponent, type PropType } from 'vue'
import LiveLineChart from './index'
import { groupSeriesByUnit } from './group'
import type { LiveMetricSeries, MetricUnit } from './types'

export default defineComponent({
  name: 'LiveMetricsBoard',
  props: {
    series: {
      type: Array as PropType<LiveMetricSeries[]>,
      default: () => []
    },
    windowMs: {
      type: Number,
      default: 3 * 60 * 1000
    },
    emptyText: {
      type: String,
      default: 'No metrics'
    },
    height: {
      type: [Number, String] as PropType<number | string>,
      default: 220
    },
    unitTitles: {
      type: Object as PropType<Partial<Record<MetricUnit, string>>>,
      default: () => ({})
    },
    /** Overview uses a single compact row; the drawer stays stacked. */
    layout: {
      type: String as PropType<'stack' | 'row'>,
      default: 'stack'
    }
  },
  setup(props) {
    // A pinned metric stays visible even before realtime samples arrive. Removing an empty
    // group would make the layout jump and would make a successful pin look like it was ignored.
    const groups = computed(() => groupSeriesByUnit(props.series || []))

    return () => {
      if (!groups.value.length) {
        return (
          <div class="live-metrics-board-empty text-sm text-gray-400 py-2 text-center leading-6">
            {props.emptyText}
          </div>
        )
      }
      const gridClass = props.layout === 'row' ? 'grid gap-2' : 'flex flex-col gap-3'
      const gridStyle =
        props.layout === 'row'
          ? {
              gridTemplateColumns:
                groups.value.length === 1
                  ? 'minmax(0, calc((100% - 8px) / 2))'
                  : 'repeat(auto-fit, minmax(min(320px, 100%), 1fr))'
            }
          : undefined
      return (
        <div
          class={`${gridClass} live-metrics-board ${
            groups.value.length === 1 ? 'live-metrics-board-single' : ''
          }`}
          style={gridStyle}
        >
          {groups.value.map((group) => (
            <div key={group.unit} class="live-metrics-group min-w-0">
              <div class="text-xs text-gray-500 mb-1 leading-4">
                {props.unitTitles[group.unit] || group.unit}
              </div>
              <div class="live-metrics-chart-container bg-white rounded border border-gray-100 overflow-hidden">
                <LiveLineChart
                  series={group.series}
                  windowMs={props.windowMs}
                  emptyText={props.emptyText}
                  height={props.height}
                  unit={group.unit}
                />
              </div>
            </div>
          ))}
        </div>
      )
    }
  }
})
