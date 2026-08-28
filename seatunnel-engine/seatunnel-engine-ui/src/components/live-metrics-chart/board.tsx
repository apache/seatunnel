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
      type: Number,
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
    const groups = computed(() => groupSeriesByUnit(props.series || []))

    return () => {
      if (!groups.value.length) {
        return <div class="text-sm text-gray-400 py-2 text-center leading-6">{props.emptyText}</div>
      }
      const n = groups.value.length
      const gridClass =
        props.layout === 'row'
          ? n >= 3
            ? 'grid grid-cols-3 gap-2'
            : n === 2
              ? 'grid grid-cols-2 gap-2'
              : 'grid grid-cols-1 gap-2'
          : 'flex flex-col gap-3'
      return (
        <div class={gridClass}>
          {groups.value.map((group) => (
            <div key={group.unit} class="min-w-0">
              <div class="text-xs text-gray-500 mb-1 leading-4">
                {props.unitTitles[group.unit] || group.unit}
              </div>
              <div class="bg-white rounded border border-gray-100 overflow-hidden">
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
