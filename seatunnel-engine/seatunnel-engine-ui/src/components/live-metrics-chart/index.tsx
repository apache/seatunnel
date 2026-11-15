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

import { defineComponent, onBeforeUnmount, onMounted, ref, watch, type PropType } from 'vue'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import {
  GridComponent,
  LegendComponent,
  TitleComponent,
  TooltipComponent
} from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import type { LiveMetricSeries, MetricUnit } from './types'
import { formatMetricValue } from './group'

export type { LiveLineChartProps, LiveMetricPoint, LiveMetricSeries, MetricUnit } from './types'

echarts.use([
  LineChart,
  GridComponent,
  LegendComponent,
  TitleComponent,
  TooltipComponent,
  CanvasRenderer
])

const COLORS = ['#4678B9', '#18A058', '#F0A020', '#D03050', '#2080F0', '#8A2BE2']

export default defineComponent({
  name: 'LiveLineChart',
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
      default: 260
    },
    unit: {
      type: String as PropType<MetricUnit>,
      default: undefined
    }
  },
  setup(props) {
    const el = ref<HTMLDivElement>()
    let chart: echarts.ECharts | undefined

    const render = () => {
      if (!el.value) return
      if (!chart) {
        chart = echarts.init(el.value)
      }
      const series = props.series || []
      if (!series.length) {
        chart.clear()
        chart.setOption({
          title: {
            text: props.emptyText,
            left: 'center',
            top: 'middle',
            textStyle: { color: '#999', fontSize: 14, fontWeight: 'normal' }
          }
        })
        return
      }

      const now = Date.now()
      const from = now - props.windowMs
      const unit = props.unit || series[0]?.unit
      const single = series.length === 1
      const compact =
        props.height === '100%' || (typeof props.height === 'number' && props.height < 160)
      const isVisiblePoint = (point: { ts: number; value: number }) =>
        point.ts >= from && Number.isFinite(point.value)
      const hasVisiblePoints = series.some((item) => (item.points || []).some(isVisiblePoint))

      const yAxis =
        unit === 'ratio'
          ? {
              type: 'value' as const,
              min: 0,
              max: 1,
              scale: false,
              axisLabel: {
                fontSize: compact ? 9 : 10,
                margin: compact ? 4 : 8,
                formatter: (v: number) => `${Math.round(v * 100)}`
              },
              splitLine: { lineStyle: { type: 'dashed', color: '#eee' } }
            }
          : {
              show: unit !== 'count' || hasVisiblePoints,
              type: 'value' as const,
              min: compact ? 0 : undefined,
              minInterval: unit === 'count' ? 1 : undefined,
              scale: !compact,
              axisLabel: { fontSize: compact ? 9 : 10, margin: compact ? 4 : 8 },
              splitLine: { lineStyle: { type: 'dashed', color: '#eee' } }
            }

      chart.setOption(
        {
          title:
            unit === 'count' && !hasVisiblePoints
              ? {
                  show: true,
                  text: props.emptyText,
                  left: 'center',
                  top: 'middle',
                  textStyle: {
                    color: '#9ca3af',
                    fontSize: compact ? 10 : 12,
                    fontWeight: 'normal'
                  }
                }
              : single && !compact
                ? {
                    show: true,
                    text: series[0].name,
                    left: 0,
                    top: 0,
                    textStyle: { color: '#666', fontSize: compact ? 11 : 12, fontWeight: 'normal' }
                  }
                : { show: false },
          color: COLORS,
          tooltip: {
            trigger: 'axis',
            formatter: (params: unknown) => {
              const items = Array.isArray(params) ? params : [params]
              const first = items[0] as { axisValue?: number }
              const time = first?.axisValue ? new Date(first.axisValue).toLocaleTimeString() : ''
              const lines = items.map((raw) => {
                const item = raw as {
                  marker?: string
                  seriesName?: string
                  value?: [number, number]
                }
                const match = series.find((s) => s.name === item.seriesName)
                const label = match?.fullName || item.seriesName || ''
                const value = Array.isArray(item.value) ? item.value[1] : Number.NaN
                return `${item.marker || ''}${label}: ${formatMetricValue(unit, value)}`
              })
              return [time, ...lines].join('<br/>')
            }
          },
          legend:
            single && !compact
              ? { show: false }
              : {
                  type: 'scroll',
                  top: 0,
                  itemWidth: compact ? 10 : 14,
                  itemHeight: compact ? 8 : 10,
                  textStyle: { fontSize: compact ? 10 : 11 },
                  data: series.map((s) => s.name)
                },
          grid: {
            left: compact ? 28 : 48,
            right: compact ? 4 : 24,
            // Reserve one line for the legend so it never overlaps the highest Y-axis label.
            top: compact ? 30 : single ? 28 : 36,
            bottom: compact ? 10 : 32,
            containLabel: !compact
          },
          xAxis: {
            show: unit !== 'count' || hasVisiblePoints,
            type: 'time',
            min: from,
            max: now,
            boundaryGap: false,
            axisTick: { show: !compact },
            axisLine: { show: true, lineStyle: { color: compact ? '#e5e7eb' : '#d1d5db' } },
            axisLabel: compact
              ? { show: false, margin: 0 }
              : {
                  formatter: (value: number) =>
                    new Date(value).toLocaleTimeString(undefined, {
                      hour: '2-digit',
                      minute: '2-digit',
                      second: '2-digit'
                    })
                },
            splitLine: { show: false }
          },
          yAxis,
          series: series.map((s, i) => {
            const color = COLORS[i % COLORS.length]
            return {
              name: s.name,
              type: 'line',
              showSymbol: false,
              smooth: true,
              lineStyle: { width: compact ? 1.5 : 2, color },
              itemStyle: { color },
              areaStyle: compact
                ? {
                    color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                      { offset: 0, color: `${color}73` },
                      { offset: 1, color: `${color}1A` }
                    ])
                  }
                : undefined,
              data: [...(s.points || [])]
                .filter(isVisiblePoint)
                .sort((a, b) => a.ts - b.ts)
                .map((p) => [p.ts, p.value])
            }
          })
        },
        true
      )
    }

    const onResize = () => chart?.resize()
    let resizeObserver: ResizeObserver | undefined

    onMounted(() => {
      render()
      window.addEventListener('resize', onResize)
      if (el.value && typeof ResizeObserver !== 'undefined') {
        resizeObserver = new ResizeObserver(() => onResize())
        resizeObserver.observe(el.value)
      }
    })
    onBeforeUnmount(() => {
      window.removeEventListener('resize', onResize)
      resizeObserver?.disconnect()
      chart?.dispose()
      chart = undefined
    })
    watch(
      () => [props.series, props.windowMs, props.emptyText, props.unit, props.height],
      () => render(),
      { deep: true }
    )

    return () => {
      const height = typeof props.height === 'number' ? `${props.height}px` : props.height
      return (
        <div ref={el} class="live-metrics-chart w-full" style={{ height, minHeight: height }} />
      )
    }
  }
})
