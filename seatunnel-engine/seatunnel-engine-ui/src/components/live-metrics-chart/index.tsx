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

import { defineComponent, ref, watch, onMounted, onUnmounted, type PropType } from 'vue'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { TitleComponent, TooltipComponent, LegendComponent, GridComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

echarts.use([TitleComponent, TooltipComponent, LegendComponent, GridComponent, LineChart, CanvasRenderer])

export interface ChartSeries {
  name: string
  data: [number, number][]
}

export default defineComponent({
  name: 'LiveMetricsChart',
  props: {
    series: {
      type: Array as PropType<ChartSeries[]>,
      default: () => []
    },
    height: {
      type: String,
      default: '300px'
    },
    emptyText: {
      type: String,
      default: 'No data'
    }
  },
  setup(props) {
    const chartRef = ref<HTMLDivElement>()
    let chartInstance: echarts.ECharts | null = null
    let resizeObserver: ResizeObserver | null = null

    const getChartOption = () => {
      const seriesData = props.series || []
      if (seriesData.length === 0) {
        return {
          title: {
            text: props.emptyText,
            left: 'center',
            top: 'center',
            textStyle: { color: '#999', fontSize: 14 }
          }
        }
      }

      return {
        tooltip: {
          trigger: 'axis',
          formatter: (params: any) => {
            if (!Array.isArray(params)) params = [params]
            const time = new Date(params[0]?.data?.[0]).toLocaleTimeString()
            let html = `<div style="font-weight:bold;margin-bottom:4px">${time}</div>`
            params.forEach((p: any) => {
              const value = p.data?.[1]
              const formatted = typeof value === 'number' ? value.toFixed(4) : value
              html += `<div style="display:flex;align-items:center;gap:4px">
                <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color}"></span>
                ${p.seriesName}: ${formatted}
              </div>`
            })
            return html
          }
        },
        legend: {
          data: seriesData.map((s) => s.name),
          bottom: 0
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '12%',
          top: '8%',
          containLabel: true
        },
        xAxis: {
          type: 'time',
          axisLabel: {
            formatter: (value: number) => {
              const d = new Date(value)
              return d.toLocaleTimeString()
            }
          }
        },
        yAxis: {
          type: 'value',
          axisLabel: {
            formatter: (value: number) => {
              if (Math.abs(value) >= 1_000_000) return (value / 1_000_000).toFixed(1) + 'M'
              if (Math.abs(value) >= 1_000) return (value / 1_000).toFixed(1) + 'K'
              return value.toFixed(2)
            }
          }
        },
        series: seriesData.map((s) => ({
          name: s.name,
          type: 'line',
          data: s.data,
          showSymbol: false,
          smooth: true,
          animation: false,
          sampling: 'lttb'
        }))
      }
    }

    const initChart = () => {
      if (!chartRef.value) return
      if (chartInstance) {
        chartInstance.dispose()
      }
      chartInstance = echarts.init(chartRef.value)
      chartInstance.setOption(getChartOption(), true)
    }

    const updateChart = () => {
      if (!chartInstance) return
      chartInstance.setOption(getChartOption(), true)
    }

    watch(
      () => props.series,
      () => updateChart(),
      { deep: true }
    )

    onMounted(() => {
      initChart()
      if (chartRef.value) {
        resizeObserver = new ResizeObserver(() => {
          chartInstance?.resize()
        })
        resizeObserver.observe(chartRef.value)
      }
    })

    onUnmounted(() => {
      resizeObserver?.disconnect()
      chartInstance?.dispose()
      chartInstance = null
    })

    return () => (
      <div ref={chartRef} style={{ width: '100%', height: props.height }} />
    )
  }
})