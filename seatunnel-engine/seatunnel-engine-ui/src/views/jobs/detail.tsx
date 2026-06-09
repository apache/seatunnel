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

import {
  NTabs,
  NTabPane,
  NDivider,
  NTag,
  NDataTable,
  type DataTableColumns,
  NDrawer,
  NDrawerContent
} from 'naive-ui'
import {computed, defineComponent, onUnmounted, reactive, ref, watch} from 'vue'
import { getJobInfo } from '@/service/job'
import { useRoute } from 'vue-router'
import type { Job, Vertex } from '@/service/job/types'
import { useI18n } from 'vue-i18n'
import { getRemainTime } from '@/utils/time'
import { parse } from 'date-fns'
import DAG, { type DagEdgeInfo } from '@/components/directed-acyclic-graph'
import { getColorFromStatus } from '@/utils/getTypeFromStatus'
import './detail.scss'
import Configuration from '@/components/configuration'
import JobLog from '@/components/job-log'
import { formatPercentFromRatio } from '@/utils/format'
import {
  getRealtimeJobEdges,
  getRealtimeJobVertices,
  type RealtimeEdgesResponse,
  type RealtimeEdgePoint,
  type RealtimeVerticesResponse,
  type RealtimeVertexPoint
} from '@/service/realtime-metrics'
import { readVertexMetricValue, collectVertexMetrics } from './detail-metrics'

export default defineComponent({
  setup() {
    const { t } = useI18n()
    const route = useRoute()

    const jobId = route.params.jobId as string
    const job = reactive({} as Job)
    const duration = ref('')
    let timer: NodeJS.Timeout
    let fetchTimer: NodeJS.Timeout
    const fetch = async () => {
      const res = await getJobInfo(jobId)
      Object.assign(job, res)
      clearInterval(timer)
      const d = parse(res.createTime, 'yyyy-MM-dd HH:mm:ss', new Date())
      duration.value = getRemainTime(Math.abs(Date.now() - d.getTime()))
      if (isTerminalState(job.jobStatus)) {
        clearTimeout(fetchTimer)
        return
      }
      fetchTimer = setTimeout(fetch, 5000)
      if (isRunningState(job.jobStatus)) {
        timer = setInterval(() => {
          duration.value = getRemainTime(Math.abs(Date.now() - d.getTime()))
        }, 1000)
      }
    }

    fetch()

    const select = ref('Overview')
    const change = () => {
      console.log(select.value)
    }
    watch(() => select.value, change)

    // Clear the timer when the component is uninstalled
    onUnmounted(() => {
      clearInterval(timer)
      clearTimeout(fetchTimer)
      clearInterval(realtimeTimer)
    })

    const isTerminalState = (status: string) => {
      return ['FINISHED', 'FAILED', 'CANCELED','SAVEPOINT_DONE'].includes(status)
    }

    const isRunningState = (status: string) => {
      return status === 'RUNNING'
    }

    const tableData = computed(() => {
      return job.jobDag?.vertexInfoMap?.filter((v) => v.type !== 'transform') || []
    })
    const sourceCell = (
      row: Vertex,
      key:
        | 'TableSourceReceivedBytes'
        | 'TableSourceReceivedCount'
        | 'TableSourceReceivedQPS'
        | 'TableSourceReceivedBytesPerSeconds'
    ) => {
      if (row.type === 'source') {
        return row.tablePaths.reduce(
          (s, path) => s + readVertexMetricValue(job.metrics?.[key], row, path),
          0
        )
      }
      return 0
    }

    const sinkCell = (
      row: Vertex,
      key:
        | 'TableSinkWriteBytes'
        | 'TableSinkWriteCount'
        | 'TableSinkWriteQPS'
        | 'TableSinkWriteBytesPerSeconds'
    ) => {
      if (row.type === 'sink') {
        return row.tablePaths.reduce(
          (s, path) => s + readVertexMetricValue(job.metrics?.[key], row, path),
          0
        )
      }
      return 0
    }

    const columns: DataTableColumns<Vertex> = [
      {
        title: t('detail.table.name'),
        key: 'vertexName'
      },
      {
        title: t('detail.table.receivedBytes'),
        key: 'key',
        render: (row) => sourceCell(row, 'TableSourceReceivedBytes')
      },
      {
        title: t('detail.table.writeBytes'),
        key: 'key',
        render: (row) => sinkCell(row, 'TableSinkWriteBytes')
      },
      {
        title: t('detail.table.receivedCount'),
        key: 'key',
        render: (row) => sourceCell(row, 'TableSourceReceivedCount')
      },
      {
        title: t('detail.table.writeCount'),
        key: 'key',
        render: (row) => sinkCell(row, 'TableSinkWriteCount')
      },
      {
        title: t('detail.table.receivedQps'),
        key: 'key',
        render: (row) => sourceCell(row, 'TableSourceReceivedQPS')
      },
      {
        title: t('detail.table.writeQps'),
        key: 'key',
        render: (row) => sinkCell(row, 'TableSinkWriteQPS')
      },
      {
        title: t('detail.table.receivedBytesPerSecond'),
        key: 'key',
        render: (row) => sourceCell(row, 'TableSourceReceivedBytesPerSeconds')
      },
      {
        title: t('detail.table.writeBytesPerSecond'),
        key: 'key',
        render: (row) => sinkCell(row, 'TableSinkWriteBytesPerSeconds')
      }
    ]

    const focusedId = ref(0)
    const focusedEdge = ref<DagEdgeInfo>()
    const drawerShow = ref(false)
    const onFocus = (vertex?: Vertex) => {
      focusedEdge.value = undefined
      if (vertex) {
        drawerShow.value = true
        focusedId.value = vertex.vertexId
      } else {
        drawerShow.value = false
        focusedId.value = 0
      }
    }
    const onEdgeFocus = (edge?: DagEdgeInfo) => {
      focusedId.value = 0
      if (edge) {
        focusedEdge.value = edge
        drawerShow.value = true
      } else {
        focusedEdge.value = undefined
        if (!focusedId.value) {
          drawerShow.value = false
        }
      }
    }
    const onDrawerClose = () => {
      drawerShow.value = false
      focusedId.value = 0
      focusedEdge.value = undefined
    }

    const realtimeEdges = ref<RealtimeEdgesResponse>()
    const realtimeVertices = ref<RealtimeVerticesResponse>()
    const realtimeTick = ref(0)
    const realtimeWindowMs = 3 * 60 * 1000
    const realtimeWindowMsMax = 10 * 60 * 1000
    const realtimeError = ref<string | null>(null)
    const realtimeConsecutiveErrors = ref(0)
    let realtimeTimer: NodeJS.Timeout
    const fetchRealtime = async () => {
      if (!isRunningState(job.jobStatus)) return
      if (select.value !== 'Overview') return
      try {
        const [edges, vertices] = await Promise.all([
          getRealtimeJobEdges(jobId, Math.min(realtimeWindowMs, realtimeWindowMsMax)),
          getRealtimeJobVertices(jobId, Math.min(realtimeWindowMs, realtimeWindowMsMax))
        ])
        realtimeEdges.value = edges
        realtimeVertices.value = vertices
        realtimeTick.value++
        realtimeError.value = null
        realtimeConsecutiveErrors.value = 0
      } catch (e) {
        realtimeConsecutiveErrors.value++
        const status = (e as any)?.response?.status
        if (status === 503) {
          realtimeError.value = 'Realtime metrics disabled on master'
        } else if (status === 404) {
          realtimeError.value = 'Realtime metrics job not found'
        } else if (status === 401 || status === 403) {
          realtimeError.value = 'Realtime metrics unauthorized'
        } else {
          realtimeError.value = 'Failed to fetch realtime metrics'
        }
        if (realtimeConsecutiveErrors.value === 1) {
          console.warn('Fetch realtime metrics failed:', e)
        }
      }
    }
    const startRealtimePolling = () => {
      clearInterval(realtimeTimer)
      fetchRealtime()
      realtimeTimer = setInterval(fetchRealtime, 2000)
    }
    const stopRealtimePolling = () => {
      clearInterval(realtimeTimer)
    }
    watch(
      () => [job.jobStatus, select.value],
      () => {
        if (isRunningState(job.jobStatus) && select.value === 'Overview') {
          startRealtimePolling()
        } else {
          stopRealtimePolling()
        }
      }
    )

    const realtimeEdgeStats = computed<Record<number, RealtimeEdgePoint>>(() => {
      const stats: Record<number, RealtimeEdgePoint> = {}
      const edges = realtimeEdges.value?.edges || []
      const decodeTargetVertexId = (queueId: number) => {
        if (queueId >= 0) return queueId
        const abs = Math.abs(queueId)
        if (!abs) return undefined
        if (abs % 2 === 0) return abs / 2
        return (abs - 1) / 2
      }
      edges.forEach((e) => {
        const last = e.points?.[e.points.length - 1]
        if (!last) return
        const key = e.targetVertexId ?? decodeTargetVertexId(e.queueId)
        if (key === undefined) return
        stats[key] = last
      })
      return stats
    })

    const realtimeVertexStats = computed<Record<number, RealtimeVertexPoint>>(() => {
      const stats: Record<number, RealtimeVertexPoint> = {}
      const vertices = realtimeVertices.value?.vertices || []
      vertices.forEach((v) => {
        const last = v.points?.[v.points.length - 1]
        if (last) stats[v.vertexId] = last
      })
      return stats
    })

    const realtimeSeriesLimit = computed(() => {
      const bucketMs = realtimeEdges.value?.bucketMs || realtimeVertices.value?.bucketMs || 5000
      const effectiveWindowMs = Math.min(realtimeWindowMs, realtimeWindowMsMax)
      const safeBucketMs = Math.max(1, bucketMs)
      return Math.max(1, Math.ceil(effectiveWindowMs / safeBucketMs) + 1)
    })

    const focusedEdgeSeries = computed(() => {
      const targetId = focusedEdge.value?.targetVertexId
      if (!targetId) return []
      const decodeTargetVertexId = (queueId: number) => {
        if (queueId >= 0) return queueId
        const abs = Math.abs(queueId)
        if (!abs) return undefined
        if (abs % 2 === 0) return abs / 2
        return (abs - 1) / 2
      }
      const points =
        realtimeEdges.value?.edges?.find(
          (e) => (e.targetVertexId ?? decodeTargetVertexId(e.queueId)) === targetId
        )?.points || []
      return points
        .slice(-realtimeSeriesLimit.value)
        .slice()
        .sort((a, b) => b.ts - a.ts)
    })

    const focusedVertexSeries = computed(() => {
      const vertexId = focusedId.value
      if (!vertexId) return []
      const points =
        realtimeVertices.value?.vertices?.find((v) => v.vertexId === vertexId)?.points || []
      return points
        .slice(-realtimeSeriesLimit.value)
        .slice()
        .sort((a, b) => b.ts - a.ts)
    })

    const focusedVertex = computed(() => {
      const vertex = job.jobDag?.vertexInfoMap?.find((v) => v.vertexId === focusedId.value)
      const metrics = {} as any
      if (vertex?.type === 'source') {
        Object.assign(
          metrics,
          collectVertexMetrics(
            'TableSourceReceivedBytes',
            job.metrics?.TableSourceReceivedBytes,
            vertex
          ),
          collectVertexMetrics(
            'TableSourceReceivedCount',
            job.metrics?.TableSourceReceivedCount,
            vertex
          ),
          collectVertexMetrics(
            'TableSourceReceivedQPS',
            job.metrics?.TableSourceReceivedQPS,
            vertex
          ),
          collectVertexMetrics(
            'TableSourceReceivedBytesPerSeconds',
            job.metrics?.TableSourceReceivedBytesPerSeconds,
            vertex
          )
        )
      }
      if (vertex?.type === 'sink') {
        Object.assign(
          metrics,
          collectVertexMetrics('TableSinkWriteBytes', job.metrics?.TableSinkWriteBytes, vertex),
          collectVertexMetrics('TableSinkWriteCount', job.metrics?.TableSinkWriteCount, vertex),
          collectVertexMetrics('TableSinkWriteQPS', job.metrics?.TableSinkWriteQPS, vertex),
          collectVertexMetrics(
            'TableSinkWriteBytesPerSeconds',
            job.metrics?.TableSinkWriteBytesPerSeconds,
            vertex
          )
        )
      }
      const realtime = realtimeVertexStats.value[focusedId.value]
      if (realtime) {
        if (vertex?.type === 'source') {
          metrics['observability.sourceReadRatio'] = realtime.sourceReadRatio
          metrics['observability.sourceIdleRatio'] = realtime.sourceIdleRatio
        }
        if (vertex?.type === 'transform') {
          metrics['observability.transformBusyRatio'] = realtime.transformBusyRatio
          metrics['observability.transformProcessMsPerRecord'] =
            realtime.transformProcessNsPerRecord / 1_000_000
          metrics['observability.transformRecordsIn'] = realtime.transformRecordsIn
          metrics['observability.transformRecordsOut'] = realtime.transformRecordsOut
        }
        if (vertex?.type === 'sink') {
          metrics['observability.sinkBusyRatio'] = realtime.sinkBusyRatio
          metrics['observability.sinkWriteMsPerRecord'] = realtime.sinkWriteNsPerRecord / 1_000_000
          metrics['observability.sinkRecordsIn'] = realtime.sinkRecordsIn
        }
      }
      return Object.assign({}, vertex, metrics)
    })

    const focusedEdgeInfo = computed(() => {
      const edge = focusedEdge.value
      if (!edge) return undefined
      const input = job.jobDag?.vertexInfoMap?.find((v) => v.vertexId === edge.inputVertexId)
      const target = job.jobDag?.vertexInfoMap?.find((v) => v.vertexId === edge.targetVertexId)
      const m = edge.metrics || realtimeEdgeStats.value[edge.targetVertexId]
      return {
        'edge.id': edge.edgeId,
        'edge.pipelineId': edge.pipelineId,
        'edge.from': input?.vertexName || edge.inputVertexId,
        'edge.to': target?.vertexName || edge.targetVertexId,
        'edge.bpRatio': m?.bpRatio ?? 0,
        'edge.queueFillRatio': m?.queueFillRatio ?? 0,
        'edge.queueSize': m?.queueSize ?? 0,
        'edge.queueCapacity': m?.queueCapacity ?? 0,
        'edge.emitBlockedNs': m?.emitBlockedNs ?? 0
      }
    })
    const rowClassName = (row: Vertex) => {
      if (row.vertexId === focusedId.value) {
        return 'focused-row'
      }
      return ''
    }
    const rowProps = (row: Vertex) => {
      return { onClick: () => onFocus(row) }
    }

    const edgePointColumns: DataTableColumns<RealtimeEdgePoint> = [
      {
        title: t('detail.observability.time'),
        key: 'ts',
        render: (row) => new Date(row.ts).toLocaleTimeString()
      },
      {
        title: t('detail.observability.bpRatio'),
        key: 'bpRatio',
        render: (row) => formatPercentFromRatio(row.bpRatio)
      },
      {
        title: t('detail.observability.queueFillRatio'),
        key: 'queueFillRatio',
        render: (row) => formatPercentFromRatio(row.queueFillRatio)
      }
    ]

    const vertexPointColumns = computed<DataTableColumns<RealtimeVertexPoint>>(() => {
      const base: DataTableColumns<RealtimeVertexPoint> = [
        {
          title: t('detail.observability.time'),
          key: 'ts',
          render: (row) => new Date(row.ts).toLocaleTimeString()
        }
      ]
      const v = focusedVertex.value as any
      const type = v?.type
      if (type === 'source') {
        return base.concat([
          {
            title: t('detail.observability.sourceReadRatio'),
            key: 'sourceReadRatio',
            render: (row) => formatPercentFromRatio(row.sourceReadRatio)
          },
          {
            title: t('detail.observability.sourceIdleRatio'),
            key: 'sourceIdleRatio',
            render: (row) => formatPercentFromRatio(row.sourceIdleRatio)
          }
        ])
      }
      if (type === 'transform') {
        return base.concat([
          {
            title: t('detail.observability.transformBusyRatio'),
            key: 'transformBusyRatio',
            render: (row) => formatPercentFromRatio(row.transformBusyRatio)
          },
          {
            title: t('detail.observability.processMsPerRecord'),
            key: 'transformProcessNsPerRecord',
            render: (row) => (row.transformProcessNsPerRecord / 1_000_000).toFixed(3)
          },
          {
            title: t('detail.observability.recordsIn'),
            key: 'transformRecordsIn'
          },
          {
            title: t('detail.observability.recordsOut'),
            key: 'transformRecordsOut'
          }
        ])
      }
      if (type === 'sink') {
        return base.concat([
          {
            title: t('detail.observability.sinkBusyRatio'),
            key: 'sinkBusyRatio',
            render: (row) => formatPercentFromRatio(row.sinkBusyRatio)
          },
          {
            title: t('detail.observability.writeMsPerRecord'),
            key: 'sinkWriteNsPerRecord',
            render: (row) => (row.sinkWriteNsPerRecord / 1_000_000).toFixed(3)
          },
          {
            title: t('detail.observability.recordsIn'),
            key: 'sinkRecordsIn'
          }
        ])
      }
      // Fallback: show a minimal common view.
      return base
    })
    return () => (
      <div class="w-full bg-white px-12 pt-6 pb-12 border border-gray-100 rounded-xl">
	        <div class="font-bold text-xl">
	          {job.jobName}
	          <NTag bordered={false} color={getColorFromStatus(job.jobStatus)} class="ml-3">
	            {job.jobStatus}
	          </NTag>
	          {realtimeError.value ? (
	            <span title={realtimeError.value}>
	              <NTag bordered={false} type="warning" class="ml-3">
	                Realtime metrics unavailable
	              </NTag>
	            </span>
	          ) : null}
	        </div>
        <div class="mt-3 flex items-center gap-3">
          <span>{t('detail.id')}:</span>
          <span class="font-bold">{job.jobId}</span>
          <NDivider vertical />
          <span>{t('detail.createTime')}:</span>
          <span class="font-bold">{job.createTime}</span>
          <NDivider vertical />
          <span>{t('detail.duration')}:</span>
          <span class="font-bold">{duration.value}</span>
        </div>
        <div class="tab-wrap relative">
          <NTabs v-model:value={select.value} type="line" animated>
            <NTabPane name="Overview" tab={t('detail.tabs.overview')}>
              <DAG
                job={job}
                focusedId={focusedId.value}
                onNodeClick={onFocus}
                onEdgeClick={onEdgeFocus}
                realtimeEdgeStats={realtimeEdgeStats.value}
                realtimeVertexStats={realtimeVertexStats.value}
                realtimeTick={realtimeTick.value}
              />
              <NDataTable
                columns={columns}
                data={tableData.value}
                pagination={false}
                scrollX="auto"
                bordered
                rowClassName={rowClassName}
                rowProps={rowProps}
              />
            </NTabPane>
            <NTabPane name="Exception" tab={t('detail.tabs.exception')}>
              <pre style="white-space: pre-wrap; word-wrap: break-word; background-color: #f5f5f5; padding: 12px; border-radius: 4px; overflow: auto; max-height: 600px; font-family: monospace; line-height: 1.5;">
                {job.errorMsg}
              </pre>
            </NTabPane>
            <NTabPane name="Configuration" tab={t('detail.tabs.configuration')}>
              <Configuration data={job.envOptions || job.jobDag.envOptions}></Configuration>
            </NTabPane>
            <NTabPane name="Log" tab={t('detail.tabs.log')}>
              <JobLog jobId={job.jobId}></JobLog>
            </NTabPane>
          </NTabs>
          <NDrawer
            show={select.value === 'Overview' && drawerShow.value}
            showMask={false}
            width={'40%'}
            to=".tab-wrap"
            style="top:42px"
            closeOnEsc={false}
            mask-closable={false}
            onUpdateShow={onDrawerClose}
          >
            {focusedEdge.value ? (
              <NDrawerContent title={focusedEdgeInfo.value?.['edge.id']} closable>
                <Configuration data={focusedEdgeInfo.value}></Configuration>
                <NDivider />
                <NDataTable
                  columns={edgePointColumns}
                  data={focusedEdgeSeries.value}
                  pagination={false}
                  bordered
                />
              </NDrawerContent>
            ) : (
              <NDrawerContent title={focusedVertex.value?.vertexName} closable>
                <Configuration data={focusedVertex.value}></Configuration>
                <NDivider />
                <NDataTable
                  columns={vertexPointColumns.value}
                  data={focusedVertexSeries.value}
                  pagination={false}
                  bordered
                />
              </NDrawerContent>
            )}
          </NDrawer>
        </div>
      </div>
    )
  }
})
