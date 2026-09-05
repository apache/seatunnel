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
  NDrawerContent,
  NButton,
  NSpace,
  useMessage
} from 'naive-ui'
import { computed, defineComponent, onUnmounted, reactive, ref, watch } from 'vue'
import { getJobInfo } from '@/service/job'
import { useRoute } from 'vue-router'
import type { Job, Vertex } from '@/service/job/types'
import { useI18n } from 'vue-i18n'
import { getRemainTime } from '@/utils/time'
import { parse } from 'date-fns'
import DAG, { type DagEdgeInfo } from '@/components/directed-acyclic-graph'
import LiveMetricsBoard from '@/components/live-metrics-chart/board'
import { getColorFromStatus } from '@/utils/getTypeFromStatus'
import './detail.scss'
import Configuration from '@/components/configuration'
import JobLog from '@/components/job-log'
import {
  REALTIME_POLL_INTERVAL_MS,
  REALTIME_WINDOW_MS_DEFAULT,
  describeRealtimeFetchError,
  effectiveRealtimeWindowMs,
  fetchJobRealtimeMetrics,
  type RealtimeEdgesResponse,
  type RealtimeEdgePoint,
  type RealtimeVerticesResponse,
  type RealtimeVertexPoint
} from '@/service/realtime-metrics'
import {
  readVertexMetricValue,
  collectVertexMetrics,
  extractVertexIdentifier
} from './detail-metrics'
import {
  LIVE_METRICS_PIN_LIMIT,
  useLiveMetricsPinStore,
  type PinnedMetricRef
} from '@/store/live-metrics-pin'
import {
  buildSeriesFromEdgePoints,
  buildSeriesFromVertexPoints,
  decodeTargetVertexId,
  edgePinFields,
  edgeSeriesId,
  resolvePinnedSeries,
  shortOperatorLabel,
  vertexPinFields,
  vertexSeriesId
} from './detail-live-metrics'

export default defineComponent({
  setup() {
    const { t } = useI18n()
    const route = useRoute()
    const message = useMessage()
    const pinStore = useLiveMetricsPinStore()

    const jobId = route.params.jobId as string
    pinStore.ensureJob(jobId)
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
        pinStore.clear()
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
      pinStore.clear()
    })

    const isTerminalState = (status: string) => {
      return ['FINISHED', 'FAILED', 'CANCELED', 'SAVEPOINT_DONE'].includes(status)
    }

    const isRunningState = (status: string) => {
      return status === 'RUNNING'
    }

    const tableData = computed(() => {
      return job.jobDag?.vertexInfoMap?.filter((v) => v.type !== 'transform') || []
    })
    const formatNumber = (val: number): number => {
      if (Number.isInteger(val)) return val
      return Math.round(val * 100) / 100
    }
    const sourceCell = (
      row: Vertex,
      key:
        | 'TableSourceReceivedBytes'
        | 'TableSourceReceivedCount'
        | 'TableSourceReceivedQPS'
        | 'TableSourceReceivedBytesPerSeconds'
    ) => {
      if (row.type === 'source') {
        const val = row.tablePaths.reduce(
          (s, path) => s + readVertexMetricValue(job.metrics?.[key], row, path),
          0
        )
        return formatNumber(val)
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
        const val = row.tablePaths.reduce(
          (s, path) => s + readVertexMetricValue(job.metrics?.[key], row, path),
          0
        )
        return formatNumber(val)
      }
      return 0
    }
    const flushSignalQpsCell = (row: Vertex) => {
      const vertexId = extractVertexIdentifier(row.vertexName) || row.vertexName
      if (row.type === 'source') {
        return formatNumber(Number(job.metrics?.FlushSignalQPSPerVertex?.[vertexId]) || 0)
      }
      if (row.type === 'sink') {
        return formatNumber(Number(job.metrics?.FlushSignalSinkQPSPerVertex?.[vertexId]) || 0)
      }
      return '--'
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
      },
      {
        title: 'Flush Signal QPS',
        key: 'key',
        render: (row) => flushSignalQpsCell(row)
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
    const realtimeWindowMs = REALTIME_WINDOW_MS_DEFAULT
    const realtimeError = ref<string | null>(null)
    const realtimeConsecutiveErrors = ref(0)
    let realtimeTimer: NodeJS.Timeout
    const fetchRealtime = async () => {
      if (!isRunningState(job.jobStatus)) return
      if (select.value !== 'Overview') return
      try {
        const { edges, vertices } = await fetchJobRealtimeMetrics(jobId, realtimeWindowMs)
        realtimeEdges.value = edges
        realtimeVertices.value = vertices
        realtimeTick.value++
        realtimeError.value = null
        realtimeConsecutiveErrors.value = 0
      } catch (e) {
        realtimeConsecutiveErrors.value++
        realtimeError.value = describeRealtimeFetchError(e)
        if (realtimeConsecutiveErrors.value === 1) {
          console.warn('Fetch realtime metrics failed:', e)
        }
      }
    }
    const startRealtimePolling = () => {
      clearInterval(realtimeTimer)
      fetchRealtime()
      realtimeTimer = setInterval(fetchRealtime, REALTIME_POLL_INTERVAL_MS)
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
      const effectiveWindowMs = effectiveRealtimeWindowMs(realtimeWindowMs)
      const safeBucketMs = Math.max(1, bucketMs)
      return Math.max(1, Math.ceil(effectiveWindowMs / safeBucketMs) + 1)
    })

    const vertexNameById = computed(() => {
      const map: Record<number, string> = {}
      job.jobDag?.vertexInfoMap?.forEach((v) => {
        map[v.vertexId] = v.vertexName
      })
      return map
    })

    const pinnedSeries = computed(() => {
      // depend on realtimeTick so chart refreshes with poll
      void realtimeTick.value
      return resolvePinnedSeries(
        pinStore.pins,
        realtimeVertices.value,
        realtimeEdges.value,
        vertexNameById.value,
        realtimeSeriesLimit.value
      )
    })

    const drawerVertexChartSeries = computed(() => {
      const vertex = job.jobDag?.vertexInfoMap?.find((v) => v.vertexId === focusedId.value)
      if (!vertex) return []
      const points =
        realtimeVertices.value?.vertices?.find((v) => v.vertexId === vertex.vertexId)?.points || []
      return vertexPinFields(vertex.type, t).map((field) => {
        return buildSeriesFromVertexPoints(
          vertex.vertexId,
          vertex.vertexName,
          field,
          points,
          realtimeSeriesLimit.value
        )
      })
    })

    const drawerEdgeChartSeries = computed(() => {
      const edge = focusedEdge.value
      if (!edge) return []
      const input = job.jobDag?.vertexInfoMap?.find((v) => v.vertexId === edge.inputVertexId)
      const target = job.jobDag?.vertexInfoMap?.find((v) => v.vertexId === edge.targetVertexId)
      const label = `${shortOperatorLabel(input?.vertexName || String(edge.inputVertexId))} → ${shortOperatorLabel(target?.vertexName || String(edge.targetVertexId))}`
      const points =
        realtimeEdges.value?.edges?.find(
          (e) => (e.targetVertexId ?? decodeTargetVertexId(e.queueId)) === edge.targetVertexId
        )?.points || []
      return edgePinFields(t).map((field) => {
        return buildSeriesFromEdgePoints(
          edge.targetVertexId,
          label,
          field,
          points,
          realtimeSeriesLimit.value
        )
      })
    })

    const onTogglePin = (ref: PinnedMetricRef) => {
      const result = pinStore.toggle(ref)
      if (result === 'limit') {
        message.warning(t('detail.liveMetrics.pinLimit', { limit: LIVE_METRICS_PIN_LIMIT }))
      }
    }

    const renderPinControls = (
      kind: 'vertex' | 'edge',
      targetId: number,
      baseName: string,
      fields: ReturnType<typeof vertexPinFields>
    ) => (
      <NSpace class="mb-3" size="small" wrap>
        {fields.map((field) => {
          const id =
            kind === 'vertex'
              ? vertexSeriesId(targetId, field.field)
              : edgeSeriesId(targetId, field.field)
          const pinned = pinStore.isPinned(id)
          return (
            <NButton
              size="tiny"
              type={pinned ? 'primary' : 'default'}
              secondary={!pinned}
              onClick={() =>
                onTogglePin({
                  id,
                  name: `${shortOperatorLabel(baseName)} · ${field.label}`,
                  kind,
                  targetId,
                  field: field.field
                })
              }
            >
              {pinned ? t('detail.liveMetrics.unpin') : t('detail.liveMetrics.pin')} · {field.label}
            </NButton>
          )
        })}
      </NSpace>
    )

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
        const vertexId = extractVertexIdentifier(vertex.vertexName)
        if (vertexId) {
          if (job.metrics?.FlushSignalTotalPerVertex?.[vertexId]) {
            metrics[`FlushSignalTotal.${vertexId}`] =
              job.metrics.FlushSignalTotalPerVertex[vertexId]
          }
          if (job.metrics?.FlushSignalQueueSuccessTotalPerVertex?.[vertexId]) {
            metrics[`FlushSignalQueueSuccess.${vertexId}`] =
              job.metrics.FlushSignalQueueSuccessTotalPerVertex[vertexId]
          }
          if (job.metrics?.FlushSignalQueueFailureTotalPerVertex?.[vertexId]) {
            metrics[`FlushSignalQueueFailure.${vertexId}`] =
              job.metrics.FlushSignalQueueFailureTotalPerVertex[vertexId]
          }
        }
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
        const vertexId = extractVertexIdentifier(vertex.vertexName)
        if (vertexId) {
          if (job.metrics?.FlushSignalSinkSuccessTotalPerVertex?.[vertexId]) {
            metrics[`FlushSignalSinkSuccess.${vertexId}`] =
              job.metrics.FlushSignalSinkSuccessTotalPerVertex[vertexId]
          }
          if (job.metrics?.FlushSignalSinkFailureTotalPerVertex?.[vertexId]) {
            metrics[`FlushSignalSinkFailure.${vertexId}`] =
              job.metrics.FlushSignalSinkFailureTotalPerVertex[vertexId]
          }
        }
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
              <div class="mt-2 mb-2 border border-gray-100 rounded-lg px-3 pt-2 pb-2 bg-gray-50">
                <div class="flex items-baseline justify-between mb-2">
                  <div class="font-semibold text-base">{t('detail.liveMetrics.pinnedTitle')}</div>
                  <div class="text-xs text-gray-500">
                    {t('detail.liveMetrics.pinnedHint', { limit: LIVE_METRICS_PIN_LIMIT })}
                    {pinStore.pins.length
                      ? ` · ${pinStore.pins.length}/${LIVE_METRICS_PIN_LIMIT}`
                      : ''}
                  </div>
                </div>
                {pinStore.pins.length ? (
                  <NSpace class="mb-2" size="small" wrap>
                    {pinStore.pins.map((p) => (
                      <NTag key={p.id} closable type="info" onClose={() => pinStore.unpin(p.id)}>
                        {p.name}
                      </NTag>
                    ))}
                  </NSpace>
                ) : null}
                <LiveMetricsBoard
                  series={pinnedSeries.value}
                  windowMs={effectiveRealtimeWindowMs(realtimeWindowMs)}
                  emptyText={t('detail.liveMetrics.emptyPinned')}
                  height={140}
                  layout="row"
                  unitTitles={{
                    ratio: t('detail.liveMetrics.unitRatio'),
                    duration: t('detail.liveMetrics.unitDuration'),
                    count: t('detail.liveMetrics.unitCount')
                  }}
                />
              </div>
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
                {renderPinControls(
                  'edge',
                  focusedEdge.value.targetVertexId,
                  `${shortOperatorLabel(String(focusedEdgeInfo.value?.['edge.from']))} → ${shortOperatorLabel(String(focusedEdgeInfo.value?.['edge.to']))}`,
                  edgePinFields(t)
                )}
                <LiveMetricsBoard
                  series={drawerEdgeChartSeries.value}
                  windowMs={effectiveRealtimeWindowMs(realtimeWindowMs)}
                  emptyText={t('detail.liveMetrics.chartEmpty')}
                  height={180}
                  unitTitles={{
                    ratio: t('detail.liveMetrics.unitRatio'),
                    duration: t('detail.liveMetrics.unitDuration'),
                    count: t('detail.liveMetrics.unitCount')
                  }}
                />
              </NDrawerContent>
            ) : (
              <NDrawerContent title={focusedVertex.value?.vertexName} closable>
                <Configuration data={focusedVertex.value}></Configuration>
                <NDivider />
                {renderPinControls(
                  'vertex',
                  focusedId.value,
                  focusedVertex.value?.vertexName || String(focusedId.value),
                  vertexPinFields((focusedVertex.value as any)?.type, t)
                )}
                <LiveMetricsBoard
                  series={drawerVertexChartSeries.value}
                  windowMs={effectiveRealtimeWindowMs(realtimeWindowMs)}
                  emptyText={t('detail.liveMetrics.chartEmpty')}
                  height={180}
                  unitTitles={{
                    ratio: t('detail.liveMetrics.unitRatio'),
                    duration: t('detail.liveMetrics.unitDuration'),
                    count: t('detail.liveMetrics.unitCount')
                  }}
                />
              </NDrawerContent>
            )}
          </NDrawer>
        </div>
      </div>
    )
  }
})
