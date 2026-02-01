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

import { defineComponent, ref } from 'vue'
import {
  NButton,
  NCard,
  NDataTable,
  NDatePicker,
  NInput,
  NInputNumber,
  NLayout,
  NLayoutContent,
  NModal,
  NSpace,
  NTag,
  type DataTableColumns
} from 'naive-ui'
import { TraceService } from '@/service/trace'
import type { TraceDetail, TraceEntry, TraceSummary } from '@/service/trace/types'

const formatMs = (ms?: number) => {
  if (!ms) return '-'
  const d = new Date(ms)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(
    d.getMinutes()
  )}:${pad(d.getSeconds())}`
}

const stageName = (stage: number) => {
  switch (stage) {
    case 1:
      return 'SOURCE_EMIT'
    case 2:
      return 'QUEUE_IN'
    case 3:
      return 'QUEUE_OUT'
    case 4:
      return 'TRANSFORM_IN'
    case 5:
      return 'TRANSFORM_OUT'
    case 6:
      return 'SINK_WRITE_DONE'
    default:
      return String(stage)
  }
}

export default defineComponent({
  name: 'TraceView',
  setup() {
    const jobId = ref('')
    const tableId = ref('')
    const limit = ref(100)
    const offset = ref(0)
    const timeRange = ref<[number, number] | null>(null)

    const loading = ref(false)
    const error = ref('')
    const items = ref<TraceSummary[]>([])

    const detailOpen = ref(false)
    const detailLoading = ref(false)
    const detail = ref<TraceDetail | null>(null)

    const fetchList = async () => {
      loading.value = true
      error.value = ''
      try {
        const fromMs = timeRange.value?.[0]
        const toMs = timeRange.value?.[1]
        const res = await TraceService.listTraces({
          jobId: jobId.value || undefined,
          tableId: tableId.value || undefined,
          fromMs,
          toMs,
          limit: limit.value,
          offset: offset.value
        })
        items.value = res.items || []
      } catch (e: any) {
        error.value = e?.message || 'failed'
        items.value = []
      } finally {
        loading.value = false
      }
    }

    const openDetail = async (row: TraceSummary) => {
      detailOpen.value = true
      detailLoading.value = true
      detail.value = null
      try {
        const res = await TraceService.getTrace(String(row.traceId), {
          sinkTaskId: String(row.sinkTaskId)
        })
        detail.value = res
      } catch (e) {
        detail.value = null
      } finally {
        detailLoading.value = false
      }
    }

    const traceColumns: DataTableColumns<TraceSummary> = [
      { title: 'TraceId', key: 'traceId' },
      { title: 'JobId', key: 'jobId' },
      { title: 'TableId', key: 'tableId' },
      { title: 'SinkTaskId', key: 'sinkTaskId' },
      {
        title: 'ReceivedAt',
        key: 'receivedTimeMs',
        render: (row) => formatMs(row.receivedTimeMs)
      },
      {
        title: 'Entries',
        key: 'entryCount',
        render: (row) => <NTag size="small">{row.entryCount}</NTag>
      },
      {
        title: 'Action',
        key: 'action',
        render: (row) => (
          <NButton size="small" tertiary onClick={() => openDetail(row)}>
            View
          </NButton>
        )
      }
    ]

    const entryColumns: DataTableColumns<TraceEntry> = [
      { title: '#', key: 'index', width: 60 },
      {
        title: 'Stage',
        key: 'stage',
        render: (row) => stageName(row.stage)
      },
      { title: 'TaskId', key: 'taskId' },
      {
        title: 'Ts',
        key: 'tsMs',
        render: (row) => formatMs(row.tsMs)
      },
      { title: 'Worker', key: 'workerAddress' },
      { title: 'TaskGroup', key: 'taskGroupName' }
    ]

    return () => (
      <NLayout>
        <NLayoutContent>
          <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
            <h2 class="font-bold text-2xl pb-4">Trace</h2>
            <NSpace align="center" wrap>
              <NInput
                value={jobId.value}
                onUpdateValue={(v) => (jobId.value = v)}
                placeholder="jobId"
                style="width: 240px"
              />
              <NInput
                value={tableId.value}
                onUpdateValue={(v) => (tableId.value = v)}
                placeholder="tableId"
                style="width: 240px"
              />
              <NDatePicker
                type="datetimerange"
                value={timeRange.value as any}
                onUpdateValue={(v) => (timeRange.value = v as any)}
                clearable
              />
              <NInputNumber
                value={limit.value}
                onUpdateValue={(v) => (limit.value = v || 100)}
                min={1}
                max={2000}
                style="width: 120px"
              />
              <NInputNumber
                value={offset.value}
                onUpdateValue={(v) => (offset.value = v || 0)}
                min={0}
                style="width: 120px"
              />
              <NButton type="primary" loading={loading.value} onClick={fetchList}>
                Search
              </NButton>
              {error.value && <span class="text-red-500">{error.value}</span>}
            </NSpace>
            <div class="pt-4">
              <NDataTable
                columns={traceColumns}
                data={items.value}
                loading={loading.value}
                bordered={false}
              />
            </div>
          </div>

          <NModal show={detailOpen.value} onUpdateShow={(v) => (detailOpen.value = v)}>
            <NCard
              style="width: 1100px"
              title="Trace Detail"
              bordered={false}
              closable
              onClose={() => (detailOpen.value = false)}
            >
              {detailLoading.value && <div>Loading...</div>}
              {!detailLoading.value && detail.value?.trace && (
                <div class="pb-4">
                  <NSpace>
                    <div>traceId: {detail.value.trace.traceId}</div>
                    <div>jobId: {detail.value.trace.jobId}</div>
                    <div>tableId: {detail.value.trace.tableId}</div>
                    <div>sinkTaskId: {detail.value.trace.sinkTaskId}</div>
                  </NSpace>
                  <NSpace class="pt-2">
                    <div>receivedAt: {formatMs(detail.value.trace.receivedTimeMs)}</div>
                    <div>startTs: {formatMs(detail.value.trace.startTsMs)}</div>
                  </NSpace>
                </div>
              )}
              {!detailLoading.value && (
                <NDataTable
                  columns={entryColumns}
                  data={detail.value?.entries || []}
                  bordered={false}
                />
              )}
            </NCard>
          </NModal>
        </NLayoutContent>
      </NLayout>
    )
  }
})

