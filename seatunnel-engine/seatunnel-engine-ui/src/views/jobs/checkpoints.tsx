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

import { defineComponent, onMounted, ref } from 'vue'
import { NAlert, NButton, NDataTable, NSpace, NTag, type DataTableColumns } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useRouter } from 'vue-router'
import { JobsService } from '@/service/job'
import type {
  CheckpointHistoryRecord,
  CheckpointInfo,
  CheckpointOverview,
  CheckpointPipeline
} from '@/service/job/types'

export default defineComponent({
  props: {
    jobId: {
      type: String,
      required: true
    }
  },
  setup(props) {
    const { t } = useI18n()
    const router = useRouter()
    const overview = ref<CheckpointOverview | null>(null)
    const history = ref<CheckpointHistoryRecord[]>([])
    const loading = ref(false)
    const error = ref('')

    const checkpointLabel = (checkpoint?: CheckpointInfo | null) => {
      if (!checkpoint) {
        return '-'
      }
      return `${checkpoint.checkpointType || '-'} #${checkpoint.checkpointId}`
    }

    const refresh = async () => {
      loading.value = true
      error.value = ''
      try {
        const [overviewResponse, historyResponse] = await Promise.all([
          JobsService.getCheckpointOverview(props.jobId),
          JobsService.getCheckpointHistory(props.jobId, { limit: 50 })
        ])
        overview.value = overviewResponse
        history.value = historyResponse || []
      } catch (e) {
        error.value = t('detail.checkpoints.loadFailed')
      } finally {
        loading.value = false
      }
    }

    onMounted(refresh)

    const restoreLatestState = () => {
      router.push({
        name: 'jobs',
        query: {
          restoreJobId: props.jobId
        }
      })
    }

    const pipelineColumns: DataTableColumns<CheckpointPipeline> = [
      { title: t('detail.checkpoints.pipeline'), key: 'pipelineId' },
      {
        title: t('detail.checkpoints.triggered'),
        key: 'triggered',
        render: (row) => row.counts?.triggered ?? 0
      },
      {
        title: t('detail.checkpoints.completed'),
        key: 'completed',
        render: (row) => row.counts?.completed ?? 0
      },
      {
        title: t('detail.checkpoints.failed'),
        key: 'failed',
        render: (row) => row.counts?.failed ?? 0
      },
      {
        title: t('detail.checkpoints.inProgress'),
        key: 'inProgress',
        render: (row) => row.counts?.inProgress ?? 0
      },
      {
        title: t('detail.checkpoints.restored'),
        key: 'restored',
        render: (row) => row.counts?.restored ?? 0
      },
      {
        title: t('detail.checkpoints.latestCompleted'),
        key: 'latestCompleted',
        render: (row) => checkpointLabel(row.latestCompleted)
      },
      {
        title: t('detail.checkpoints.latestSavepoint'),
        key: 'latestSavepoint',
        render: (row) => checkpointLabel(row.latestSavepoint)
      },
      {
        title: t('detail.checkpoints.action'),
        key: 'actions',
        render: () => (
          <NButton size="small" tertiary type="primary" onClick={restoreLatestState}>
            {t('detail.checkpoints.restoreLatest')}
          </NButton>
        )
      }
    ]

    const historyColumns: DataTableColumns<CheckpointHistoryRecord> = [
      { title: t('detail.checkpoints.pipeline'), key: 'pipelineId' },
      {
        title: t('detail.checkpoints.checkpoint'),
        key: 'checkpoint',
        render: (row) => checkpointLabel(row.checkpoint)
      },
      {
        title: t('detail.checkpoints.status'),
        key: 'status',
        render: (row) => <NTag bordered={false}>{row.checkpoint?.status || '-'}</NTag>
      },
      {
        title: t('detail.checkpoints.duration'),
        key: 'durationMillis',
        render: (row) => row.checkpoint?.durationMillis ?? '-'
      },
      {
        title: t('detail.checkpoints.stateSize'),
        key: 'stateSize',
        render: (row) => row.checkpoint?.stateSize ?? '-'
      },
      {
        title: t('detail.checkpoints.failureReason'),
        key: 'failureReason',
        render: (row) => row.checkpoint?.failureReason || '-'
      }
    ]

    return () => (
      <NSpace vertical size="large">
        <NSpace justify="space-between" align="center">
          <span>
            {overview.value?.updatedAt
              ? t('detail.checkpoints.updatedAt', { time: overview.value.updatedAt })
              : t('detail.checkpoints.noSnapshot')}
          </span>
          <NButton loading={loading.value} onClick={refresh}>
            {t('detail.checkpoints.refresh')}
          </NButton>
        </NSpace>
        {error.value && <NAlert type="error">{error.value}</NAlert>}
        <NDataTable
          columns={pipelineColumns}
          data={overview.value?.pipelines || []}
          loading={loading.value}
          pagination={false}
          bordered={false}
        />
        <NDataTable
          columns={historyColumns}
          data={history.value}
          loading={loading.value}
          pagination={{ pageSize: 10 }}
          bordered={false}
        />
      </NSpace>
    )
  }
})
