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

import { defineComponent, h, onUnmounted, ref } from 'vue'
import { NAlert, NButton, NDataTable, NPopconfirm, NSpace, NTag } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { JobsService } from '@/service/job'
import type { DataTableColumns } from 'naive-ui'
import type { Job } from '@/service/job/types'
import { useRouter } from 'vue-router'
import { getColorFromStatus } from '@/utils/getTypeFromStatus'

type FeedbackType = 'success' | 'error'

interface Feedback {
  type: FeedbackType
  message: string
}

export default defineComponent({
  setup() {
    const { t } = useI18n()

    const jobs = ref([] as Job[])
    const page = ref(1)
    const pageSize = ref(10)
    const total = ref(0)
    const actionLoading = ref('')
    const feedback = ref<Feedback | null>(null)

    let timer: ReturnType<typeof setTimeout> | undefined
    let fetchVersion = 0
    const fetch = async () => {
      if (timer) {
        clearTimeout(timer)
        timer = undefined
      }
      const currentVersion = ++fetchVersion
      try {
        const res = await JobsService.getRunningJobs(page.value, pageSize.value)
        if (currentVersion !== fetchVersion) {
          return
        }
        jobs.value = res.data || []
        total.value = res.total || 0
      } catch (error) {
        if (currentVersion === fetchVersion) {
          feedback.value = {
            type: 'error',
            message: t('jobs.actions.refreshFailed')
          }
        }
      } finally {
        if (currentVersion === fetchVersion) {
          timer = setTimeout(fetch, 5000)
        }
      }
    }
    onUnmounted(() => {
      fetchVersion++
      if (timer) {
        clearTimeout(timer)
      }
    })

    fetch()

    const router = useRouter()
    function createColumns(): DataTableColumns<Job> {
      const view = (job: Job) => {
        router.push({ name: 'detail', params: { jobId: job.jobId } })
      }

      const controlJob = async (job: Job, force: boolean, savepoint = false) => {
        const action = savepoint ? 'savepoint' : force ? 'cancel' : 'stop'
        actionLoading.value = `${job.jobId}-${action}`
        feedback.value = null
        try {
          await JobsService.stopJob({
            jobId: job.jobId,
            isStopWithSavePoint: savepoint,
            force
          })
          feedback.value = {
            type: 'success',
            message: t(
              savepoint
                ? 'jobs.actions.savepointSuccess'
                : force
                  ? 'jobs.actions.cancelSuccess'
                  : 'jobs.actions.stopSuccess',
              {
                job: job.jobName || job.jobId
              }
            )
          }
          await fetch()
        } catch (error) {
          feedback.value = {
            type: 'error',
            message: t(
              savepoint
                ? 'jobs.actions.savepointFailed'
                : force
                  ? 'jobs.actions.cancelFailed'
                  : 'jobs.actions.stopFailed',
              {
                job: job.jobName || job.jobId
              }
            )
          }
        } finally {
          actionLoading.value = ''
        }
      }

      return [
        {
          title: 'No',
          key: 'No',
          render: (row: Job, index: number) => h('div', index + 1)
        },
        {
          title: 'Id',
          key: 'jobId',
          sorter: 'default'
        },
        {
          title: 'Name',
          key: 'jobName',
          sorter: 'default'
        },
        {
          title: 'Create Time',
          key: 'createTime',
          sorter: 'default'
        },
        {
          title: 'Status',
          key: 'jobStatus',
          render(row) {
            return (
              <NTag bordered={false} color={getColorFromStatus(row.jobStatus)}>
                {row.jobStatus}
              </NTag>
            )
          }
        },
        {
          title: 'Action',
          key: 'actions',
          render(row) {
            return (
              <NSpace size="small">
                {h(
                  NButton,
                  {
                    strong: true,
                    tertiary: true,
                    size: 'small',
                    onClick: () => view(row)
                  },
                  { default: () => t('jobs.actions.view') }
                )}
                <NPopconfirm
                  positiveText={t('jobs.actions.confirm')}
                  negativeText={t('jobs.actions.cancelConfirm')}
                  onPositiveClick={() => controlJob(row, false)}
                >
                  {{
                    trigger: () => (
                      <NButton
                        size="small"
                        tertiary
                        loading={actionLoading.value === `${row.jobId}-stop`}
                      >
                        {t('jobs.actions.stop')}
                      </NButton>
                    ),
                    default: () =>
                      t('jobs.actions.stopConfirmMessage', {
                        job: row.jobName || row.jobId
                      })
                  }}
                </NPopconfirm>
                <NPopconfirm
                  positiveText={t('jobs.actions.confirm')}
                  negativeText={t('jobs.actions.cancelConfirm')}
                  onPositiveClick={() => controlJob(row, false, true)}
                >
                  {{
                    trigger: () => (
                      <NButton
                        size="small"
                        tertiary
                        type="warning"
                        loading={actionLoading.value === `${row.jobId}-savepoint`}
                      >
                        {t('jobs.actions.savepoint')}
                      </NButton>
                    ),
                    default: () =>
                      t('jobs.actions.savepointConfirmMessage', {
                        job: row.jobName || row.jobId
                      })
                  }}
                </NPopconfirm>
                <NPopconfirm
                  positiveText={t('jobs.actions.confirm')}
                  negativeText={t('jobs.actions.cancelConfirm')}
                  onPositiveClick={() => controlJob(row, true)}
                >
                  {{
                    trigger: () => (
                      <NButton
                        size="small"
                        tertiary
                        type="error"
                        loading={actionLoading.value === `${row.jobId}-cancel`}
                      >
                        {t('jobs.actions.cancel')}
                      </NButton>
                    ),
                    default: () =>
                      t('jobs.actions.cancelConfirmMessage', {
                        job: row.jobName || row.jobId
                      })
                  }}
                </NPopconfirm>
              </NSpace>
            )
          }
        }
      ]
    }

    const columns = createColumns()
    return () => (
      <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
        <h2 class="font-bold text-2xl pb-6">{t('jobs.runningJobs')}</h2>
        {feedback.value && (
          <NAlert
            class="mb-4"
            type={feedback.value.type}
            closable
            onClose={() => {
              feedback.value = null
            }}
          >
            {feedback.value.message}
          </NAlert>
        )}
        <NDataTable
          columns={columns}
          data={jobs.value}
          remote={true}
          pagination={{
            page: page.value,
            pageSize: pageSize.value,
            itemCount: total.value,
            showSizePicker: true,
            pageSizes: [10, 20, 50, 100, 500],
            showQuickJumper: true,
            onUpdatePage: (newPage: number) => {
              page.value = newPage
              fetch()
            },
            onUpdatePageSize: (newPageSize: number) => {
              pageSize.value = newPageSize
              page.value = 1
              fetch()
            }
          }}
          bordered={false}
        />
      </div>
    )
  }
})
