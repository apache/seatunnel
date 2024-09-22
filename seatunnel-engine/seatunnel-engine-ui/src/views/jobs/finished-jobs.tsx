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

import { defineComponent, getCurrentInstance, h, ref } from 'vue'
import { useMessage, NDataTable, NTag } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { JobsService } from '@/service/job'
import type { DataTableColumns } from 'naive-ui'
import { NButton } from 'naive-ui'
import type { Job } from '@/service/job/types'

export default defineComponent({
  setup() {
    const { t } = useI18n()

    const jobs = ref([] as Job[])

    JobsService.getFinishedJobs().then((res) => (jobs.value = res))
    function createColumns(): DataTableColumns<Job> {
      const view = (job: Job) => {}
      return [
        {
          title: 'No',
          key: 'No',
          render: (row, index) => h('div', index + 1)
        },
        {
          title: 'Id',
          key: 'jobId'
        },
        {
          title: 'Name',
          key: 'jobName'
        },
        {
          title: 'Create Time',
          key: 'createTime'
        },
        {
          title: 'Status',
          key: 'jobStatus',
          render(row) {
            return h(NTag, { bordered: false, type: 'success' }, { default: () => row.jobStatus })
          }
        },
        {
          title: 'Action',
          key: 'actions',
          render(row) {
            return h(
              NButton,
              {
                strong: true,
                tertiary: true,
                size: 'small',
                onClick: () => view(row)
              },
              { default: () => 'View' }
            )
          }
        }
      ]
    }

    const columns = createColumns()
    return () => (
      <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
        <h2 class="font-bold text-2xl pb-6">{t('jobs.finishedJobs')}</h2>
        <NDataTable columns={columns} data={jobs.value} pagination={false} bordered={false} />
      </div>
    )
  }
})
