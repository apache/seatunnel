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

import { NTabs, NTabPane, NDivider, NTag } from 'naive-ui'
import { defineComponent, getCurrentInstance, h, reactive, ref, watch } from 'vue'
import { getJobInfo } from '@/service/job'
import { useRoute } from 'vue-router'
import type { Job } from '@/service/job/types'
import { useI18n } from 'vue-i18n'
import { getRemainTime } from '@/utils/time'
import { format, parse } from 'date-fns'
import Main from '@/components/directed-acyclic-graph/main'
import { getTypeFromStatus } from '@/utils/getTypeFromStatus'

export default defineComponent({
  setup() {
    const { t } = useI18n()
    const route = useRoute()

    const jobId = route.params.jobId as string
    const job = reactive({} as Job)
    const duration = ref('')
    getJobInfo(jobId).then((res) => {
      Object.assign(job, res)
      const d = parse(res.createTime, 'yyyy-MM-dd HH:mm:ss', new Date())
      setInterval(() => {
        duration.value = getRemainTime(Math.abs(Date.now() - d.getTime()))
      }, 1000)
    })

    const select = ref('Overview')
    const change = () => {
      console.log(select.value)
    }
    watch(() => select.value, change)
    return () => (
      <div class="w-full bg-white px-12 pt-6 pb-12 border border-gray-100 rounded-xl">
        <div class="font-bold text-xl">
          {job.jobName}
          <NTag bordered={false} type={getTypeFromStatus(job.jobStatus)} class="ml-3">
            {job.jobStatus}
          </NTag>
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
        <NTabs v-model:value={select.value} type="line" animated>
          <NTabPane name="Overview" tab="Overview">
            <Main {...{ job: job }} />
          </NTabPane>
          <NTabPane name="Logs" tab="Logs">
            Logs
          </NTabPane>
          <NTabPane name="Exception" tab="Exception">
            Exception
          </NTabPane>
          <NTabPane name="Metrics" tab="Metrics">
            Metrics
          </NTabPane>
        </NTabs>
      </div>
    )
  }
})
