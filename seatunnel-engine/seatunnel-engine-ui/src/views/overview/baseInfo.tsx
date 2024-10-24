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

import { defineComponent, onUnmounted, ref } from 'vue'
import { NSpace, NCard } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { overviewService } from '@/service/overview'
import type { Overview } from '@/service/overview/types'

export default defineComponent({
  setup() {
    const { t } = useI18n()

    const data = ref({} as Overview)

    let timer: NodeJS.Timeout
    const fetch = async () => {
      data.value = await overviewService.getOverview()
      timer = setTimeout(fetch, 5000)
    }
    onUnmounted(() => clearTimeout(timer))

    fetch()

    return () => (
      <NSpace wrap-item={false}>
        <NCard title="Workers" hoverable style="flex:1">
          <span class="text-2xl font-bold">{data.value.workers}</span>
          <div class="border border-b-0 mt-3" />
          <NSpace class="mt-3" size={16}>
            <span>Total Slot: {data.value.totalSlot}</span>
            <span>Unassigned Slot: {data.value.unassignedSlot}</span>
          </NSpace>
        </NCard>
        <NCard title="Running Jobs" hoverable style="flex:1">
          <span class="text-2xl font-bold">{data.value.runningJobs}</span>
          <div class="border border-b-0 mt-3" />
          <NSpace class="mt-3" size={16}>
            <span>Cancelled: {data.value.cancelledJobs}</span>
            <span>Failed: {data.value.failedJobs}</span>
            <span>Finished: {data.value.finishedJobs}</span>
          </NSpace>
        </NCard>
      </NSpace>
    )
  }
})
