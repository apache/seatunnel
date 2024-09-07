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

import { defineComponent, getCurrentInstance, reactive } from 'vue'
import {
    NSpace,
    NLayout,
    NLayoutContent,
    useMessage,
    NCard
} from 'naive-ui'
import { useI18n } from 'vue-i18n'
import RunningJobs from '@/views/jobs/running-jobs'
import FinishedJobs from '@/views/jobs/finished-jobs'
import { getOverview } from '@/service/overview'
import { Overview } from '@/service/overview/types'

export default defineComponent({
    setup() {
        window.$message = useMessage()
        const { t } = useI18n()

        const data = reactive({} as Overview);
        getOverview().then(res => Object.assign(data, res))

        return { t, data }
    },
    render() {
        return (
            <NLayout>
                <NLayoutContent>
                    <NSpace wrap-item={false} class='mb-6'>
                        <NCard title='Availiable Task Slots' hoverable style='flex:1'>
                            <span class='text-2xl font-bold'>{this.data.workers}</span>
                            <div class='border border-b-0 mt-3' />
                            <NSpace class='mt-3' size={16}>
                                <span>Total Slot: {this.data.totalSlot}</span>
                                <span>Unassigned Slot: {this.data.unassignedSlot}</span>
                            </NSpace>
                        </NCard>
                        <NCard title='Running Jobs' hoverable style='flex:1'>
                            <span class='text-2xl font-bold'>{this.data.runningJobs}</span>
                            <div class='border border-b-0 mt-3' />
                            <NSpace class='mt-3' size={16}>
                                <span>Cancelled: {this.data.cancelledJobs}</span>
                                <span>Failed: {this.data.failedJobs}</span>
                                <span>Finished: {this.data.failedJobs}</span>
                            </NSpace>
                        </NCard>
                    </NSpace>
                    <RunningJobs class='mb-6' />
                    <FinishedJobs />
                </NLayoutContent>
            </NLayout>
        )
    }
})
