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

import { describe, test, expect, vi, beforeEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
// import { createTestingPinia } from '@pinia/testing'
import runningJobs from '@/views/jobs/running-jobs'
import { createApp } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import i18n from '@/locales'
import finishedJobs from '@/views/jobs/finished-jobs'
import { JobsService } from '@/service/job'
import type { Job } from '@/service/job/types'

describe('jobs', () => {
  const app = createApp({})
  beforeEach(() => {
    const pinia = createPinia()
    app.use(pinia)
    setActivePinia(createPinia())
  })
  test('test', () => {
    expect(1).toBe(1)
  })
  test('Running Jobs component', async () => {
    const mockData = [] as Job[]

    vi.spyOn(JobsService, 'getRunningJobs').mockResolvedValue(mockData)
    const wrapper = mount(runningJobs, {
      global: {
        // plugins: [createTestingPinia({ createSpy: vi.fn() }), i18n]
        plugins: [i18n]
      }
    })
    await flushPromises()
    expect(wrapper.text()).toContain('Running Jobs')
  })
  test('Finished Jobs component', async () => {
    const mockData = [
      {
        jobId: '888413907541032961',
        jobName: 'SeaTunnel_Job',
        jobStatus: 'FINISHED',
        errorMsg: '',
        createTime: '2024-09-17 21:19:41',
        finishTime: '2024-09-17 21:19:44'
      }
    ] as Job[]

    vi.spyOn(JobsService, 'getFinishedJobs').mockResolvedValue(mockData)

    const wrapper = mount(finishedJobs, {
      global: {
        // plugins: [createTestingPinia({ createSpy: vi.fn() }), i18n]
        plugins: [i18n]
      }
    })
    expect(JobsService.getFinishedJobs).toHaveBeenCalledTimes(1)
    expect(JobsService.getFinishedJobs).toHaveBeenCalledWith()
    await flushPromises()
    expect(wrapper.text()).toContain('SeaTunnel_Job')
  })
})
