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

import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
// import { createTestingPinia } from '@pinia/testing'
import { NPopconfirm } from 'naive-ui'
import runningJobs from '@/views/jobs/running-jobs'
import jobOperations from '@/views/jobs/job-operations'
import { createApp } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import i18n from '@/locales'
import finishedJobs from '@/views/jobs/finished-jobs'
import { JobsService } from '@/service/job'
import type { JobPage, Job } from '@/service/job/types'

const routeState = vi.hoisted(() => ({
  query: {} as Record<string, string>,
  push: vi.fn()
}))

vi.mock('vue-router', () => ({
  useRoute: () => ({
    query: routeState.query
  }),
  useRouter: () => ({
    push: routeState.push
  })
}))

describe('jobs', () => {
  const app = createApp({})
  beforeEach(() => {
    const pinia = createPinia()
    app.use(pinia)
    setActivePinia(createPinia())
    routeState.query = {}
    routeState.push.mockReset()
  })
  afterEach(() => {
    vi.restoreAllMocks()
  })
  test('Running Jobs component', async () => {
    const mockData = {
      data: [
        {
          jobId: '888413907541032961',
          jobName: 'SeaTunnel_Job',
          jobStatus: 'RUNNING',
          errorMsg: '',
          createTime: '2024-09-17 21:19:41',
          finishTime: ''
        }
      ] as Job[],
      total: 1
    } as JobPage

    vi.spyOn(JobsService, 'getRunningJobs').mockResolvedValue(mockData)
    const stopJobSpy = vi.spyOn(JobsService, 'stopJob').mockResolvedValue({
      jobId: '888413907541032961'
    })
    const wrapper = mount(runningJobs, {
      global: {
        // plugins: [createTestingPinia({ createSpy: vi.fn() }), i18n]
        plugins: [i18n]
      }
    })
    await flushPromises()
    expect(wrapper.text()).toContain('Running Jobs')
    expect(wrapper.text()).toContain('Stop')
    expect(wrapper.text()).toContain('Savepoint')
    expect(wrapper.text()).toContain('Cancel')
    const confirmations = wrapper.findAllComponents(NPopconfirm)
    expect(confirmations).toHaveLength(3)
    const clickConfirmation = async (index: number) => {
      const onPositiveClick = confirmations[index].props('onPositiveClick') as (
        event: MouseEvent
      ) => Promise<void> | void
      await onPositiveClick(new MouseEvent('click'))
    }
    await clickConfirmation(0)
    expect(stopJobSpy).toHaveBeenCalledWith({
      jobId: '888413907541032961',
      isStopWithSavePoint: false,
      force: false
    })
    await clickConfirmation(1)
    expect(stopJobSpy).toHaveBeenCalledWith({
      jobId: '888413907541032961',
      isStopWithSavePoint: true,
      force: false
    })
    await clickConfirmation(2)
    expect(stopJobSpy).toHaveBeenCalledWith({
      jobId: '888413907541032961',
      isStopWithSavePoint: false,
      force: true
    })
    wrapper.unmount()
  })
  test('Finished Jobs component', async () => {
    const mockData = {
      data: [
        {
          jobId: '888413907541032961',
          jobName: 'SeaTunnel_Job',
          jobStatus: 'FINISHED',
          errorMsg: '',
          createTime: '2024-09-17 21:19:41',
          finishTime: '2024-09-17 21:19:44'
        }
      ] as Job[],
      total: 1
    } as JobPage

    vi.spyOn(JobsService, 'getFinishedJobs').mockResolvedValue(mockData)

    const wrapper = mount(finishedJobs, {
      global: {
        // plugins: [createTestingPinia({ createSpy: vi.fn() }), i18n]
        plugins: [i18n]
      }
    })
    expect(JobsService.getFinishedJobs).toHaveBeenCalledTimes(1)
    expect(JobsService.getFinishedJobs).toHaveBeenCalledWith(1, 10)
    await flushPromises()
    expect(wrapper.text()).toContain('SeaTunnel_Job')
    wrapper.unmount()
  })
  test('Job Operations component', async () => {
    routeState.query = {
      restoreJobId: '888413907541032961'
    }
    const submitJobSpy = vi.spyOn(JobsService, 'submitJob').mockResolvedValue({
      jobId: '888413907541032961',
      jobName: 'SeaTunnel_Job'
    })
    const wrapper = mount(jobOperations, {
      global: {
        plugins: [i18n]
      }
    })

    await flushPromises()
    expect(wrapper.text()).toContain('Submit Job')
    expect(wrapper.text()).toContain('Config Text')
    expect(wrapper.text()).toContain('Config File')
    expect(wrapper.text()).toContain('Restore Job ID')
    await wrapper.find('textarea').setValue('env { job.mode = "BATCH" }')
    const submitButton = wrapper.findAll('button').find((button) => button.text() === 'Submit')
    expect(submitButton).toBeTruthy()
    await submitButton?.trigger('click')
    await flushPromises()
    expect(submitJobSpy).toHaveBeenCalledWith({
      config: 'env { job.mode = "BATCH" }',
      format: 'hocon',
      jobName: '',
      jobId: '888413907541032961',
      isStartWithSavePoint: true
    })
    wrapper.unmount()
  })
})
