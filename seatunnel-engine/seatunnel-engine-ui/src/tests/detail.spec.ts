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
import { createApp, h } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import { NMessageProvider } from 'naive-ui'
import i18n from '@/locales'
import detail from '@/views/jobs/detail'
import { getJobInfo } from '@/service/job'
import type { Job } from '@/service/job/types'

const { warningSpy } = vi.hoisted(() => ({ warningSpy: vi.fn() }))

vi.mock('naive-ui', async (importOriginal) => {
  const actual = await importOriginal<typeof import('naive-ui')>()
  return {
    ...actual,
    useMessage: () => ({
      warning: warningSpy,
      error: vi.fn(),
      success: vi.fn(),
      info: vi.fn()
    })
  }
})

vi.mock('@/service/job', () => ({
  getJobInfo: vi.fn(),
  getRunningJobInfo: vi.fn()
}))

vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { jobId: '123456789' } }),
  useRouter: () => ({ push: vi.fn() })
}))

vi.mock('@/components/directed-acyclic-graph', () => ({
  default: { template: '<div></div>' }
}))

vi.mock('@/components/live-metrics-chart', () => ({
  default: { template: '<div></div>' }
}))

vi.mock('@/components/live-metrics-chart/board', () => ({
  default: { template: '<div></div>' }
}))

vi.mock('@/service/realtime-metrics', async () => {
  const actual = await vi.importActual<typeof import('@/service/realtime-metrics')>(
    '@/service/realtime-metrics'
  )
  return {
    ...actual,
    fetchJobRealtimeMetrics: vi.fn().mockResolvedValue({
      edges: { bucketMs: 5000, fromMs: 0, toMs: 0, edges: [] },
      vertices: { bucketMs: 5000, fromMs: 0, toMs: 0, vertices: [] },
      windowMs: actual.REALTIME_WINDOW_MS_DEFAULT
    })
  }
})

describe('detail', () => {
  const app = createApp({})
  beforeEach(() => {
    const pinia = createPinia()
    app.use(pinia)
    setActivePinia(createPinia())
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  test('should not display NaN when tablePaths does not match metrics keys', async () => {
    const mockJob = {
      jobId: '123456789',
      jobName: 'Oracle-CDC-Test',
      jobStatus: 'FINISHED',
      errorMsg: '',
      createTime: '2026-05-21 10:00:00',
      finishTime: '2026-05-21 11:00:00',
      metrics: {
        SourceReceivedBytes: '119028',
        SourceReceivedBytesPerSeconds: '1024',
        SourceReceivedCount: '141',
        SourceReceivedQPS: '10',
        SinkWriteBytes: '98304',
        SinkWriteBytesPerSeconds: '512',
        SinkWriteCount: '138',
        SinkWriteQPS: '9',
        TableSourceReceivedBytes: { 'Source[0].fake': '119028' },
        TableSourceReceivedCount: { 'Source[0].fake': '141' },
        TableSourceReceivedQPS: { 'Source[0].fake': '10' },
        TableSourceReceivedBytesPerSeconds: { 'Source[0].fake': '1024' },
        TableSinkWriteBytes: { 'Sink[0].fake': '98304' },
        TableSinkWriteCount: { 'Sink[0].fake': '138' },
        TableSinkWriteQPS: { 'Sink[0].fake': '9' },
        TableSinkWriteBytesPerSeconds: { 'Sink[0].fake': '512' }
      },
      jobDag: {
        jobId: '123456789',
        pipelineEdges: {},
        vertexInfoMap: [
          {
            vertexId: 1,
            type: 'source',
            vertexName: 'pipeline-1 [Source[0]-FakeSource]',
            tablePaths: ['fake']
          },
          {
            vertexId: 2,
            type: 'sink',
            vertexName: 'pipeline-1 [Sink[0]-FakeSink]',
            tablePaths: ['fake']
          }
        ]
      },
      pluginJarsUrls: []
    } as unknown as Job

    vi.mocked(getJobInfo).mockResolvedValue(mockJob)

    const pinia = createPinia()
    setActivePinia(pinia)
    const wrapper = mount(
      {
        setup() {
          return () => h(NMessageProvider, null, { default: () => h(detail) })
        }
      },
      {
        global: {
          plugins: [i18n, pinia]
        }
      }
    )

    await flushPromises()

    expect(wrapper.text()).toContain('Oracle-CDC-Test')

    expect(wrapper.text()).not.toContain('NaN')
  })

  test('should show a missing-job state and stop polling for found:false', async () => {
    vi.useFakeTimers()
    vi.mocked(getJobInfo).mockClear()
    vi.mocked(getJobInfo).mockResolvedValue({
      jobId: '123456789',
      found: false
    } as unknown as Job)

    const pinia = createPinia()
    setActivePinia(pinia)
    const wrapper = mount(
      {
        setup() {
          return () => h(NMessageProvider, null, { default: () => h(detail) })
        }
      },
      {
        global: {
          plugins: [i18n, pinia]
        }
      }
    )

    await flushPromises()

    expect(wrapper.text()).toContain('Job not found')
    expect(wrapper.text()).toContain('123456789')
    expect(wrapper.text()).not.toContain('Create Time')
    expect(getJobInfo).toHaveBeenCalledTimes(1)

    vi.advanceTimersByTime(6000)
    await flushPromises()

    expect(getJobInfo).toHaveBeenCalledTimes(1)
  })

  test('should treat two status-less responses from an older server as missing', async () => {
    vi.useFakeTimers()
    vi.mocked(getJobInfo).mockClear()
    vi.mocked(getJobInfo).mockResolvedValue({ jobId: '123456789' } as unknown as Job)

    const pinia = createPinia()
    setActivePinia(pinia)
    const wrapper = mount(
      {
        setup() {
          return () => h(NMessageProvider, null, { default: () => h(detail) })
        }
      },
      {
        global: {
          plugins: [i18n, pinia]
        }
      }
    )

    await flushPromises()
    expect(wrapper.text()).not.toContain('Job not found')

    vi.advanceTimersByTime(5000)
    await flushPromises()

    expect(getJobInfo).toHaveBeenCalledTimes(2)
    expect(wrapper.text()).toContain('Job not found')
  })

  test('should recover from one status-less response', async () => {
    vi.useFakeTimers()
    vi.mocked(getJobInfo).mockClear()
    vi.mocked(getJobInfo)
      .mockResolvedValueOnce({ jobId: '123456789' } as unknown as Job)
      .mockResolvedValueOnce({
        jobId: '123456789',
        jobName: 'Recovered',
        jobStatus: 'FINISHED',
        errorMsg: '',
        createTime: '2026-05-21 10:00:00',
        finishTime: '2026-05-21 11:00:00',
        metrics: {},
        jobDag: { jobId: '123456789', pipelineEdges: {}, vertexInfoMap: [] },
        pluginJarsUrls: []
      } as unknown as Job)

    const pinia = createPinia()
    setActivePinia(pinia)
    const wrapper = mount(
      {
        setup() {
          return () => h(NMessageProvider, null, { default: () => h(detail) })
        }
      },
      {
        global: {
          plugins: [i18n, pinia]
        }
      }
    )

    await flushPromises()
    expect(wrapper.text()).not.toContain('Job not found')

    vi.advanceTimersByTime(5000)
    await flushPromises()

    expect(getJobInfo).toHaveBeenCalledTimes(2)
    expect(wrapper.text()).toContain('Recovered')
  })

  test('should show an error result when getJobInfo rejects', async () => {
    vi.mocked(getJobInfo).mockRejectedValue(new Error('master unavailable'))

    const pinia = createPinia()
    setActivePinia(pinia)
    const wrapper = mount(
      {
        setup() {
          return () => h(NMessageProvider, null, { default: () => h(detail) })
        }
      },
      {
        global: {
          plugins: [i18n, pinia]
        }
      }
    )

    await flushPromises()

    expect(wrapper.text()).toContain('Could not load job')
    expect(wrapper.text()).toContain('master unavailable')
  })

  test('should keep the loaded job when a refresh is rejected', async () => {
    vi.useFakeTimers()
    vi.mocked(getJobInfo).mockClear()
    const runningJob = {
      jobId: '123456789',
      jobName: 'Still Here',
      jobStatus: 'RUNNING',
      errorMsg: '',
      createTime: '2026-05-21 10:00:00',
      metrics: {},
      jobDag: { jobId: '123456789', pipelineEdges: {}, vertexInfoMap: [] },
      pluginJarsUrls: []
    } as unknown as Job
    vi.mocked(getJobInfo)
      .mockResolvedValueOnce(runningJob)
      .mockRejectedValueOnce(new Error('temporary blip'))
      .mockResolvedValueOnce({ ...runningJob, jobName: 'Recovered' } as Job)

    const pinia = createPinia()
    setActivePinia(pinia)
    const wrapper = mount(
      {
        setup() {
          return () => h(NMessageProvider, null, { default: () => h(detail) })
        }
      },
      {
        global: {
          plugins: [i18n, pinia]
        }
      }
    )

    await flushPromises()
    expect(wrapper.text()).toContain('Still Here')

    vi.advanceTimersByTime(5000)
    await flushPromises()

    expect(getJobInfo).toHaveBeenCalledTimes(2)
    expect(wrapper.text()).toContain('Still Here')
    expect(wrapper.text()).not.toContain('Could not load job')

    vi.advanceTimersByTime(5000)
    await flushPromises()

    expect(getJobInfo).toHaveBeenCalledTimes(3)
    expect(wrapper.text()).toContain('Recovered')
  })

  test('should warn once per error streak and reset after a successful refresh', async () => {
    vi.useFakeTimers()
    warningSpy.mockClear()
    vi.mocked(getJobInfo).mockClear()
    const runningJob = {
      jobId: '123456789',
      jobName: 'Still Here',
      jobStatus: 'RUNNING',
      errorMsg: '',
      createTime: '2026-05-21 10:00:00',
      metrics: {},
      jobDag: { jobId: '123456789', pipelineEdges: {}, vertexInfoMap: [] },
      pluginJarsUrls: []
    } as unknown as Job
    vi.mocked(getJobInfo)
      .mockResolvedValueOnce(runningJob)
      .mockRejectedValueOnce(new Error('blip'))
      .mockRejectedValueOnce(new Error('blip'))
      .mockRejectedValueOnce(new Error('other blip'))
      .mockResolvedValueOnce({ ...runningJob, jobName: 'Recovered' } as Job)
      .mockRejectedValueOnce(new Error('blip'))

    const pinia = createPinia()
    setActivePinia(pinia)
    mount(
      {
        setup() {
          return () => h(NMessageProvider, null, { default: () => h(detail) })
        }
      },
      {
        global: {
          plugins: [i18n, pinia]
        }
      }
    )

    await flushPromises()
    expect(warningSpy).not.toHaveBeenCalled()

    vi.advanceTimersByTime(5000)
    await flushPromises()
    expect(warningSpy).toHaveBeenCalledTimes(1)

    vi.advanceTimersByTime(5000)
    await flushPromises()
    expect(warningSpy).toHaveBeenCalledTimes(1)

    vi.advanceTimersByTime(5000)
    await flushPromises()
    expect(warningSpy).toHaveBeenCalledTimes(2)

    vi.advanceTimersByTime(5000)
    await flushPromises()
    expect(warningSpy).toHaveBeenCalledTimes(2)

    vi.advanceTimersByTime(5000)
    await flushPromises()
    expect(warningSpy).toHaveBeenCalledTimes(3)
  })
})
