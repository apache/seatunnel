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
import { flushPromises, mount, type VueWrapper } from '@vue/test-utils'
import { createApp, h } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import { NMessageProvider } from 'naive-ui'
import i18n from '@/locales'
import detail from '@/views/jobs/detail'
import { getJobInfo } from '@/service/job'
import { getJobCheckpointOverview } from '@/service/checkpoint'
import type { CheckpointOverviewResponse } from '@/service/checkpoint'
import type { Job } from '@/service/job/types'

vi.mock('@/service/checkpoint', () => ({
  getJobCheckpointOverview: vi.fn()
}))

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
    vi.mocked(getJobCheckpointOverview)
      .mockReset()
      .mockResolvedValue({ jobId: '123456789', pipelines: [] })
    const pinia = createPinia()
    app.use(pinia)
    setActivePinia(createPinia())
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
    wrapper.unmount()
  })

  test('renders checkpoint counts and the latest event from the checkpoint service', async () => {
    vi.mocked(getJobInfo).mockResolvedValue({
      jobId: '123456789',
      jobName: 'runtime-summary',
      jobStatus: 'FINISHED',
      createTime: '2026-05-21 10:00:00',
      errorMsg: '  connector failed  ',
      jobDag: { jobId: '123456789', vertexInfoMap: [], pipelineEdges: {} },
      metrics: {},
      pluginJarsUrls: []
    } as unknown as Job)
    vi.mocked(getJobCheckpointOverview).mockResolvedValue({
      jobId: '123456789',
      pipelines: [
        {
          pipelineId: 1,
          counts: { triggered: 4, completed: 3, failed: 1, inProgress: 0, restored: 0 },
          latestCompleted: { checkpointId: 7, status: 'COMPLETED', durationMillis: 1234 },
          latestFailed: {},
          latestSavepoint: {},
          inProgress: [],
          history: []
        }
      ]
    })
    const pinia = createPinia()
    setActivePinia(pinia)
    const wrapper = mount(
      {
        setup: () => () => h(NMessageProvider, null, { default: () => h(detail) })
      },
      { global: { plugins: [i18n, pinia] } }
    )
    await flushPromises()

    expect(wrapper.findAll('.runtime-context-item')[1].text()).toContain('3/4')
    expect(wrapper.findAll('.runtime-context-item')[1].text()).toContain('#7 COMPLETED 1.23 s')
    expect(wrapper.find('.runtime-error-snippet').text()).toBe('connector failed')
    expect(wrapper.find('.runtime-large-summary').exists()).toBe(false)
    wrapper.unmount()
  })

  describe('checkpoint refresh', () => {
    let wrapper: VueWrapper | undefined
    const snapshot: CheckpointOverviewResponse = {
      jobId: '123456789',
      pipelines: [
        {
          pipelineId: 1,
          counts: { triggered: 9, completed: 8, failed: 1, inProgress: 0, restored: 0 },
          latestCompleted: { checkpointId: 17, status: 'COMPLETED', durationMillis: 500 },
          latestFailed: {},
          latestSavepoint: {},
          inProgress: [],
          history: []
        }
      ]
    }
    const deferred = () => {
      let resolve!: (value: CheckpointOverviewResponse) => void
      let reject!: (reason: Error) => void
      const promise = new Promise<CheckpointOverviewResponse>((res, rej) => {
        resolve = res
        reject = rej
      })
      return { promise, resolve, reject }
    }
    const mountDetail = async () => {
      const pinia = createPinia()
      setActivePinia(pinia)
      wrapper = mount(
        {
          setup: () => () => h(NMessageProvider, null, { default: () => h(detail) })
        },
        { global: { plugins: [i18n, pinia] } }
      )
      await flushPromises()
      return wrapper
    }

    beforeEach(() => {
      // Keep setImmediate real so flushPromises can drain deferred API responses.
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] })
      vi.mocked(getJobInfo).mockResolvedValue({
        jobId: '123456789',
        jobName: 'runtime-refresh',
        jobStatus: 'RUNNING',
        createTime: '2026-05-21 10:00:00',
        errorMsg: '',
        jobDag: { jobId: '123456789', vertexInfoMap: [], pipelineEdges: {} },
        metrics: {},
        pluginJarsUrls: []
      } as unknown as Job)
    })

    afterEach(() => {
      wrapper?.unmount()
      wrapper = undefined
      vi.useRealTimers()
      vi.restoreAllMocks()
    })

    test('shows unavailable without zero counts when the monitor has no snapshot', async () => {
      vi.mocked(getJobCheckpointOverview).mockResolvedValue({ jobId: '123456789' })
      const view = await mountDetail()
      const panel = view.findAll('.runtime-context-item')[1]
      expect(panel.find('.runtime-context-value').text()).toBe(
        i18n.global.t('detail.runtime.unavailable')
      )
      expect(panel.text()).toContain(i18n.global.t('detail.runtime.checkpointUnavailable'))
      expect(panel.text()).not.toContain('0/0')
    })

    test('retains valid zero counts for an available snapshot with no pipelines', async () => {
      const view = await mountDetail()
      const panel = view.findAll('.runtime-context-item')[1]
      expect(panel.find('.runtime-context-value').text()).toContain('0/0')
      expect(panel.text()).not.toContain(i18n.global.t('detail.runtime.checkpointUnavailable'))
    })

    test.each(['missing snapshot', 'request failure'])(
      'clears previous checkpoint values after %s',
      async (outcome) => {
        vi.spyOn(console, 'warn').mockImplementation(() => {})
        vi.mocked(getJobCheckpointOverview).mockResolvedValueOnce(snapshot)
        if (outcome === 'missing snapshot') {
          vi.mocked(getJobCheckpointOverview).mockResolvedValueOnce({ jobId: '123456789' })
        } else {
          vi.mocked(getJobCheckpointOverview).mockRejectedValueOnce(
            new Error('checkpoint unavailable')
          )
        }
        const view = await mountDetail()
        expect(view.findAll('.runtime-context-item')[1].text()).toContain('#17 COMPLETED')
        await vi.advanceTimersByTimeAsync(5000)
        await flushPromises()
        const panel = view.findAll('.runtime-context-item')[1]
        expect(panel.text()).toContain(i18n.global.t('detail.runtime.checkpointUnavailable'))
        expect(panel.text()).not.toContain('#17')
        expect(panel.text()).not.toContain('8/9')
        expect(panel.text()).not.toContain('0/0')
      }
    )

    test.each(['success', 'failure'])(
      'ignores an older %s after a newer request succeeds',
      async (outcome) => {
        const older = deferred()
        const newer = deferred()
        vi.mocked(getJobCheckpointOverview)
          .mockReturnValueOnce(older.promise)
          .mockReturnValueOnce(newer.promise)
        const view = await mountDetail()
        await vi.advanceTimersByTimeAsync(5000)
        newer.resolve(snapshot)
        await flushPromises()
        expect(view.findAll('.runtime-context-item')[1].text()).toContain('8/9')
        if (outcome === 'success') older.resolve({ jobId: '123456789', pipelines: [] })
        else older.reject(new Error('older request failed'))
        await flushPromises()
        const panel = view.findAll('.runtime-context-item')[1]
        expect(panel.text()).toContain('8/9')
        expect(panel.text()).toContain('#17 COMPLETED')
        expect(panel.text()).not.toContain(i18n.global.t('detail.runtime.checkpointUnavailable'))
      }
    )

    test('does not restore an older successful snapshot after the latest request fails', async () => {
      vi.spyOn(console, 'warn').mockImplementation(() => {})
      const older = deferred()
      const newer = deferred()
      vi.mocked(getJobCheckpointOverview)
        .mockReturnValueOnce(older.promise)
        .mockReturnValueOnce(newer.promise)
      const view = await mountDetail()
      await vi.advanceTimersByTimeAsync(5000)
      newer.reject(new Error('latest request failed'))
      await flushPromises()
      older.resolve(snapshot)
      await flushPromises()
      const panel = view.findAll('.runtime-context-item')[1]
      expect(panel.text()).toContain(i18n.global.t('detail.runtime.checkpointUnavailable'))
      expect(panel.text()).not.toContain('8/9')
      expect(panel.text()).not.toContain('#17')
    })
  })
})
