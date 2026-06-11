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
import { createApp } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import i18n from '@/locales'
import detail from '@/views/jobs/detail'
import { getJobInfo } from '@/service/job'
import type { Job } from '@/service/job/types'

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

describe('detail', () => {
  const app = createApp({})
  beforeEach(() => {
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

    const wrapper = mount(detail, {
      global: {
        plugins: [i18n]
      }
    })

    await flushPromises()

   
    expect(wrapper.text()).toContain('Oracle-CDC-Test')

    
    expect(wrapper.text()).not.toContain('NaN')
  })
})