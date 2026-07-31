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

import { describe, test, expect, vi, afterEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import i18n from '@/locales'
import checkpoints from '@/views/jobs/checkpoints'
import { JobsService } from '@/service/job'

const routerState = vi.hoisted(() => ({
  push: vi.fn()
}))

vi.mock('vue-router', () => ({
  useRouter: () => ({
    push: routerState.push
  })
}))

describe('checkpoints', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    routerState.push.mockReset()
  })

  test('restores from checkpoint view by preloading submit job restore id', async () => {
    vi.spyOn(JobsService, 'getCheckpointOverview').mockResolvedValue({
      jobId: '123456789',
      updatedAt: 1720000000123,
      pipelines: [
        {
          pipelineId: 1,
          counts: {
            triggered: 1,
            completed: 1,
            failed: 0,
            inProgress: 0,
            restored: 0
          },
          latestCompleted: {
            checkpointId: 10,
            checkpointType: 'CHECKPOINT_TYPE',
            status: 'COMPLETED'
          }
        }
      ]
    })
    vi.spyOn(JobsService, 'getCheckpointHistory').mockResolvedValue([])

    const wrapper = mount(checkpoints, {
      props: {
        jobId: '123456789'
      },
      global: {
        plugins: [i18n]
      }
    })

    await flushPromises()
    const restoreButton = wrapper
      .findAll('button')
      .find((button) => button.text() === 'Restore Latest State')
    expect(restoreButton).toBeTruthy()
    await restoreButton?.trigger('click')
    expect(routerState.push).toHaveBeenCalledWith({
      name: 'jobs',
      query: {
        restoreJobId: '123456789'
      }
    })
    wrapper.unmount()
  })
})
