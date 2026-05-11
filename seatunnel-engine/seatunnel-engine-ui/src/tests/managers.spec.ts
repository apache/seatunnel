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
import { createPinia, setActivePinia } from 'pinia'
import i18n from '@/locales'
import type { Monitor } from '@/service/manager/types'
import { managerService } from '@/service/manager'
import managers from '@/views/managers'
import { useRoute } from 'vue-router'

vi.mock('vue-router', () => ({
  useRoute: vi.fn()
}))

describe('managers', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
    const pinia = createPinia()
    setActivePinia(pinia)
  })
  test('master managers page should show coordinator-capable nodes', async () => {
    const mockData = [
      {
        isMaster: 'true',
        nodeRole: 'MASTER',
        coordinator: 'true',
        worker: 'false',
        host: 'localhost',
        port: '5801',
        'physical.memory.total': '3.6G',
        'heap.memory.used': '229.6M'
      },
      {
        isMaster: 'false',
        nodeRole: 'MASTER_AND_WORKER',
        coordinator: 'true',
        worker: 'true',
        host: 'localhost',
        port: '5802',
        'physical.memory.total': '3.6G',
        'heap.memory.used': '1002.6M'
      },
      {
        isMaster: 'false',
        nodeRole: 'WORKER',
        coordinator: 'false',
        worker: 'true',
        host: 'localhost',
        port: '5803',
        'physical.memory.total': '3.6G',
        'heap.memory.used': '888.8M'
      }
    ] as Monitor[]

    vi.mocked(useRoute).mockReturnValue({ path: '/managers/master' } as ReturnType<typeof useRoute>)
    vi.spyOn(managerService, 'getMonitors').mockResolvedValue(mockData)

    const wrapper = mount(managers, {
      global: {
        // plugins: [createTestingPinia({ createSpy: vi.fn() }), i18n]
        plugins: [i18n]
      }
    })
    expect(managerService.getMonitors).toHaveBeenCalledTimes(1)
    expect(managerService.getMonitors).toHaveBeenCalledWith()
    await flushPromises()
    expect(wrapper.text()).toContain('5801')
    expect(wrapper.text()).toContain('5802')
    expect(wrapper.text()).not.toContain('5803')
  })

  test('worker managers page should show worker-capable nodes', async () => {
    const mockData = [
      {
        isMaster: 'true',
        nodeRole: 'MASTER',
        coordinator: 'true',
        worker: 'false',
        host: 'localhost',
        port: '5801',
        'physical.memory.total': '3.6G',
        'heap.memory.used': '229.6M'
      },
      {
        isMaster: 'false',
        nodeRole: 'MASTER_AND_WORKER',
        coordinator: 'true',
        worker: 'true',
        host: 'localhost',
        port: '5802',
        'physical.memory.total': '3.6G',
        'heap.memory.used': '1002.6M'
      },
      {
        isMaster: 'false',
        nodeRole: 'WORKER',
        coordinator: 'false',
        worker: 'true',
        host: 'localhost',
        port: '5803',
        'physical.memory.total': '3.6G',
        'heap.memory.used': '888.8M'
      }
    ] as Monitor[]

    vi.mocked(useRoute).mockReturnValue(
      { path: '/managers/workers' } as ReturnType<typeof useRoute>
    )
    vi.spyOn(managerService, 'getMonitors').mockResolvedValue(mockData)

    const wrapper = mount(managers, {
      global: {
        plugins: [i18n]
      }
    })
    await flushPromises()
    expect(wrapper.text()).not.toContain('5801')
    expect(wrapper.text()).toContain('5802')
    expect(wrapper.text()).toContain('5803')
  })
})
