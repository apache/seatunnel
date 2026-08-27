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
import { createApp } from 'vue'
import { createPinia, setActivePinia } from 'pinia'
import i18n from '@/locales'
import type { Monitor } from '@/service/manager/types'
import { managerService } from '@/service/manager'
import managers from '@/views/managers'

describe('managers', () => {
  const app = createApp({})
  beforeEach(() => {
    const pinia = createPinia()
    app.use(pinia)
    setActivePinia(createPinia())
  })
  test('managers component', async () => {
    const mockData = [
      {
        isMaster: 'true',
        host: 'localhost',
        port: '5801',
        'physical.memory.total': '3.6G',
        'heap.memory.used': '229.6M'
      },
      {
        isMaster: 'false',
        host: 'localhost',
        port: '5802',
        'physical.memory.total': '3.6G',
        'heap.memory.used': '1002.6M'
      }
    ] as Monitor[]

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
    expect(wrapper.text()).toContain('localhost')
  })
})
