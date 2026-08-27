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
import type { Overview } from '@/service/overview/types'
import baseInfo from '@/views/overview/baseInfo'
import { overviewService } from '@/service/overview'

describe('overview', () => {
  const app = createApp({})
  beforeEach(() => {
    const pinia = createPinia()
    app.use(pinia)
    setActivePinia(createPinia())
  })
  test('BaseInfo component', async () => {
    const mockData = {
      cancelledJobs: '222',
      failedJobs: '0',
      finishedJobs: '3',
      gitCommitAbbrev: '4f812e1',
      projectVersion: '2.3.8-SNAPSHOT',
      runningJobs: '0',
      totalSlot: '111',
      unassignedSlot: '0',
      workers: '1'
    } as Overview

    vi.spyOn(overviewService, 'getOverview').mockResolvedValue(mockData)

    const wrapper = mount(baseInfo, {
      global: {
        // plugins: [createTestingPinia({ createSpy: vi.fn() }), i18n]
        plugins: [i18n]
      }
    })
    expect(overviewService.getOverview).toHaveBeenCalledTimes(1)
    expect(overviewService.getOverview).toHaveBeenCalledWith()
    await flushPromises()
    expect(wrapper.text()).toContain('Total Slot: 111')
    expect(wrapper.text()).toContain('Cancelled: 222')
  })
})
