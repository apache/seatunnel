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

import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createMemoryHistory, createRouter } from 'vue-router'
import { NButton, NDataTable, NDrawer } from 'naive-ui'
import i18n from '@/locales'
import type { Monitor, WorkerResource, WorkerResourceSnapshot } from '@/service/manager/types'
import { managerService } from '@/service/manager'
import managers from '@/views/managers'
import {
  addressKey,
  bytesValue,
  joinResources,
  numberValue,
  ratioValue
} from '@/views/managers/resources'

const monitor = (host = 'localhost', port = '5802', master = false) =>
  ({
    isMaster: String(master),
    host,
    port,
    'physical.memory.total': '3.6G',
    'heap.memory.used': '1002.6M',
    'heap.memory.max': '2G',
    'load.process': '10%',
    'thread.count': '24',
    'minor.gc.count': '2',
    'major.gc.count': '0'
  }) as Monitor
const worker = (address = 'localhost:5802', dynamicSlot = false): WorkerResource => ({
  address,
  dynamicSlot,
  totalSlots: 4,
  usedSlots: 3,
  freeSlots: 1,
  cpuUsage: 0,
  memUsage: 0.5,
  availableCpuCores: 0,
  totalCpuCores: 8,
  totalHeapMemoryBytes: 1048576,
  availableHeapMemoryBytes: null,
  runningJobIds: JSON.parse('[9223372036854775807]'),
  tags: { region: 'west' }
})
const snapshot = (workers = [worker()], available = true): WorkerResourceSnapshot => ({
  available,
  collectedAt: 1723017600000,
  workers
})
const deferred = <T>() => {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

describe('manager resource helpers', () => {
  test('joins bracketed, expanded and compressed IPv6 with host and port', () => {
    expect(addressKey('2001:0db8:0:0:0:0:0:1:5801')).toBe('[2001:db8::1]:5801')
    expect(
      joinResources(
        [monitor('2001:0db8:0:0:0:0:0:1', '5801')],
        [worker('[2001:db8::1]:5801')],
        false
      )
    ).toHaveLength(1)
    expect(addressKey('[fe80::1%en0]:5801')).toBe('[fe80::1%en0]:5801')
    expect(addressKey('[::1]:80')).toBe('[::1]:80')
  })
  test('preserves unmatched, mixed-role and resource-only workers without inventing resources', () => {
    const rows = joinResources(
      [monitor('missing'), monitor('both', '5802', true)],
      [worker('both:5802'), worker('resources-only:5802')],
      false
    )
    expect(rows).toHaveLength(3)
    expect(rows.find((row) => row.address === 'missing:5802')?.resource).toBeUndefined()
    expect(rows.find((row) => row.address === 'both:5802')?.monitor).toBeDefined()
    expect(rows.find((row) => row.address === 'resources-only:5802')?.monitor).toBeUndefined()
    expect(joinResources([monitor(), monitor('master', '5801', true)], [worker()], true)).toEqual([
      { address: 'master:5801', monitor: monitor('master', '5801', true) }
    ])
  })
  test('ignores malformed records and distinguishes missing numbers from zero', () => {
    expect(
      joinResources(
        [null, {}, monitor()] as Monitor[],
        [null, {}, worker()] as WorkerResource[],
        false
      )
    ).toHaveLength(1)
    expect(numberValue(null)).toBe('—')
    expect(numberValue(0)).toBe('0')
    expect(numberValue(NaN)).toBe('—')
    expect(ratioValue(0)).toBe('0.0%')
    expect(ratioValue(0.42)).toBe('42.0%')
    expect(ratioValue(-1)).toBe('—')
    expect(ratioValue(1.5)).toBe('—')
    expect(bytesValue(null)).toBe('—')
    expect(bytesValue(1048576)).toBe('1.0 MiB')
  })
})

describe('managers', () => {
  const wrappers: ReturnType<typeof mount>[] = []
  beforeEach(() => {
    vi.useFakeTimers()
    vi.spyOn(managerService, 'getMonitors').mockResolvedValue([monitor()])
    vi.spyOn(managerService, 'getWorkerResources').mockResolvedValue(snapshot())
    i18n.global.locale.value = 'en_US'
  })
  afterEach(() => {
    wrappers.forEach((wrapper) => wrapper.unmount())
    wrappers.length = 0
    vi.restoreAllMocks()
    vi.useRealTimers()
    document.body.innerHTML = ''
  })
  async function setup(path = '/managers/workers') {
    const router = createRouter({
      history: createMemoryHistory(),
      routes: [{ path: '/managers/:role', component: managers }]
    })
    await router.push(path)
    await router.isReady()
    const wrapper = mount(managers, { global: { plugins: [i18n, router] } })
    wrappers.push(wrapper)
    return { wrapper, router }
  }
  test('renders fixed slots, monitoring and resource details without lossy job IDs', async () => {
    const { wrapper } = await setup()
    await flushPromises()
    expect(wrapper.text()).toContain('localhost:5802')
    expect(wrapper.text()).toContain('3 / 4 used, 1 free')
    expect(wrapper.text()).toContain('10%')
    expect(wrapper.text()).toContain('Resource response time')
    await wrapper
      .findAllComponents(NButton)
      .find((button) => button.text() === 'Details')!
      .trigger('click')
    await flushPromises()
    expect(document.body.textContent).toContain('0 / 8')
    expect(document.body.textContent).toContain('0.0%')
    expect(document.body.textContent).toContain('heap.memory.max')
    expect(document.body.textContent).not.toContain('922337203685')
    expect(wrapper.findComponent(NDrawer).props('show')).toBe(true)
  })
  test('dynamic slots never show tracked totals as capacity', async () => {
    vi.mocked(managerService.getWorkerResources).mockResolvedValue(
      snapshot([worker('localhost:5802', true)])
    )
    const { wrapper } = await setup()
    await flushPromises()
    expect(wrapper.text()).toContain('Dynamic — 3 used (no fixed capacity)')
    expect(wrapper.text()).not.toContain('3 / 4')
  })
  test('master page never reads worker resources or renders slots', async () => {
    vi.mocked(managerService.getMonitors).mockResolvedValue([
      monitor(),
      monitor('master', '5801', true)
    ])
    const { wrapper } = await setup('/managers/master')
    await flushPromises()
    expect(wrapper.text()).toContain('master:5801')
    expect(wrapper.text()).not.toContain('localhost')
    expect(wrapper.text()).not.toContain('Slots')
    expect(managerService.getWorkerResources).not.toHaveBeenCalled()
  })
  test('clears failed resource snapshots and recovers on the next refresh', async () => {
    const { wrapper } = await setup()
    await flushPromises()
    vi.mocked(managerService.getWorkerResources).mockResolvedValueOnce(snapshot([], false))
    await vi.advanceTimersByTimeAsync(30_000)
    expect(wrapper.text()).toContain('Worker resources are unavailable')
    expect(wrapper.text()).not.toContain('3 / 4')
    expect(wrapper.text()).not.toContain('Resource response time:')
    expect(wrapper.text()).toContain('localhost')
    await vi.advanceTimersByTimeAsync(30_000)
    expect(wrapper.text()).toContain('3 / 4')
    expect(wrapper.text()).not.toContain('Worker resources are unavailable')
  })
  test('keeps resources when monitoring fails and handles both endpoint failures without leaking errors', async () => {
    vi.mocked(managerService.getMonitors).mockRejectedValueOnce(new Error('secret server response'))
    const { wrapper } = await setup()
    await flushPromises()
    expect(wrapper.text()).toContain('System monitoring is unavailable')
    expect(wrapper.text()).toContain('3 / 4')
    expect(wrapper.text()).not.toContain('secret')
    vi.mocked(managerService.getMonitors).mockRejectedValueOnce(new Error('timeout'))
    vi.mocked(managerService.getWorkerResources).mockRejectedValueOnce(new Error('timeout'))
    await vi.advanceTimersByTimeAsync(30_000)
    expect(wrapper.text()).toContain('Worker resources are unavailable')
    expect(wrapper.findComponent(NDataTable).props('data')).toEqual([])
    expect(wrapper.findComponent(NDataTable).props('loading')).toBe(false)
  })
  test('does not overlap slow requests, ignores obsolete routes, and fetches the current route', async () => {
    const pending = deferred<Monitor[]>()
    vi.mocked(managerService.getMonitors).mockReturnValueOnce(pending.promise)
    const { wrapper, router } = await setup()
    await vi.advanceTimersByTimeAsync(90_000)
    expect(managerService.getMonitors).toHaveBeenCalledTimes(1)
    await router.push('/managers/master')
    await flushPromises()
    expect(managerService.getMonitors).toHaveBeenCalledTimes(1)
    vi.mocked(managerService.getMonitors).mockResolvedValue([monitor('new-master', '5801', true)])
    pending.resolve([monitor('old-worker')])
    await flushPromises()
    expect(wrapper.text()).toContain('new-master')
    expect(wrapper.text()).not.toContain('old-worker')
    expect(managerService.getMonitors).toHaveBeenCalledTimes(2)
    expect(managerService.getWorkerResources).toHaveBeenCalledTimes(1)
  })
  test('unmount ignores in-flight completion and never schedules another poll', async () => {
    const pending = deferred<Monitor[]>()
    vi.mocked(managerService.getMonitors).mockReturnValueOnce(pending.promise)
    const { wrapper } = await setup()
    wrapper.unmount()
    wrappers.length = 0
    pending.resolve([monitor()])
    await flushPromises()
    await vi.advanceTimersByTimeAsync(90_000)
    expect(managerService.getMonitors).toHaveBeenCalledTimes(1)
  })
  test('unmount cancels a scheduled poll and malformed records do not stop refresh', async () => {
    vi.mocked(managerService.getMonitors).mockResolvedValueOnce([null, {}, monitor()] as Monitor[])
    vi.mocked(managerService.getWorkerResources).mockResolvedValueOnce(
      snapshot([null, {}, worker()] as WorkerResource[])
    )
    const { wrapper } = await setup()
    await flushPromises()
    expect(wrapper.text()).toContain('3 / 4')
    await vi.advanceTimersByTimeAsync(30_000)
    expect(managerService.getMonitors).toHaveBeenCalledTimes(2)
    wrapper.unmount()
    wrappers.length = 0
    await vi.advanceTimersByTimeAsync(90_000)
    expect(managerService.getMonitors).toHaveBeenCalledTimes(2)
  })
})
