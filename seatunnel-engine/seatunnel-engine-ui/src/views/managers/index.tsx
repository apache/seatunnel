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

import { computed, defineComponent, onBeforeUnmount, ref, watch } from 'vue'
import {
  NAlert,
  NButton,
  NDataTable,
  NDescriptions,
  NDescriptionsItem,
  NDrawer,
  NDrawerContent,
  NLayout,
  NLayoutContent,
  NSpace
} from 'naive-ui'
import type { DataTableColumns } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import { managerService } from '@/service/manager'
import type { Monitor, WorkerResource, WorkerResourceSnapshot } from '@/service/manager/types'
import { bytesValue, joinResources, numberValue, ratioValue } from './resources'
import type { NodeResources } from './resources'

export default defineComponent({
  setup() {
    const { t } = useI18n()
    const route = useRoute()
    const isMaster = computed(() => route?.path.endsWith('/master') || false)
    const rows = ref<NodeResources[]>([])
    const loading = ref(false)
    const monitorUnavailable = ref(false)
    const resourceUnavailable = ref(false)
    const collectedAt = ref<number>()
    const selectedAddress = ref<string>()
    const selected = computed(() => rows.value.find((row) => row.address === selectedAddress.value))
    let timer: ReturnType<typeof setTimeout> | undefined
    let generation = 0
    let disposed = false
    const isHidden = () => document.visibilityState === 'hidden'

    const refresh = async () => {
      if (disposed || isHidden() || loading.value) return
      clearTimeout(timer)
      loading.value = true
      const requestGeneration = generation
      const master = isMaster.value
      try {
        const [monitorResult, resourceResult] = await Promise.allSettled([
          managerService.getMonitors(),
          master ? Promise.resolve(undefined) : managerService.getWorkerResources()
        ])
        if (!disposed && requestGeneration === generation) {
          const monitors: Monitor[] =
            monitorResult.status === 'fulfilled' && Array.isArray(monitorResult.value)
              ? monitorResult.value
              : []
          monitorUnavailable.value =
            monitorResult.status === 'rejected' || !Array.isArray(monitorResult.value)
          const snapshot: WorkerResourceSnapshot | undefined =
            resourceResult.status === 'fulfilled' ? resourceResult.value : undefined
          const available = snapshot?.available === true && Array.isArray(snapshot.workers)
          resourceUnavailable.value = !master && !available
          // Replace, rather than retain, old resource values after failed refreshes.
          rows.value = joinResources(monitors, available ? snapshot!.workers : [], master)
          collectedAt.value =
            available && Number.isFinite(snapshot!.collectedAt) && snapshot!.collectedAt > 0
              ? snapshot!.collectedAt
              : undefined
        }
      } finally {
        loading.value = false
        if (!disposed && !isHidden()) {
          if (requestGeneration !== generation) void refresh()
          else timer = setTimeout(refresh, 30_000)
        }
      }
    }

    const onVisibilityChange = () => {
      clearTimeout(timer)
      if (isHidden()) {
        // Discard the pending sample and refresh after it settles if visibility returns first.
        generation++
      } else {
        void refresh()
      }
    }
    document.addEventListener('visibilitychange', onVisibilityChange)

    watch(
      isMaster,
      () => {
        generation++
        clearTimeout(timer)
        rows.value = []
        selectedAddress.value = undefined
        collectedAt.value = undefined
        monitorUnavailable.value = false
        resourceUnavailable.value = false
        void refresh()
      },
      { immediate: true }
    )
    onBeforeUnmount(() => {
      disposed = true
      clearTimeout(timer)
      document.removeEventListener('visibilitychange', onVisibilityChange)
    })

    const monitorValue = (row: NodeResources, field: keyof Monitor) => row.monitor?.[field] ?? '—'
    const slots = (resource?: WorkerResource) => {
      if (typeof resource?.dynamicSlot !== 'boolean') return '—'
      if (resource.dynamicSlot) {
        return t('managers.dynamic_used', { used: numberValue(resource.usedSlots) })
      }
      return t('managers.fixed_slots', {
        used: numberValue(resource.usedSlots),
        total: numberValue(resource.totalSlots),
        free: numberValue(resource.freeSlots)
      })
    }
    const columns = computed<DataTableColumns<NodeResources>>(() => [
      { title: t('managers.address'), key: 'address' },
      { title: t('managers.cpu'), key: 'cpu', render: (row) => monitorValue(row, 'load.process') },
      {
        title: t('managers.heap'),
        key: 'heap',
        render: (row) =>
          `${monitorValue(row, 'heap.memory.used')} / ${monitorValue(row, 'heap.memory.max')}`
      },
      {
        title: t('managers.physical'),
        key: 'physical',
        render: (row) => monitorValue(row, 'physical.memory.total')
      },
      {
        title: t('managers.gc'),
        key: 'gc',
        render: (row) =>
          `${monitorValue(row, 'minor.gc.count')} / ${monitorValue(row, 'major.gc.count')}`
      },
      {
        title: t('managers.threads'),
        key: 'threads',
        render: (row) => monitorValue(row, 'thread.count')
      },
      ...(!isMaster.value
        ? [
            {
              title: t('managers.slots'),
              key: 'slots',
              width: 240,
              render: (row: NodeResources) => (
                <span style={{ display: 'block', whiteSpace: 'normal', overflowWrap: 'anywhere' }}>
                  {slots(row.resource)}
                </span>
              )
            }
          ]
        : []),
      {
        title: t('managers.details'),
        key: 'details',
        fixed: isMaster.value ? 'right' : undefined,
        width: 95,
        render: (row) => (
          <NButton
            size="small"
            onClick={() => {
              selectedAddress.value = row.address
            }}
          >
            {t('managers.details')}
          </NButton>
        )
      }
    ])
    const resourceDetails = (resource: WorkerResource) => [
      [t('managers.slots'), slots(resource)],
      [
        t('managers.cpu_resources'),
        `${numberValue(resource.availableCpuCores)} / ${numberValue(resource.totalCpuCores)}`
      ],
      [
        t('managers.heap_resources'),
        `${bytesValue(resource.availableHeapMemoryBytes)} / ${bytesValue(resource.totalHeapMemoryBytes)}`
      ],
      [t('managers.cpu_usage'), ratioValue(resource.cpuUsage)],
      [t('managers.memory_usage'), ratioValue(resource.memUsage)],
      [
        t('managers.running_jobs'),
        Array.isArray(resource.runningJobIds) ? String(resource.runningJobIds.length) : '—'
      ],
      [t('managers.tags'), resource.tags ? JSON.stringify(resource.tags) : '—']
    ]

    return () => (
      <NLayout>
        <NLayoutContent>
          <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
            <NSpace justify="space-between">
              <h2 class="font-bold text-2xl pb-6">{t('managers.managers')}</h2>
              <NButton
                loading={loading.value}
                disabled={loading.value}
                onClick={() => void refresh()}
              >
                {t('managers.refresh')}
              </NButton>
            </NSpace>
            <p class="pb-3">{t('managers.refresh_hint')}</p>
            {monitorUnavailable.value && (
              <NAlert type="warning">{t('managers.monitor_unavailable')}</NAlert>
            )}
            {resourceUnavailable.value && (
              <NAlert type="warning">{t('managers.resource_unavailable')}</NAlert>
            )}
            {!isMaster.value && (
              <p class="py-3">
                {t('managers.snapshot_hint')}
                {collectedAt.value &&
                  ` ${t('managers.collected_at')}: ${new Date(collectedAt.value).toLocaleString()}`}
              </p>
            )}
            <NDataTable
              columns={columns.value}
              data={rows.value}
              loading={loading.value}
              rowKey={(row: NodeResources) => row.address}
              pagination={{ pageSize: 20 }}
              tableLayout={isMaster.value ? 'auto' : 'fixed'}
              scrollX={1200}
              bordered={false}
            />
            <NDrawer
              show={!!selected.value}
              width="min(640px, 100vw)"
              onUpdateShow={(show) => {
                if (!show) selectedAddress.value = undefined
              }}
            >
              <NDrawerContent title={selected.value?.address} closable>
                {selected.value?.resource && (
                  <>
                    <h3>{t('managers.resources')}</h3>
                    <NDescriptions column={1} bordered>
                      {resourceDetails(selected.value.resource).map(([label, value]) => (
                        <NDescriptionsItem key={label} label={label}>
                          {value}
                        </NDescriptionsItem>
                      ))}
                    </NDescriptions>
                  </>
                )}
                {!isMaster.value && !selected.value?.resource && (
                  <NAlert type="warning">{t('managers.resource_missing')}</NAlert>
                )}
                <h3 class="py-3">{t('managers.monitoring')}</h3>
                {selected.value?.monitor ? (
                  <NDescriptions column={1} bordered>
                    {Object.entries(selected.value.monitor).map(([field, value]) => (
                      <NDescriptionsItem key={field} label={field}>
                        {String(value ?? '—')}
                      </NDescriptionsItem>
                    ))}
                  </NDescriptions>
                ) : (
                  <NAlert type="warning">{t('managers.monitor_missing')}</NAlert>
                )}
              </NDrawerContent>
            </NDrawer>
          </div>
        </NLayoutContent>
      </NLayout>
    )
  }
})
