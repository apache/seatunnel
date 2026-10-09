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
  NForm,
  NFormItem,
  NInput,
  NLayout,
  NLayoutContent,
  NPopconfirm,
  NSpace,
  NTag,
  NTooltip
} from 'naive-ui'
import type { DataTableColumns } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import { managerService } from '@/service/manager'
import type { Monitor, WorkerResource, WorkerResourceSnapshot } from '@/service/manager/types'
import { isRequestOutcomeUnknown } from '@/service/service'
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
    // Tag editor state. Only the member that serves the current REST request can be edited, so the
    // selection is keyed by member UUID and re-resolved after every refresh.
    const selectedMonitor = ref<Monitor | null>(null)
    const tagContent = ref('')
    const tagMessage = ref('')
    const tagError = ref('')
    const tagLoading = ref(false)
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
          if (selectedMonitor.value) {
            selectedMonitor.value =
              monitors.find(
                (monitor) => monitor?.uuid && monitor.uuid === selectedMonitor.value?.uuid
              ) || null
          }
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
        selectedMonitor.value = null
        tagContent.value = ''
        tagMessage.value = ''
        tagError.value = ''
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

    const parseTags = () => {
      const tags: Record<string, string> = {}
      const tagKeys = new Set<string>()
      for (const rawLine of tagContent.value.split('\n')) {
        const line = rawLine.trim()
        if (!line) {
          continue
        }
        const separatorIndex = line.indexOf('=')
        const key = line.substring(0, separatorIndex).trim()
        if (!key) {
          throw new Error(t('managers.tagEditor.invalid'))
        }
        if (tagKeys.has(key)) {
          throw new Error(t('managers.tagEditor.duplicate'))
        }
        tagKeys.add(key)
        tags[key] = line.substring(separatorIndex + 1).trim()
      }
      return tags
    }

    // The poll loop allows one request in flight at a time. If a poll is already running it may
    // have sampled the member before the update was applied, so discard it and poll again instead
    // of trusting the mutation response.
    const refreshAfterTagUpdate = async () => {
      if (loading.value) {
        generation++
        return
      }
      await refresh()
    }

    const updateTags = async (clear = false) => {
      if (tagLoading.value) {
        return
      }
      tagMessage.value = ''
      tagError.value = ''
      if (!selectedMonitor.value?.uuid) {
        tagError.value = t('managers.tagEditor.workerRequired')
        return
      }
      if (!clear && !tagContent.value.trim()) {
        tagError.value = t('managers.tagEditor.contentRequired')
        return
      }
      let tags: Record<string, string>
      try {
        tags = clear ? {} : parseTags()
      } catch (error) {
        tagError.value = error instanceof Error ? error.message : t('managers.tagEditor.invalid')
        return
      }

      tagLoading.value = true
      try {
        await managerService.updateTags({
          uuid: selectedMonitor.value.uuid,
          tags
        })
        if (clear) {
          tagContent.value = ''
        }
        tagMessage.value = t('managers.tagEditor.success')
        await refreshAfterTagUpdate()
      } catch (error) {
        tagError.value = isRequestOutcomeUnknown(error)
          ? t('managers.tagEditor.outcomeUnknown')
          : t('managers.tagEditor.failed')
      } finally {
        tagLoading.value = false
      }
    }

    const selectMonitor = (monitor: Monitor) => {
      selectedMonitor.value = monitor
      tagContent.value = Object.entries(monitor.tags || {})
        .map(([key, value]) => `${key}=${value}`)
        .join('\n')
      tagMessage.value = ''
      tagError.value = ''
    }

    const formatTags = (tags?: Record<string, string>) => {
      const entries = Object.entries(tags || {})
      if (!entries.length) {
        return '—'
      }
      return entries.map(([key, value]) => `${key}=${value}`).join(', ')
    }

    const monitorValue = (row: NodeResources, field: keyof Monitor) => row.monitor?.[field] ?? '—'
    // Monitoring values are flat strings except the member tags, which are a structured map.
    const monitorFieldText = (field: string, value: unknown) =>
      field === 'tags' && value && typeof value === 'object'
        ? formatTags(value as Record<string, string>)
        : String(value ?? '—')
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
    const renderTags = (row: NodeResources) => {
      const monitor = row.monitor
      const isSelected = Boolean(monitor?.uuid && selectedMonitor.value?.uuid === monitor.uuid)
      return (
        <NSpace size="small" align="center">
          <span>{formatTags(monitor?.tags)}</span>
          {monitor?.localMember && (
            <NTag bordered={false} type="success">
              {t('managers.tagEditor.local')}
            </NTag>
          )}
          {!isMaster.value && monitor && (
            <NButton
              size="small"
              tertiary
              disabled={!monitor.localMember || !monitor.uuid}
              type={isSelected ? 'primary' : 'default'}
              onClick={() => selectMonitor(monitor)}
            >
              {t('managers.tagEditor.select')}
            </NButton>
          )}
          {!isMaster.value && monitor && !monitor.localMember && (
            <NTooltip>
              {{
                trigger: () => <NTag bordered={false}>{t('managers.tagEditor.remote')}</NTag>,
                default: () => t('managers.tagEditor.remoteHint')
              }}
            </NTooltip>
          )}
        </NSpace>
      )
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
        title: t('managers.tags'),
        key: 'tags',
        width: isMaster.value ? undefined : 260,
        render: (row) => renderTags(row)
      },
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
          {!isMaster.value && (
            <div class="w-full bg-white p-6 border border-gray-100 rounded-xl mb-6">
              <NSpace justify="space-between" align="center" class="pb-6">
                <h2 class="font-bold text-2xl">{t('managers.tagEditor.title')}</h2>
                <span>
                  {selectedMonitor.value
                    ? `${selectedMonitor.value.host}:${selectedMonitor.value.port}`
                    : t('managers.tagEditor.noWorkerSelected')}
                </span>
              </NSpace>
              {tagMessage.value && (
                <NAlert
                  class="mb-4"
                  type="success"
                  closable
                  onClose={() => (tagMessage.value = '')}
                >
                  {tagMessage.value}
                </NAlert>
              )}
              {tagError.value && (
                <NAlert class="mb-4" type="error" closable onClose={() => (tagError.value = '')}>
                  {tagError.value}
                </NAlert>
              )}
              <NForm labelPlacement="left" labelWidth={100}>
                <NFormItem label={t('managers.tagEditor.content')}>
                  <NInput
                    value={tagContent.value}
                    type="textarea"
                    placeholder={t('managers.tagEditor.placeholder')}
                    autosize={{ minRows: 3, maxRows: 8 }}
                    onUpdateValue={(value) => {
                      tagContent.value = value
                    }}
                  />
                </NFormItem>
                <NSpace justify="end">
                  <NPopconfirm
                    positiveText={t('managers.tagEditor.confirm')}
                    negativeText={t('managers.tagEditor.cancelConfirm')}
                    onPositiveClick={() => updateTags(true)}
                  >
                    {{
                      trigger: () => (
                        <NButton
                          loading={tagLoading.value}
                          disabled={!selectedMonitor.value || tagLoading.value}
                        >
                          {t('managers.tagEditor.clear')}
                        </NButton>
                      ),
                      default: () => t('managers.tagEditor.clearConfirmMessage')
                    }}
                  </NPopconfirm>
                  <NPopconfirm
                    positiveText={t('managers.tagEditor.confirm')}
                    negativeText={t('managers.tagEditor.cancelConfirm')}
                    onPositiveClick={() => updateTags()}
                  >
                    {{
                      trigger: () => (
                        <NButton
                          type="primary"
                          loading={tagLoading.value}
                          disabled={
                            !selectedMonitor.value || !tagContent.value.trim() || tagLoading.value
                          }
                        >
                          {t('managers.tagEditor.update')}
                        </NButton>
                      ),
                      default: () => t('managers.tagEditor.updateConfirmMessage')
                    }}
                  </NPopconfirm>
                </NSpace>
              </NForm>
            </div>
          )}
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
              scrollX={isMaster.value ? 1200 : 1460}
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
                        {monitorFieldText(field, value)}
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
