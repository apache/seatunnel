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
import { defineComponent, ref } from 'vue'
import {
  NAlert,
  NButton,
  NDataTable,
  NForm,
  NFormItem,
  NInput,
  NLayout,
  NLayoutContent,
  NSpace,
  NTag,
  NTooltip
} from 'naive-ui'
import { useI18n } from 'vue-i18n'
import type { DataTableColumns } from 'naive-ui'
import { managerService } from '@/service/manager'
import type { Monitor } from '@/service/manager/types'
import { useRoute } from 'vue-router'

export default defineComponent({
  setup() {
    const { t } = useI18n()
    const route = useRoute()
    const monitors = ref([] as Monitor[])
    const selectedMonitor = ref<Monitor | null>(null)
    const tagContent = ref('')
    const tagMessage = ref('')
    const tagError = ref('')
    const tagLoading = ref(false)

    const fetch = async () => {
      let res = await managerService.getMonitors()
      const isMaster = route?.path.endsWith('/master') || false
      res = res.filter((row) => row.isMaster === String(isMaster)) || []
      monitors.value = res
      if (selectedMonitor.value) {
        selectedMonitor.value =
          res.find((row) => row.uuid && row.uuid === selectedMonitor.value?.uuid) || null
      }
    }
    fetch()

    const parseTags = () => {
      const tags: Record<string, string> = {}
      for (const rawLine of tagContent.value.split('\n')) {
        const line = rawLine.trim()
        if (!line) {
          continue
        }
        const separatorIndex = line.indexOf('=')
        if (separatorIndex <= 0) {
          throw new Error(t('managers.tags.invalid'))
        }
        tags[line.substring(0, separatorIndex).trim()] = line.substring(separatorIndex + 1).trim()
      }
      return tags
    }

    const updateTags = async (clear = false) => {
      tagLoading.value = true
      tagMessage.value = ''
      tagError.value = ''
      try {
        if (!selectedMonitor.value?.uuid) {
          tagError.value = t('managers.tags.workerRequired')
          return
        }
        await managerService.updateTags({
          uuid: selectedMonitor.value.uuid,
          tags: clear ? {} : parseTags()
        })
        if (clear) {
          tagContent.value = ''
        }
        tagMessage.value = t('managers.tags.success')
      } catch (error) {
        tagError.value = error instanceof Error ? error.message : t('managers.tags.failed')
      } finally {
        tagLoading.value = false
      }
    }

    const selectMonitor = (row: Monitor) => {
      selectedMonitor.value = row
      tagContent.value = Object.entries(row.tags || {})
        .map(([key, value]) => `${key}=${value}`)
        .join('\n')
      tagMessage.value = ''
      tagError.value = ''
    }

    const formatTags = (tags?: Record<string, string>) => {
      const entries = Object.entries(tags || {})
      if (!entries.length) {
        return '-'
      }
      return entries.map(([key, value]) => `${key}=${value}`).join(', ')
    }

    function createColumns(): DataTableColumns<Monitor> {
      return [
        {
          title: 'Host',
          key: 'host'
        },
        {
          title: 'Port',
          key: 'port'
        },
        {
          title: 'Physical MEM',
          key: 'physical.memory.total'
        },
        {
          title: 'Heap MEM Used',
          key: 'heap.memory.used'
        },
        {
          title: 'Tags',
          key: 'tags',
          render: (row) => formatTags(row.tags)
        },
        {
          title: 'Action',
          key: 'actions',
          render(row) {
            const selected = Boolean(row.uuid && selectedMonitor.value?.uuid === row.uuid)
            return (
              <NSpace size="small" align="center">
                {row.localMember && (
                  <NTag bordered={false} type="success">
                    {t('managers.tags.local')}
                  </NTag>
                )}
                <NButton
                  size="small"
                  tertiary
                  disabled={!row.localMember}
                  type={selected ? 'primary' : 'default'}
                  onClick={() => selectMonitor(row)}
                >
                  {t('managers.tags.select')}
                </NButton>
                {!row.localMember && (
                  <NTooltip>
                    {{
                      trigger: () => <NTag bordered={false}>{t('managers.tags.remote')}</NTag>,
                      default: () => t('managers.tags.remoteHint')
                    }}
                  </NTooltip>
                )}
              </NSpace>
            )
          }
        }
      ]
    }

    const columns = createColumns()
    return () => (
      <NLayout>
        <NLayoutContent>
          {!route?.path.endsWith('/master') && (
            <div class="w-full bg-white p-6 border border-gray-100 rounded-xl mb-6">
              <NSpace justify="space-between" align="center" class="pb-6">
                <h2 class="font-bold text-2xl">{t('managers.tags.title')}</h2>
                <span>
                  {selectedMonitor.value
                    ? `${selectedMonitor.value.host}:${selectedMonitor.value.port}`
                    : t('managers.tags.noWorkerSelected')}
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
                <NFormItem label={t('managers.tags.content')}>
                  <NInput
                    value={tagContent.value}
                    type="textarea"
                    placeholder={t('managers.tags.placeholder')}
                    autosize={{ minRows: 3, maxRows: 8 }}
                    onUpdateValue={(value) => {
                      tagContent.value = value
                    }}
                  />
                </NFormItem>
                <NSpace justify="end">
                  <NButton
                    loading={tagLoading.value}
                    disabled={!selectedMonitor.value}
                    onClick={() => updateTags(true)}
                  >
                    {t('managers.tags.clear')}
                  </NButton>
                  <NButton
                    type="primary"
                    loading={tagLoading.value}
                    disabled={!selectedMonitor.value}
                    onClick={() => updateTags()}
                  >
                    {t('managers.tags.update')}
                  </NButton>
                </NSpace>
              </NForm>
            </div>
          )}
          <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
            <h2 class="font-bold text-2xl pb-6">{t('managers.managers')}</h2>
            <NDataTable
              columns={columns}
              data={monitors.value}
              pagination={false}
              bordered={false}
            />
          </div>
        </NLayoutContent>
      </NLayout>
    )
  }
})
