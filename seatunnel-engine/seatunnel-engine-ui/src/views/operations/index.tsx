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

import { defineComponent, onMounted, ref } from 'vue'
import {
  NAlert,
  NButton,
  NDataTable,
  NDescriptions,
  NDescriptionsItem,
  NForm,
  NFormItem,
  NInput,
  NLayout,
  NLayoutContent,
  NSelect,
  NSpace,
  NTag,
  type DataTableColumns
} from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { operationsService } from '@/service/operations'
import type {
  ConditionRule,
  HttpServiceStatus,
  OptionMetadata,
  OptionRuleResponse,
  PluginType,
  RequiredOptionRule,
  ValueConstraint
} from '@/service/operations/types'

type OptionRow = OptionMetadata & {
  section: string
  ruleType: string
  expression: string
}

interface ConditionRuleRow {
  expression: string
  requiredCount: number
  optionalCount: number
  nestedConditionCount: number
}

interface ValueConstraintRow {
  expression: string
  condition: string
}

export default defineComponent({
  setup() {
    const { t } = useI18n()
    const pluginType = ref<PluginType>('source')
    const pluginName = ref('FakeSource')
    const optionRules = ref<OptionRuleResponse | null>(null)
    const httpStatus = ref<HttpServiceStatus | null>(null)
    const optionLoading = ref(false)
    const statusLoading = ref(false)
    const optionError = ref('')
    const statusError = ref('')

    const pluginTypeOptions = [
      { label: 'Source', value: 'source' },
      { label: 'Sink', value: 'sink' },
      { label: 'Transform', value: 'transform' }
    ]

    const optionRows = () => {
      const optional =
        optionRules.value?.optionRule?.optionalOptions?.map((option) => ({
          ...option,
          section: t('operations.optionRules.optional'),
          ruleType: '-',
          expression: '-'
        })) || []
      const required =
        optionRules.value?.optionRule?.requiredOptions?.flatMap((rule: RequiredOptionRule) =>
          (rule.options || []).map((option) => ({
            ...option,
            section: t('operations.optionRules.required'),
            ruleType: rule.ruleType,
            expression: rule.expression || '-'
          }))
        ) || []
      return [...required, ...optional]
    }

    const conditionRows = (): ConditionRuleRow[] =>
      (optionRules.value?.optionRule?.conditionRules || []).map((rule: ConditionRule) => ({
        expression: rule.expression || '-',
        requiredCount: rule.optionRule?.requiredOptions?.length || 0,
        optionalCount: rule.optionRule?.optionalOptions?.length || 0,
        nestedConditionCount: rule.optionRule?.conditionRules?.length || 0
      }))

    const valueConstraintRows = (): ValueConstraintRow[] =>
      (optionRules.value?.optionRule?.valueConstraints || []).map((rule: ValueConstraint) => ({
        expression: rule.expression || '-',
        condition: rule.conditionTree ? JSON.stringify(rule.conditionTree) : '-'
      }))

    const fetchOptionRules = async () => {
      if (!pluginName.value.trim()) {
        optionError.value = t('operations.optionRules.pluginRequired')
        return
      }

      optionLoading.value = true
      optionError.value = ''
      try {
        optionRules.value = await operationsService.getOptionRules(
          pluginType.value,
          pluginName.value.trim()
        )
      } catch (e) {
        optionRules.value = null
        optionError.value = t('operations.optionRules.loadFailed')
      } finally {
        optionLoading.value = false
      }
    }

    const fetchHttpStatus = async () => {
      statusLoading.value = true
      statusError.value = ''
      try {
        httpStatus.value = await operationsService.getHttpServiceStatus()
      } catch (e) {
        httpStatus.value = null
        statusError.value = t('operations.httpStatus.loadFailed')
      } finally {
        statusLoading.value = false
      }
    }

    onMounted(() => {
      fetchOptionRules()
      fetchHttpStatus()
    })

    const optionColumns: DataTableColumns<OptionRow> = [
      { title: t('operations.optionRules.section'), key: 'section' },
      { title: t('operations.optionRules.ruleType'), key: 'ruleType' },
      { title: t('operations.optionRules.expression'), key: 'expression' },
      { title: t('operations.optionRules.key'), key: 'key' },
      { title: t('operations.optionRules.type'), key: 'type' },
      {
        title: t('operations.optionRules.defaultValue'),
        key: 'defaultValue',
        render: (row) =>
          row.defaultValue === undefined || row.defaultValue === null
            ? '-'
            : String(row.defaultValue)
      },
      {
        title: t('operations.optionRules.description'),
        key: 'description',
        render: (row) => row.description || '-'
      }
    ]

    const conditionColumns: DataTableColumns<ConditionRuleRow> = [
      { title: t('operations.optionRules.expression'), key: 'expression' },
      {
        title: t('operations.optionRules.requiredCount'),
        key: 'requiredCount'
      },
      {
        title: t('operations.optionRules.optionalCount'),
        key: 'optionalCount'
      },
      {
        title: t('operations.optionRules.conditionCount'),
        key: 'nestedConditionCount'
      }
    ]

    const valueConstraintColumns: DataTableColumns<ValueConstraintRow> = [
      { title: t('operations.optionRules.expression'), key: 'expression' },
      { title: t('operations.optionRules.condition'), key: 'condition' }
    ]

    const enabledTag = (enabled?: boolean) => (
      <NTag bordered={false} type={enabled ? 'success' : 'default'}>
        {enabled ? t('operations.httpStatus.enabled') : t('operations.httpStatus.disabled')}
      </NTag>
    )

    return () => (
      <NLayout>
        <NLayoutContent>
          <NSpace vertical size="large">
            <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
              <h2 class="font-bold text-2xl pb-6">{t('operations.optionRules.title')}</h2>
              <NForm labelPlacement="left" labelWidth={120}>
                <NFormItem label={t('operations.optionRules.pluginType')}>
                  <NSelect
                    value={pluginType.value}
                    options={pluginTypeOptions}
                    onUpdateValue={(value) => {
                      pluginType.value = value as PluginType
                    }}
                  />
                </NFormItem>
                <NFormItem label={t('operations.optionRules.pluginName')}>
                  <NInput
                    value={pluginName.value}
                    placeholder={t('operations.optionRules.pluginNamePlaceholder')}
                    onUpdateValue={(value) => {
                      pluginName.value = value
                    }}
                  />
                </NFormItem>
                <NSpace justify="end">
                  <NButton type="primary" loading={optionLoading.value} onClick={fetchOptionRules}>
                    {t('operations.optionRules.load')}
                  </NButton>
                </NSpace>
              </NForm>
              {optionError.value && (
                <NAlert class="mt-4" type="error">
                  {optionError.value}
                </NAlert>
              )}
              <NDataTable
                class="mt-4"
                columns={optionColumns}
                data={optionRows()}
                loading={optionLoading.value}
                pagination={{ pageSize: 10 }}
                bordered={false}
              />
              <h3 class="font-bold text-lg pt-6 pb-3">
                {t('operations.optionRules.conditionRules')}
              </h3>
              <NDataTable
                columns={conditionColumns}
                data={conditionRows()}
                loading={optionLoading.value}
                pagination={false}
                bordered={false}
              />
              <h3 class="font-bold text-lg pt-6 pb-3">
                {t('operations.optionRules.valueConstraints')}
              </h3>
              <NDataTable
                columns={valueConstraintColumns}
                data={valueConstraintRows()}
                loading={optionLoading.value}
                pagination={false}
                bordered={false}
              />
            </div>
            <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
              <NSpace justify="space-between" align="center" class="pb-6">
                <h2 class="font-bold text-2xl">{t('operations.httpStatus.title')}</h2>
                <NButton loading={statusLoading.value} onClick={fetchHttpStatus}>
                  {t('operations.httpStatus.refresh')}
                </NButton>
              </NSpace>
              {statusError.value && (
                <NAlert class="mb-4" type="error">
                  {statusError.value}
                </NAlert>
              )}
              <NDescriptions bordered column={2}>
                <NDescriptionsItem label={t('operations.httpStatus.http')}>
                  {enabledTag(httpStatus.value?.httpEnabled)}
                </NDescriptionsItem>
                <NDescriptionsItem label={t('operations.httpStatus.httpPort')}>
                  {httpStatus.value?.httpPort ?? '-'}
                </NDescriptionsItem>
                <NDescriptionsItem label={t('operations.httpStatus.https')}>
                  {enabledTag(httpStatus.value?.httpsEnabled)}
                </NDescriptionsItem>
                <NDescriptionsItem label={t('operations.httpStatus.httpsPort')}>
                  {httpStatus.value?.httpsPort ?? '-'}
                </NDescriptionsItem>
                <NDescriptionsItem label={t('operations.httpStatus.contextPath')}>
                  {httpStatus.value?.contextPath || '/'}
                </NDescriptionsItem>
                <NDescriptionsItem label={t('operations.httpStatus.dynamicPort')}>
                  {enabledTag(httpStatus.value?.dynamicPortEnabled)}
                </NDescriptionsItem>
                <NDescriptionsItem label={t('operations.httpStatus.basicAuth')}>
                  {enabledTag(httpStatus.value?.basicAuthEnabled)}
                </NDescriptionsItem>
                <NDescriptionsItem label={t('operations.httpStatus.mutualTls')}>
                  {enabledTag(httpStatus.value?.mutualTlsEnabled)}
                </NDescriptionsItem>
              </NDescriptions>
            </div>
          </NSpace>
        </NLayoutContent>
      </NLayout>
    )
  }
})
