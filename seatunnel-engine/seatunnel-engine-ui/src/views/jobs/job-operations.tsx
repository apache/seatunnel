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
  NForm,
  NFormItem,
  NInput,
  NPopconfirm,
  NSelect,
  NSpace,
  NSwitch,
  NTabPane,
  NTabs,
  NUpload
} from 'naive-ui'
import type { SelectOption, UploadFileInfo } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useRoute } from 'vue-router'
import { JobsService } from '@/service/job'
import { isRequestOutcomeUnknown } from '@/service/service'
import type { ConfigFormat, RestoreMode, SubmitJobResponse } from '@/service/job/types'

type FeedbackType = 'success' | 'warning' | 'error'

interface Feedback {
  type: FeedbackType
  message: string
}

const configFormatOptions: SelectOption[] = [
  { label: 'HOCON', value: 'hocon' },
  { label: 'JSON', value: 'json' },
  { label: 'SQL', value: 'sql' }
]

const restoreModeOptions: SelectOption[] = [
  { label: 'Checkpoint', value: 'CHECKPOINT' },
  { label: 'Savepoint', value: 'SAVEPOINT' }
]

const isRestoreMode = (value: unknown): value is RestoreMode =>
  value === 'CHECKPOINT' || value === 'SAVEPOINT'

export default defineComponent({
  setup() {
    const { t } = useI18n()
    const route = useRoute()
    const initialRestoreSourceJobId =
      typeof route.query.restoreSourceJobId === 'string' ? route.query.restoreSourceJobId : ''
    const routeRestoreMode = route.query.restoreMode
    const initialRestoreMode: RestoreMode = isRestoreMode(routeRestoreMode)
      ? routeRestoreMode
      : 'SAVEPOINT'
    const textJobName = ref('')
    const fileJobName = ref('')
    const textRestoreSourceJobId = ref(initialRestoreSourceJobId)
    const fileRestoreSourceJobId = ref(initialRestoreSourceJobId)
    const textRestoreEnabled = ref(Boolean(initialRestoreSourceJobId))
    const fileRestoreEnabled = ref(Boolean(initialRestoreSourceJobId))
    const textRestoreMode = ref<RestoreMode>(initialRestoreMode)
    const fileRestoreMode = ref<RestoreMode>(initialRestoreMode)
    const configFormat = ref<ConfigFormat>('hocon')
    const configContent = ref('')
    const fileList = ref<UploadFileInfo[]>([])
    const feedback = ref<Feedback | null>(null)
    const submittingText = ref(false)
    const submittingFile = ref(false)

    const setFeedback = (type: FeedbackType, message: string) => {
      feedback.value = { type, message }
    }

    const formatSubmitSuccess = (response: SubmitJobResponse) => {
      const jobLabel = response.jobName
        ? `${response.jobName} (${response.jobId})`
        : `${response.jobId}`
      return t('jobs.operations.submitSuccess', { job: jobLabel })
    }

    const submitText = async () => {
      if (submittingText.value) {
        return
      }
      if (!configContent.value.trim()) {
        setFeedback('warning', t('jobs.operations.configRequired'))
        return
      }
      if (textRestoreEnabled.value && !textRestoreSourceJobId.value.trim()) {
        setFeedback('warning', t('jobs.operations.restoreSourceJobIdRequired'))
        return
      }

      submittingText.value = true
      feedback.value = null
      try {
        const response = await JobsService.submitJob({
          config: configContent.value,
          format: configFormat.value,
          jobName: textJobName.value,
          restoreMode: textRestoreEnabled.value ? textRestoreMode.value : undefined,
          restoreSourceJobId: textRestoreEnabled.value ? textRestoreSourceJobId.value : undefined
        })
        setFeedback('success', formatSubmitSuccess(response))
      } catch (error) {
        setFeedback(
          'error',
          isRequestOutcomeUnknown(error)
            ? t('jobs.operations.submitOutcomeUnknown')
            : t('jobs.operations.submitFailed')
        )
      } finally {
        submittingText.value = false
      }
    }

    const submitFile = async () => {
      if (submittingFile.value) {
        return
      }
      const file = fileList.value[0]?.file
      if (!file) {
        setFeedback('warning', t('jobs.operations.fileRequired'))
        return
      }
      if (fileRestoreEnabled.value && !fileRestoreSourceJobId.value.trim()) {
        setFeedback('warning', t('jobs.operations.restoreSourceJobIdRequired'))
        return
      }

      submittingFile.value = true
      feedback.value = null
      try {
        const response = await JobsService.submitJobByUploadFile({
          file,
          jobName: fileJobName.value,
          restoreMode: fileRestoreEnabled.value ? fileRestoreMode.value : undefined,
          restoreSourceJobId: fileRestoreEnabled.value ? fileRestoreSourceJobId.value : undefined
        })
        setFeedback('success', formatSubmitSuccess(response))
      } catch (error) {
        setFeedback(
          'error',
          isRequestOutcomeUnknown(error)
            ? t('jobs.operations.submitOutcomeUnknown')
            : t('jobs.operations.submitFailed')
        )
      } finally {
        submittingFile.value = false
      }
    }

    return () => (
      <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
        <h2 class="font-bold text-2xl pb-4">{t('jobs.operations.title')}</h2>
        {feedback.value && (
          <NAlert
            class="mb-4"
            type={feedback.value.type}
            closable
            onClose={() => {
              feedback.value = null
            }}
          >
            {feedback.value.message}
          </NAlert>
        )}
        <NTabs type="line" animated>
          <NTabPane name="text" tab={t('jobs.operations.textSubmit')}>
            <NForm labelPlacement="left" labelWidth={120}>
              <NFormItem label={t('jobs.operations.jobName')}>
                <NInput
                  value={textJobName.value}
                  placeholder={t('jobs.operations.jobNamePlaceholder')}
                  onUpdateValue={(value) => {
                    textJobName.value = value
                  }}
                />
              </NFormItem>
              <NFormItem label={t('jobs.operations.configFormat')}>
                <NSelect
                  value={configFormat.value}
                  options={configFormatOptions}
                  onUpdateValue={(value) => {
                    configFormat.value = value as ConfigFormat
                  }}
                />
              </NFormItem>
              <NFormItem label={t('jobs.operations.restore')}>
                <NSwitch
                  value={textRestoreEnabled.value}
                  onUpdateValue={(value) => {
                    textRestoreEnabled.value = value
                  }}
                />
              </NFormItem>
              {textRestoreEnabled.value && (
                <>
                  <NFormItem label={t('jobs.operations.restoreMode')}>
                    <NSelect
                      value={textRestoreMode.value}
                      options={restoreModeOptions}
                      onUpdateValue={(value) => {
                        textRestoreMode.value = value as RestoreMode
                      }}
                    />
                  </NFormItem>
                  <NFormItem label={t('jobs.operations.restoreSourceJobId')}>
                    <NInput
                      value={textRestoreSourceJobId.value}
                      placeholder={t('jobs.operations.restoreSourceJobIdPlaceholder')}
                      onUpdateValue={(value) => {
                        textRestoreSourceJobId.value = value
                      }}
                    />
                  </NFormItem>
                </>
              )}
              <NFormItem label={t('jobs.operations.configContent')}>
                <NInput
                  value={configContent.value}
                  type="textarea"
                  placeholder={t('jobs.operations.configPlaceholder')}
                  autosize={{ minRows: 8, maxRows: 18 }}
                  onUpdateValue={(value) => {
                    configContent.value = value
                  }}
                />
              </NFormItem>
              <NSpace justify="end">
                <NButton
                  onClick={() => {
                    configContent.value = ''
                    textJobName.value = ''
                    textRestoreSourceJobId.value = ''
                    textRestoreEnabled.value = false
                    textRestoreMode.value = 'SAVEPOINT'
                  }}
                >
                  {t('jobs.operations.reset')}
                </NButton>
                <NPopconfirm
                  positiveText={t('jobs.operations.confirm')}
                  negativeText={t('jobs.operations.cancelConfirm')}
                  onPositiveClick={submitText}
                >
                  {{
                    trigger: () => (
                      <NButton
                        type="primary"
                        loading={submittingText.value}
                        disabled={
                          !configContent.value.trim() ||
                          (textRestoreEnabled.value && !textRestoreSourceJobId.value.trim())
                        }
                      >
                        {t('jobs.operations.submit')}
                      </NButton>
                    ),
                    default: () => t('jobs.operations.submitConfirmMessage')
                  }}
                </NPopconfirm>
              </NSpace>
            </NForm>
          </NTabPane>
          <NTabPane name="file" tab={t('jobs.operations.fileSubmit')}>
            <NForm labelPlacement="left" labelWidth={120}>
              <NFormItem label={t('jobs.operations.jobName')}>
                <NInput
                  value={fileJobName.value}
                  placeholder={t('jobs.operations.jobNamePlaceholder')}
                  onUpdateValue={(value) => {
                    fileJobName.value = value
                  }}
                />
              </NFormItem>
              <NFormItem label={t('jobs.operations.configFile')}>
                <NUpload
                  accept=".json,.conf,.config,.sql"
                  defaultUpload={false}
                  max={1}
                  fileList={fileList.value}
                  onUpdateFileList={(nextFileList) => {
                    fileList.value = nextFileList
                  }}
                >
                  {{
                    default: () => <NButton>{t('jobs.operations.chooseFile')}</NButton>
                  }}
                </NUpload>
              </NFormItem>
              <NFormItem label={t('jobs.operations.restore')}>
                <NSwitch
                  value={fileRestoreEnabled.value}
                  onUpdateValue={(value) => {
                    fileRestoreEnabled.value = value
                  }}
                />
              </NFormItem>
              {fileRestoreEnabled.value && (
                <>
                  <NFormItem label={t('jobs.operations.restoreMode')}>
                    <NSelect
                      value={fileRestoreMode.value}
                      options={restoreModeOptions}
                      onUpdateValue={(value) => {
                        fileRestoreMode.value = value as RestoreMode
                      }}
                    />
                  </NFormItem>
                  <NFormItem label={t('jobs.operations.restoreSourceJobId')}>
                    <NInput
                      value={fileRestoreSourceJobId.value}
                      placeholder={t('jobs.operations.restoreSourceJobIdPlaceholder')}
                      onUpdateValue={(value) => {
                        fileRestoreSourceJobId.value = value
                      }}
                    />
                  </NFormItem>
                </>
              )}
              <NSpace justify="end">
                <NButton
                  onClick={() => {
                    fileList.value = []
                    fileJobName.value = ''
                    fileRestoreSourceJobId.value = ''
                    fileRestoreEnabled.value = false
                    fileRestoreMode.value = 'SAVEPOINT'
                  }}
                >
                  {t('jobs.operations.reset')}
                </NButton>
                <NPopconfirm
                  positiveText={t('jobs.operations.confirm')}
                  negativeText={t('jobs.operations.cancelConfirm')}
                  onPositiveClick={submitFile}
                >
                  {{
                    trigger: () => (
                      <NButton
                        type="primary"
                        loading={submittingFile.value}
                        disabled={
                          !fileList.value[0]?.file ||
                          (fileRestoreEnabled.value && !fileRestoreSourceJobId.value.trim())
                        }
                      >
                        {t('jobs.operations.submit')}
                      </NButton>
                    ),
                    default: () => t('jobs.operations.submitConfirmMessage')
                  }}
                </NPopconfirm>
              </NSpace>
            </NForm>
          </NTabPane>
        </NTabs>
      </div>
    )
  }
})
