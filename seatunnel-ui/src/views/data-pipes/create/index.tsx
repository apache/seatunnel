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
import { useI18n } from 'vue-i18n'
import { useRouter } from 'vue-router'
import {
  NBreadcrumb,
  NBreadcrumbItem,
  NButton,
  NCard,
  NIcon,
  NInput,
  NSpace,
  NTooltip,
  NDropdown
} from 'naive-ui'
import { BulbOutlined } from '@vicons/antd'
import MonacoEditor from '@/components/monaco-editor'
import Log from '@/components/log'
import ConfigurationModal from './components/configuration-modal'
import type { Router } from 'vue-router'
import type { Ref } from 'vue'

const DataPipesCreate = defineComponent({
  setup() {
    const { t } = useI18n()
    const router: Router = useRouter()
    const showConfigurationModal: Ref<boolean> = ref(false)
    const configurationType: Ref<
      'engine-parameter' | 'self-defined-parameter'
    > = ref('engine-parameter')

    const handleClickDataPipes = () => {
      router.push({ path: '/data-pipes/list' })
    }

    const handleSelectConfiguration = (
      key: 'engine-parameter' | 'self-defined-parameter'
    ) => {
      configurationType.value = key
      showConfigurationModal.value = true
    }

    const handleCancelConfigurationModal = () => {
      showConfigurationModal.value = false
    }

    const handleConfirmConfigurationModal = () => {
      showConfigurationModal.value = false
    }

    return {
      t,
      showConfigurationModal,
      configurationType,
      handleClickDataPipes,
      handleSelectConfiguration,
      handleCancelConfigurationModal,
      handleConfirmConfigurationModal
    }
  },
  render() {
    return (
      <NSpace vertical>
        <NCard>
          {{
            header: () => (
              <NSpace align='center'>
                <NBreadcrumb>
                  <NBreadcrumbItem onClick={this.handleClickDataPipes}>
                    {this.t('data_pipes.data_pipes')}
                  </NBreadcrumbItem>
                  <NBreadcrumbItem>
                    {this.t('data_pipes.create')}
                  </NBreadcrumbItem>
                </NBreadcrumb>
              </NSpace>
            ),
            'header-extra': () => (
              <NSpace>
                <NButton secondary>{this.t('data_pipes.cancel')}</NButton>
                <NButton secondary>{this.t('data_pipes.save')}</NButton>
              </NSpace>
            )
          }}
        </NCard>
        <NCard>
          <NSpace align='center'>
            <span>{this.t('data_pipes.name')}</span>
            <NSpace align='center'>
              <NInput
                clearable
                maxlength='100'
                showCount
                style={{ width: '600px' }}
              />
              <NTooltip placement='right' trigger='hover'>
                {{
                  default: () => <span>{this.t('data_pipes.name_tips')}</span>,
                  trigger: () => (
                    <NIcon size='20' style={{ cursor: 'pointer' }}>
                      <BulbOutlined />
                    </NIcon>
                  )
                }}
              </NTooltip>
            </NSpace>
          </NSpace>
        </NCard>
        <NCard>
          <NSpace vertical>
            <NSpace justify='end'>
              <NButton secondary>{this.t('data_pipes.execute')}</NButton>
              <NButton secondary>{this.t('data_pipes.kill')}</NButton>
              <NButton secondary>{this.t('data_pipes.stop')}</NButton>
              <NDropdown
                trigger='click'
                options={[
                  {
                    label: this.t('data_pipes.engine_parameter'),
                    key: 'engine-parameter'
                  },
                  {
                    label: this.t('data_pipes.self_defined_parameter'),
                    key: 'self-defined-parameter'
                  }
                ]}
                onSelect={this.handleSelectConfiguration}
              >
                <NButton secondary>
                  {this.t('data_pipes.configuration')}
                </NButton>
              </NDropdown>
            </NSpace>
            <MonacoEditor />
          </NSpace>
        </NCard>
        <NCard>
          <Log />
        </NCard>
        <ConfigurationModal
          type={this.configurationType}
          showModal={this.showConfigurationModal}
          onCancelModal={this.handleCancelConfigurationModal}
          onConfirmModal={this.handleConfirmConfigurationModal}
        />
      </NSpace>
    )
  }
})

export default DataPipesCreate
