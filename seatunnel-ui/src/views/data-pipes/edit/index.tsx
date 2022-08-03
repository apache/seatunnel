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

import { defineComponent } from 'vue'
import {
  NBreadcrumb,
  NBreadcrumbItem,
  NButton,
  NCard,
  NSpace,
  NInput,
  NIcon,
  NTooltip
} from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useRouter } from 'vue-router'
import { BulbOutlined } from '@vicons/antd'
import MonacoEditor from '@/components/monaco-editor'
import type { Router } from 'vue-router'

const DataPipesEdit = defineComponent({
  setup() {
    const { t } = useI18n()
    const router: Router = useRouter()

    const handleClickDataPipes = () => {
      router.push({ path: '/data-pipes/list' })
    }

    return { t, handleClickDataPipes }
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
                  <NBreadcrumbItem>{this.t('data_pipes.edit')}</NBreadcrumbItem>
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
          <MonacoEditor />
        </NCard>
      </NSpace>
    )
  }
})

export default DataPipesEdit
