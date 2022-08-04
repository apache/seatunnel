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
  NSpace,
  NCard,
  NButton,
  NBreadcrumb,
  NBreadcrumbItem,
  NTabs,
  NTabPane
} from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useRouter } from 'vue-router'
import MonacoEditor from '@/components/monaco-editor'
import DetailOverview from './components/detail-overview'
import type { Router } from 'vue-router'

const DataPipesDetail = defineComponent({
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
                  <NBreadcrumbItem>user-order-tables-10</NBreadcrumbItem>
                </NBreadcrumb>
                <div
                  class={['w-3', 'h-3', 'rounded-full', 'bg-green-400']}
                ></div>
                <span
                  style={{
                    fontSize: 'var(--n-font-size)',
                    color: 'var(--n-item-text-color-active)'
                  }}
                >
                  {this.t('data_pipes.stop')}
                </span>
              </NSpace>
            ),
            'header-extra': () => (
              <NSpace>
                <NButton secondary>{this.t('data_pipes.execute')}</NButton>
                <NButton secondary>{this.t('data_pipes.kill')}</NButton>
                <NButton secondary>{this.t('data_pipes.stop')}</NButton>
              </NSpace>
            )
          }}
        </NCard>
        <NTabs type='segment' class='mt-9'>
          <NTabPane name='overview' tab={this.t('data_pipes.overview')}>
            <NCard>
              <DetailOverview />
            </NCard>
          </NTabPane>
          <NTabPane name='script' tab={this.t('data_pipes.script')}>
            <NCard>
              <MonacoEditor />
            </NCard>
          </NTabPane>
        </NTabs>
      </NSpace>
    )
  }
})

export default DataPipesDetail
