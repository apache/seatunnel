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

import { defineComponent, onMounted, toRefs } from 'vue'
import { NGi, NGrid, NSpace, NTabs, NTabPane, NDataTable } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useDetailOverview } from './use-detail-overview'
import Log from '@/components/log'

const DetailOverview = defineComponent({
  setup() {
    const { t } = useI18n()
    const { state, createColumns } = useDetailOverview()

    onMounted(() => {
      createColumns(state)
    })

    return { t, ...toRefs(state) }
  },
  render() {
    return (
      <NSpace vertical>
        <NGrid x-gap='12' cols='2'>
          <NGi>{this.t('data_pipes.input_metrics')}</NGi>
          <NGi>{this.t('data_pipes.output_metrics')}</NGi>
        </NGrid>
        <NGrid x-gap='12' cols='4'>
          <NGi
            class={['flex', 'justify-between', 'py-4', 'px-3', 'bg-gray-50']}
          >
            <span>1212</span>
            <span class='text-gray-400'>
              {this.t('data_pipes.bytes_per_second')}
            </span>
          </NGi>
          <NGi
            class={['flex', 'justify-between', 'py-4', 'px-3', 'bg-gray-50']}
          >
            <span>1212</span>
            <span class='text-gray-400'>
              {this.t('data_pipes.record_per_second')}
            </span>
          </NGi>
          <NGi
            class={['flex', 'justify-between', 'py-4', 'px-3', 'bg-gray-50']}
          >
            <span>1212</span>
            <span class='text-gray-400'>
              {this.t('data_pipes.bytes_per_second')}
            </span>
          </NGi>
          <NGi
            class={['flex', 'justify-between', 'py-4', 'px-3', 'bg-gray-50']}
          >
            <span>1212</span>
            <span class='text-gray-400'>
              {this.t('data_pipes.record_per_second')}
            </span>
          </NGi>
        </NGrid>
        <NGrid x-gap='12' cols='4'>
          <NGi
            class={['flex', 'justify-between', 'py-4', 'px-3', 'bg-gray-50']}
          >
            <span>1212</span>
            <span class='text-gray-400'>
              {this.t('data_pipes.total_bytes')}
            </span>
          </NGi>
          <NGi
            class={['flex', 'justify-between', 'py-4', 'px-3', 'bg-gray-50']}
          >
            <span>1212</span>
            <span class='text-gray-400'>
              {this.t('data_pipes.total_records')}
            </span>
          </NGi>
          <NGi
            class={['flex', 'justify-between', 'py-4', 'px-3', 'bg-gray-50']}
          >
            <span>1212</span>
            <span class='text-gray-400'>
              {this.t('data_pipes.total_bytes')}
            </span>
          </NGi>
          <NGi
            class={['flex', 'justify-between', 'py-4', 'px-3', 'bg-gray-50']}
          >
            <span>1212</span>
            <span class='text-gray-400'>
              {this.t('data_pipes.total_records')}
            </span>
          </NGi>
        </NGrid>
        <NTabs type='line' justify-content='space-evenly' class='mt-7'>
          <NTabPane name='run-log' tab={this.t('data_pipes.run_log')}>
            <Log />
          </NTabPane>
          <NTabPane
            name='historical-run-logs'
            tab={this.t('data_pipes.historical_run_logs')}
          >
            <NDataTable
              loading={this.loading}
              columns={this.columns}
              data={this.tableData}
            />
          </NTabPane>
        </NTabs>
      </NSpace>
    )
  }
})

export default DetailOverview
