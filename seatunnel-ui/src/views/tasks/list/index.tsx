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
import {
  NButton,
  NCard,
  NDataTable,
  NInput,
  NPagination,
  NSpace,
  NSelect
} from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useTable } from './use-table'

const TasksList = defineComponent({
  setup() {
    const { t } = useI18n()
    const { state, createColumns } = useTable()

    const handleSearch = () => {}

    onMounted(() => {
      createColumns(state)
    })

    return { t, handleSearch, ...toRefs(state) }
  },
  render() {
    return (
      <NSpace vertical>
        <NCard title={this.t('tasks.tasks')}>
          {{
            'header-extra': () => (
              <NSpace>
                <NInput
                  placeholder={this.t('tasks.tasks_name')}
                  style={{ width: '200px' }}
                />
                <NSelect
                  placeholder={this.t('tasks.state')}
                  style={{ width: '200px' }}
                  options={[
                    { label: this.t('tasks.success'), value: 'success' },
                    { label: this.t('tasks.fail'), value: 'fail' },
                    { label: this.t('tasks.running'), value: 'running' }
                  ]}
                />
                <NButton onClick={this.handleSearch}>
                  {this.t('tasks.search')}
                </NButton>
              </NSpace>
            )
          }}
        </NCard>
        <NCard>
          <NSpace vertical>
            <NDataTable
              loading={this.loading}
              columns={this.columns}
              data={this.tableData}
            />
            <NSpace justify='center'>
              <NPagination
                v-model:page={this.page}
                v-model:page-size={this.pageSize}
                page-count={this.totalPage}
                show-size-picker
                page-sizes={[10, 30, 50]}
                show-quick-jumper
              />
            </NSpace>
          </NSpace>
        </NCard>
      </NSpace>
    )
  }
})

export default TasksList
