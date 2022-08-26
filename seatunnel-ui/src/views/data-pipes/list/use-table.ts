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

import { reactive, ref, h } from 'vue'
import { useI18n } from 'vue-i18n'
import { NSpace, NButton, NIcon, NDropdown } from 'naive-ui'
import { EllipsisOutlined } from '@vicons/antd'

export function useTable() {
  const { t } = useI18n()
  const state = reactive({
    columns: [],
    tableData: [{ username: '' }],
    page: ref(1),
    pageSize: ref(10),
    totalPage: ref(1),
    row: {},
    loading: ref(false),
    showDeleteModal: ref(false),
    showPublishModal: ref(false)
  })

  const createColumns = (state: any) => {
    state.columns = [
      {
        title: t('data_pipes.name'),
        key: 'name'
      },
      {
        title: t('data_pipes.state'),
        key: 'state'
      },
      {
        title: t('data_pipes.executed_time'),
        key: 'executedTime'
      },
      {
        title: t('data_pipes.modification_time'),
        key: 'modificationTime'
      },
      {
        title: t('data_pipes.operation'),
        key: 'operation',
        render: (row: any) =>
          h(NSpace, null, {
            default: () => [
              h(NButton, { text: true }, t('data_pipes.execute')),
              h(NButton, { text: true }, t('data_pipes.edit')),
              h(
                NButton,
                { text: true, onClick: () => handlePublish(row) },
                t('data_pipes.publish')
              ),
              h(
                NButton,
                {
                  text: true,
                  trigger: 'click'
                },
                h(
                  NDropdown,
                  {
                    options: [{ key: 'delete', label: t('data_pipes.delete') }],
                    onClick: () => handleDelete(row)
                  },
                  h(NIcon, {}, h(EllipsisOutlined))
                )
              )
            ]
          })
      }
    ]
  }

  const handleDelete = (row: any) => {
    state.showDeleteModal = true
    state.row = row
  }

  const handlePublish = (row: any) => {
    state.showPublishModal = true
    state.row = row
  }

  return { state, createColumns }
}
