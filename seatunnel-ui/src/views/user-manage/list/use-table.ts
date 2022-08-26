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
import { NSpace, NButton } from 'naive-ui'

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
    showFormModal: ref(false),
    showDeleteModal: ref(false),
    status: ref(0)
  })

  const createColumns = (state: any) => {
    state.columns = [
      {
        title: t('user_manage.username'),
        key: 'username'
      },
      {
        title: t('user_manage.state'),
        key: 'state'
      },
      {
        title: t('user_manage.email'),
        key: 'email'
      },
      {
        title: t('user_manage.creation_time'),
        key: 'creationTime'
      },
      {
        title: t('user_manage.last_landing_time'),
        key: 'lastLandingTime'
      },
      {
        title: t('user_manage.operation'),
        key: 'operation',
        render: (row: any) =>
          h(NSpace, null, {
            default: () => [
              h(NButton, { text: true }, t('user_manage.enable')),
              h(
                NButton,
                { text: true, onClick: () => handleEdit(row) },
                t('user_manage.edit')
              ),
              h(
                NButton,
                { text: true, onClick: () => handleDelete(row) },
                t('user_manage.delete')
              )
            ]
          })
      }
    ]
  }

  const handleEdit = (row: any) => {
    state.showFormModal = true
    state.status = 1
    state.row = row
  }

  const handleDelete = (row: any) => {
    state.showDeleteModal = true
    state.row = row
  }

  return { state, createColumns }
}
