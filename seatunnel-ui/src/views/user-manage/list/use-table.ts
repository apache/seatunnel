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
import { useAsyncState } from '@vueuse/core'
import { useI18n } from 'vue-i18n'
import { NSpace, NButton } from 'naive-ui'
import { userList, userDelete } from '@/service/user'
import type { ResponseTable } from '@/service/types'
import type { UserDetail } from '@/service/user/types'

export function useTable() {
  const { t } = useI18n()
  const state = reactive({
    columns: [],
    tableData: [{ username: '' }],
    pageNo: ref(1),
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
        key: 'name'
      },
      {
        title: t('user_manage.status'),
        key: 'status'
      },
      {
        title: t('user_manage.create_time'),
        key: 'createTime'
      },
      {
        title: t('user_manage.update_time'),
        key: 'updateTime'
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
    //if (state.tableData.length === 1 && state.pageNo > 1) {
    //  --state.pageNo
    //}
    //
    //userDelete(row.id).then(() => {
    //  getTableData({
    //    pageSize: state.pageSize,
    //    pageNo: state.pageNo
    //  })
    //})
    state.showDeleteModal = true
    state.row = row
  }

  const getTableData = (params: any) => {
    if (state.loading) return
    state.loading = true
    useAsyncState(
      userList({ ...params }).then((res: ResponseTable<Array<UserDetail> | []>) => {
        state.tableData = res.data.data
        state.totalPage = res.data.totalPage
        state.loading = false
      }),
      {}
    )
  }

  return { state, createColumns, getTableData }
}
