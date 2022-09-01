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

import { useI18n } from 'vue-i18n'
import { h, reactive, ref } from 'vue'
import { NButton, NSpace, NTag, NIcon } from 'naive-ui'
import { UploadOutlined, DownloadOutlined } from '@vicons/antd'

export function useTable() {
  const { t } = useI18n()

  const state = reactive({
    columns: [],
    tableData: [{ state: 'success' }, { state: 'fail' }, { state: 'running' }],
    page: ref(1),
    pageSize: ref(10),
    totalPage: ref(1),
    loading: ref(false)
  })

  const createColumns = (state: any) => {
    state.columns = [
      {
        title: t('tasks.task_name'),
        key: 'task_name'
      },
      {
        title: t('tasks.state'),
        key: 'state',
        render: (row: any) => {
          if (row.state === 'success') {
            return h(NTag, { type: 'success' }, t('tasks.success'))
          } else if (row.state === 'fail') {
            return h(NTag, { type: 'error' }, t('tasks.fail'))
          } else if (row.state === 'running') {
            return h(NTag, { type: 'info' }, t('tasks.running'))
          }
        }
      },
      {
        title: t('tasks.run_frequency'),
        key: 'run_frequency'
      },
      {
        title: t('tasks.next_run'),
        key: 'next_run'
      },
      {
        title: t('tasks.last_run'),
        key: 'last_run'
      },
      {
        title: t('tasks.last_total_bytes'),
        key: 'last_total_bytes',
        render: (row: any) =>
          h(NSpace, {}, [
            h(
              NTag,
              { type: 'success' },
              { icon: h(NIcon, {}, h(UploadOutlined)), default: 12 + ' (KB)' }
            ),
            h(
              NTag,
              { type: 'error' },
              { icon: h(NIcon, {}, h(DownloadOutlined)), default: 16 + ' (KB)' }
            )
          ])
      },
      {
        title: t('tasks.last_total_records'),
        key: 'last_total_records',
        render: (row: any) =>
          h(NSpace, {}, [
            h(
              NTag,
              { type: 'success' },
              { icon: h(NIcon, {}, h(UploadOutlined)), default: 66 }
            ),
            h(
              NTag,
              { type: 'error' },
              { icon: h(NIcon, {}, h(DownloadOutlined)), default: 77 }
            )
          ])
      },
      {
        title: t('tasks.operation'),
        key: 'operation',
        render: (row: any) =>
          h(NSpace, null, {
            default: () => [
              h(NButton, { text: true }, t('tasks.rerun')),
              h(NButton, { text: true }, t('tasks.kill')),
              h(NButton, { text: true }, t('tasks.view_log'))
            ]
          })
      }
    ]
  }

  return { state, createColumns }
}
