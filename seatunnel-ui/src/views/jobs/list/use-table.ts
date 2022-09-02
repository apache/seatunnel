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
import { NButton, NSpace, NSwitch } from 'naive-ui'

export function useTable() {
  const { t } = useI18n()

  const state = reactive({
    columns: [],
    tableData: [{}],
    page: ref(1),
    pageSize: ref(10),
    totalPage: ref(1),
    loading: ref(false)
  })

  const createColumns = (state: any) => {
    state.columns = [
      {
        title: t('jobs.data_pipe_name'),
        key: 'data_pipe_name'
      },
      {
        title: t('jobs.plan'),
        key: 'plan'
      },
      {
        title: t('jobs.create_date'),
        key: 'create_date'
      },
      {
        title: t('jobs.publish'),
        key: 'publish',
        render: (row: any) => h(NSwitch, { round: false })
      },
      {
        title: t('jobs.operation'),
        key: 'operation',
        render: (row: any) =>
          h(NSpace, null, {
            default: () => [
              h(NButton, { text: true }, t('jobs.executed_immediately')),
              h(NButton, { text: true }, t('jobs.stop_plan'))
            ]
          })
      }
    ]
  }

  return { state, createColumns }
}
