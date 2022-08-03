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

import { reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'

export function useDetailOverview() {
  const { t } = useI18n()
  const state = reactive({
    loading: ref(false),
    columns: [],
    tableData: [{ name: '' }]
  })

  const createColumns = (state: any) => {
    state.columns = [
      {
        title: t('data_pipes.name'),
        key: 'name'
      },
      {
        title: t('data_pipes.execute_time'),
        key: 'executeTime'
      },
      {
        title: t('data_pipes.end_time'),
        key: 'endTime'
      },
      {
        title: t('data_pipes.state'),
        key: 'state'
      }
    ]
  }

  return { state, createColumns }
}
