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

import { h, reactive, ref, SetupContext } from 'vue'
import { useI18n } from 'vue-i18n'
import { NButton, NSpace } from 'naive-ui'

export function useConfigurationModal() {
  const { t } = useI18n()
  const state = reactive({
    parameterForm: ref(),
    model: {
      key: ref(''),
      value: ref('')
    },
    rules: {
      key: {
        required: true,
        trigger: ['input', 'blur'],
        message: t('data_pipes.model_validate_tips')
      },
      value: {
        required: true,
        trigger: ['input', 'blur'],
        message: t('data_pipes.model_validate_tips')
      }
    },
    loading: ref(false),
    columns: [],
    tableData: [{ key: 'key1', value: 'value1' }]
  })

  const createColumns = (state: any) => {
    state.columns = [
      {
        title: t('data_pipes.key'),
        key: 'key'
      },
      {
        title: t('data_pipes.value'),
        key: 'value'
      },
      {
        title: t('data_pipes.operation'),
        key: 'operation',
        render: (row: any) =>
          h(
            NButton,
            { text: true, onClick: () => handleDelete(row) },
            t('user_manage.delete')
          )
      }
    ]
  }

  const handleValidate = () => {
    state.parameterForm.validate((errors: any) => {
      if (!errors) {
        state.tableData.push({ key: state.model.key, value: state.model.value })
        state.model.key = ''
        state.model.value = ''
      } else {
        return
      }
    })
  }

  const handleDelete = (row: any) => {}

  return { state, createColumns, handleValidate }
}
