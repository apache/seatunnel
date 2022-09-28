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

import { reactive, ref, SetupContext } from 'vue'
import { useI18n } from 'vue-i18n'
import { userAdd, userUpdate } from '@/service/user'

export function useFormModal(
  props: any,
  ctx: SetupContext<('cancelModal' | 'confirmModal')[]>
) {
  const { t } = useI18n()
  const state = reactive({
    userManageForm: ref(),
    model: {
      id: ref(),
      username: ref(''),
      password: ref(''),
      status: ref(0)
    },
    rules: {
      username: {
        required: true,
        trigger: ['input', 'blur'],
        message: t('user_manage.model_validate_tips')
      },
      password: {
        required: true,
        trigger: ['input', 'blur'],
        message: t('user_manage.model_validate_tips')
      }
    }
  })

  const handleValidate = (status: number) => {
    state.userManageForm.validate((errors: any) => {
      if (errors) return

      status === 0 ? handleAdd() : handleUpdate()
    })
  }

  const clearForm = () => {
    state.model.id = ''
    state.model.username = ''
    state.model.password = ''
    state.model.status = 0
  }

  const handleAdd = () => {
    userAdd({
      username: state.model.username,
      password: state.model.password,
      status: state.model.status,
      type: 0
    }).then(() => {
      ctx.emit('confirmModal', props.showModal)
    })
  }

  const handleUpdate = () => {
    userUpdate(state.model.id, {
      username: state.model.username,
      password: state.model.password,
      status: state.model.status,
      type: 0
    }).then(() => {
      ctx.emit('confirmModal', props.showModal)
    })
  }

  return { state, handleValidate, clearForm }
}
