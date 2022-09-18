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
import { userLogin } from '@/service/user'
import { useUserStore } from '@/store/user'
import { useRouter } from 'vue-router'
import type { FormRules } from 'naive-ui'
import type { Router } from 'vue-router'

export function useForm() {
  const router: Router = useRouter()
  const { t } = useI18n()
  const userStore = useUserStore()

  const state = reactive({
    loginFormRef: ref(),
    loginForm: {
      username: '',
      password: ''
    },
    rules: {
      username: {
        trigger: ['input', 'blur'],
        validator() {
          if (state.loginForm.username === '') {
            return new Error(t('login.username_tips'))
          }
        }
      },
      password: {
        trigger: ['input', 'blur'],
        validator() {
          if (state.loginForm.password === '') {
            return new Error(t('login.password_tips'))
          }
        }
      }
    } as FormRules
  })

  const handleLogin = () => {
    userLogin({ ...state.loginForm }).then((res: any) => {
      userStore.setUserInfo(res.data)
      router.push({ path: '/data-pipes' })
    })
  }

  return {
    state,
    handleLogin
  }
}
