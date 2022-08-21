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
import { reactive, h } from 'vue'
import { useRouter } from 'vue-router'
import type { Router } from 'vue-router'

export function useSettingDropdown() {
  const { t } = useI18n()
  const router: Router = useRouter()

  const dropdownOptions = [
    {
      key: 'header',
      type: 'render',
      render: () =>
        h('h3', { class: ['py-1.5', 'px-3', 'font-medium'] }, t('menu.manage'))
    },
    {
      key: 'header-divider',
      type: 'divider'
    },
    { key: 'user-manage', label: t('menu.user_manage') }
  ]

  const state = reactive({
    dropdownOptions
  })

  const handleSelect = (key: string) => {
    if (key === 'user-manage') {
      router.push({ path: '/user-manage' })
    }
  }

  return { state, handleSelect }
}
