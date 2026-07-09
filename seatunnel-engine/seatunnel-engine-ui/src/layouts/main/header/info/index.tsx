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

import { defineComponent, reactive, computed } from 'vue'
import { NButton, NDropdown, NSpace } from 'naive-ui'
import { overviewService } from '@/service/overview'
import type { Overview } from '@/service/overview/types'
import { useSettingStore } from '@/store/setting'
import type { Locales } from '@/store/setting/types'
import { useI18n } from 'vue-i18n'

const Logo = defineComponent({
  setup() {
    const data = reactive({} as Overview)
    overviewService.getOverview().then((res) => Object.assign(data, res))
    const settingStore = useSettingStore()
    const { t } = useI18n()

    const langLabel = computed(() =>
      settingStore.getLocales === 'zh_CN' ? '中文' : 'EN'
    )

    const langOptions = computed(() => [
      { label: 'English', key: 'en_US' },
      { label: '中文', key: 'zh_CN' }
    ])

    const onSelectLang = (key: string) => {
      if (key === 'zh_CN' || key === 'en_US') {
        settingStore.setLocales(key as Locales)
      }
    }
    return { data, t, langLabel, langOptions, onSelectLang }
  },
  render() {
    return (
      <NSpace justify="center" align="center" wrap={false} class="h-16 mr-6">
        <h2 class="text-base font-bold">{this.t('common.version')}:</h2>
        <span class="text-base text-nowrap">{this.data.projectVersion}</span>
        <h2 class="text-base font-bold ml-4">{this.t('common.commit')}:</h2>
        <span class="text-base text-nowrap">{this.data.gitCommitAbbrev}</span>
        <NDropdown
          trigger="click"
          options={this.langOptions}
          onSelect={this.onSelectLang}
        >
          <NButton
            quaternary
            size="small"
            class="ml-4 text-white"
            style="border: 1px solid rgba(255,255,255,0.6);"
          >
            {this.t('common.language')}: {this.langLabel}
          </NButton>
        </NDropdown>
      </NSpace>
    )
  }
})

export default Logo
