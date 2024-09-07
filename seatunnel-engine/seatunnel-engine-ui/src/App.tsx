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

import { defineComponent, computed, watch, ref } from 'vue'
import {
  NConfigProvider,
  NMessageProvider,
  NDialogProvider,
  darkTheme,
  dateZhCN,
  dateEnUS,
  zhCN,
  enUS
} from 'naive-ui'
import { useThemeStore } from '@/store/theme'
import { useSettingStore } from '@/store/setting'
import { useI18n } from 'vue-i18n'
import themeList from '@/themes'
import type { GlobalThemeOverrides } from 'naive-ui'
import type { Ref } from 'vue'
import type { CustomThemeCommonVars, ThemeCommonVars } from 'naive-ui/es/config-provider/src/interface'

const App = defineComponent({
  setup() {
    const themeStore = useThemeStore()
    const settingStore = useSettingStore()
    const currentTheme = computed(() =>
      themeStore.getDarkTheme ? darkTheme : undefined
    )
    const themeOverrides = computed(() => themeList[currentTheme.value ? 'dark' : 'light'])
    const setBorderRadius = (v: number) => {
      (themeOverrides.value.common as Partial<ThemeCommonVars & CustomThemeCommonVars>).borderRadius =
        v + 'px'
    }

    settingStore.getFilletValue && setBorderRadius(settingStore.getFilletValue)

    if (settingStore.getLocales) {
      const { locale } = useI18n()
      locale.value = settingStore.getLocales
    }

    watch(
      () => settingStore.getFilletValue,
      () => {
        setBorderRadius(settingStore.getFilletValue)
      }
    )

    return {
      settingStore,
      currentTheme,
      themeOverrides,
      themeList
    }
  },
  render() {
    return (
      <NConfigProvider
        theme={this.currentTheme}
        theme-overrides={this.themeOverrides}
        date-locale={
          this.settingStore.getLocales === 'zh_CN' ? dateZhCN : dateEnUS
        }
        locale={this.settingStore.getLocales === 'zh_CN' ? zhCN : enUS}
      >
        <NMessageProvider>
          <NDialogProvider>
            <router-view />
          </NDialogProvider>
        </NMessageProvider>
      </NConfigProvider>
    )
  }
})

export default App
