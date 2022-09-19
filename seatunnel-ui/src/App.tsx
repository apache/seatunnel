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

import { defineComponent, computed } from 'vue'
import {
  NConfigProvider,
  NMessageProvider,
  darkTheme,
  dateZhCN,
  dateEnUS,
  zhCN,
  enUS
} from 'naive-ui'
import { useThemeStore } from '@/store/theme'
import { useLocalesStore } from '@/store/locale'
import themeList from '@/themes'
import type { GlobalThemeOverrides } from 'naive-ui'

const App = defineComponent({
  setup() {
    const themeStore = useThemeStore()
    const currentTheme = computed(() =>
      themeStore.darkTheme ? darkTheme : undefined
    )
    const localesStore = useLocalesStore()

    return {
      currentTheme,
      localesStore
    }
  },
  render() {
    const themeOverrides: GlobalThemeOverrides =
      themeList[this.currentTheme ? 'dark' : 'light']

    return (
      <NConfigProvider
        theme={this.currentTheme}
        theme-overrides={themeOverrides}
        date-locale={
          String(this.localesStore.getLocales) === 'zh_CN' ? dateZhCN : dateEnUS
        }
        locale={String(this.localesStore.getLocales) === 'zh_CN' ? zhCN : enUS}
      >
        <NMessageProvider>
          <router-view />
        </NMessageProvider>
      </NConfigProvider>
    )
  }
})

export default App
