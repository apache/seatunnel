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

import { defineComponent, watch, ref } from 'vue'
import { useRoute } from 'vue-router'
import { NLayout, NLayoutHeader, NLayoutContent, NSpace } from 'naive-ui'
import Header from './header'
import Sidebar from './sidebar'

const Main = defineComponent({
  setup() {
    const route = useRoute()
    const routeKey = ref(route.fullPath)
    const showSide = ref(false)

    const menuKey = ref(route.meta.activeMenu as string)

    watch(
      () => route,
      () => {
        showSide.value = route?.meta?.showSide as boolean
        menuKey.value = route.meta.activeSide as string
        routeKey.value = route.fullPath
      },
      {
        immediate: true,
        deep: true
      }
    )
    return {
      showSide,
      menuKey,
      routeKey
    }
  },
  render() {
    return (
      <NLayout>
        <NLayoutHeader bordered>
          <Header />
        </NLayoutHeader>
        <NLayoutContent style={{ height: 'calc(100vh - 69px)' }}>
          <NLayout has-sider position="absolute">
            {this.showSide && <Sidebar sideKey={this.menuKey} />}
            <NLayoutContent native-scrollbar={false}>
              <NSpace
                vertical
                justify="space-between"
                style={'height: 100%;padding: 16px 22px'}
                size="small"
              >
                <router-view key={this.routeKey} class={!this.showSide && 'px-32 py-12'} />
              </NSpace>
            </NLayoutContent>
          </NLayout>
        </NLayoutContent>
      </NLayout>
    )
  }
})

export default Main
