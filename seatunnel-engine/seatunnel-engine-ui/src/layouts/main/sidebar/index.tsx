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

import { defineComponent, ref, type PropType, onMounted, h, type Component } from 'vue'
import { NIcon, NLayoutSider, NMenu } from 'naive-ui'
import { useRoute, RouterLink } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { DesktopOutline, ListOutline, PeopleOutline, PersonOutline } from '@vicons/ionicons5'

const Sidebar = defineComponent({
  name: 'Sidebar',
  props: {
    sideKey: {
      type: String as PropType<string>,
      default: ''
    }
  },
  setup() {
    const collapsedRef = ref(false)
    const defaultExpandedKeys = ['']
    const route = useRoute()
    const { t } = useI18n()

    const showDrop = ref(false)

    function renderIcon(icon: Component) {
      return () => h(NIcon, null, { default: () => h(icon) })
    }

    const sideMenuOptions = ref([
      {
        label: () =>
          h(
            RouterLink,
            {
              to: {
                path: '/overview'
              },
              exact: false
            },
            { default: () => t('menu.overview') }
          ),
        key: 'overview',
        icon: renderIcon(DesktopOutline)
      },
      {
        label: () =>
          h(
            RouterLink,
            {
              to: {
                path: '/jobs'
              },
              exact: false
            },
            { default: () => t('menu.jobs') }
          ),
        key: 'jobs',
        icon: renderIcon(ListOutline)
      },
      {
        label: () =>
          h(
            RouterLink,
            {
              to: {
                path: '/managers/workers'
              },
              exact: false
            },
            { default: () => t('menu.managers.workers') }
          ),
        key: 'workers',
        icon: renderIcon(PeopleOutline)
      },
      {
        label: () =>
          h(
            RouterLink,
            {
              to: {
                path: '/managers/master'
              },
              exact: false
            },
            { default: () => t('menu.managers.master') }
          ),
        key: 'master',
        icon: renderIcon(PersonOutline)
      }
    ])

    onMounted(() => {})

    return {
      collapsedRef,
      defaultExpandedKeys,
      showDrop,
      sideMenuOptions,
      route
    }
  },
  render() {
    return (
      <NLayoutSider
        bordered
        nativeScrollbar={false}
        show-trigger="bar"
        collapse-mode="width"
        collapsed={this.collapsedRef}
        onCollapse={() => (this.collapsedRef = true)}
        onExpand={() => (this.collapsedRef = false)}
        width={196}
      >
        <NMenu
          class="tab-vertical"
          value={this.$props.sideKey}
          options={this.sideMenuOptions}
          defaultExpandedKeys={this.defaultExpandedKeys}
        />
      </NLayoutSider>
    )
  }
})

export default Sidebar
