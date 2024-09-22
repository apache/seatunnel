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

import { defineComponent, ref, type PropType, onMounted, h } from 'vue'
import { NLayoutSider, NMenu } from 'naive-ui'
import { useRoute, RouterLink } from 'vue-router'
import { useI18n } from 'vue-i18n'

const Sidebar = defineComponent({
  name: 'Sidebar',
  props: {
    sideMenuOptions: {
      type: Array as PropType<any>,
      default: []
    },
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
        key: 'overview'
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
        key: 'jobs'
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
        key: 'workers'
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
        key: 'master'
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
