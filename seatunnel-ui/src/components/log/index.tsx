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

import { defineComponent } from 'vue'
import { NButton, NInput, NInputGroup, NLog, NSpace } from 'naive-ui'
import { useI18n } from 'vue-i18n'

const Log = defineComponent({
  setup() {
    const { t } = useI18n()

    return { t }
  },
  render() {
    return (
      <NSpace vertical>
        <NInputGroup>
          <NInput placeholder={this.t('log.please_select_log')} />
          <NButton ghost>{this.t('log.search')}</NButton>
        </NInputGroup>
        <NLog rows={30} log={'test'} class={['py-5', 'px-3', 'bg-gray-50']} />
      </NSpace>
    )
  }
})

export default Log
