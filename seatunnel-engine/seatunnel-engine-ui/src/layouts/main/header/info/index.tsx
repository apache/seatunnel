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

import { defineComponent, reactive } from 'vue'
import { NSpace } from 'naive-ui'
import { overviewService } from '@/service/overview'
import type { Overview } from '@/service/overview/types'

const Logo = defineComponent({
  setup() {
    const data = reactive({} as Overview)
    overviewService.getOverview().then((res) => Object.assign(data, res))
    return { data }
  },
  render() {
    return (
      <NSpace justify="center" align="center" wrap={false} class="h-16 mr-6">
        <h2 class="text-base font-bold">Version:</h2>
        <span class="text-base text-nowrap">{this.data.projectVersion}</span>
        <h2 class="text-base font-bold ml-4">Commit:</h2>
        <span class="text-base text-nowrap">{this.data.gitCommitAbbrev}</span>
      </NSpace>
    )
  }
})

export default Logo
