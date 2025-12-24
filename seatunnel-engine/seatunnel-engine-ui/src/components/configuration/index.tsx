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

import { NCard, NDescriptions, NDescriptionsItem, NSpace } from 'naive-ui'
import { defineComponent, type PropType } from 'vue'

export default defineComponent({
  props: {
    data: {
      type: Object as PropType<Record<string, any>>,
      default: () => ({})
    }
  },
  setup(props) {
    const format = (value: any) => {
      value = JSON.stringify(value)
      if (value) {
        value = value.replace(/^"(.*)"$/, '$1')
      }
      return value || ''
    }
    return () => (
      <NDescriptions label-placement="left" bordered column={1}>
        {props.data &&
          Object.entries(props.data).map(([key, value]) => (
            <NDescriptionsItem label={key}>{format(value)}</NDescriptionsItem>
          ))}
      </NDescriptions>
    )
  }
})
