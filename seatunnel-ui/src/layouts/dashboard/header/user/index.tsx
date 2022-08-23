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

import { defineComponent, toRefs } from 'vue'
import { NSpace, NDropdown } from 'naive-ui'
import { useUserDropdown } from './use-user-dropdown'

const User = defineComponent({
  setup() {
    const { state, handleSelect } = useUserDropdown()

    return { ...toRefs(state), handleSelect }
  },
  render() {
    return (
      <NSpace
        justify='center'
        align='center'
        class='h-16 w-12 mr-2'
        style={{ cursor: 'pointer' }}
      >
        <NDropdown
          trigger='click'
          options={this.dropdownOptions}
          onSelect={this.handleSelect}
        >
          <img
            class='h-10 w-10 rounded-full'
            src='https://avatars.githubusercontent.com/u/19239641?s=64&v=4'
            alt=''
          />
        </NDropdown>
      </NSpace>
    )
  }
})

export default User
