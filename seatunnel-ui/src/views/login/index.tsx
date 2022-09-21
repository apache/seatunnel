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

import { defineComponent, toRefs, withKeys, getCurrentInstance } from 'vue'
import { NSpace, NForm, NFormItem, NInput, NButton, useMessage } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useForm } from './use-form'

const Login = defineComponent({
  setup() {
    window.$message = useMessage()
    const { t } = useI18n()
    const { state, handleLogin } = useForm()
    const trim = getCurrentInstance()?.appContext.config.globalProperties.trim

    return {
      t,
      ...toRefs(state),
      trim,
      handleLogin
    }
  },
  render() {
    return (
      <NSpace
        justify='center'
        align='center'
        class='w-full h-screen bg-blue-400'
      >
        <div class='w-96 bg-white px-10 py-8'>
          <h2 class='text-2xl mb-6'>{this.t('login.login_to_sea_tunnel')}</h2>
          <NForm rules={this.rules} ref='loginFormRef'>
            <NFormItem
              label={this.t('login.username')}
              label-style={{ color: 'black' }}
              path='userName'
            >
              <NInput
                allowInput={this.trim}
                type='text'
                v-model={[this.loginForm.username, 'value']}
                placeholder={this.t('login.username_tips')}
                autofocus
                onKeydown={withKeys(this.handleLogin, ['enter'])}
              />
            </NFormItem>
            <NFormItem
              label={this.t('login.password')}
              label-style={{ color: 'black' }}
              path='userPassword'
            >
              <NInput
                allowInput={this.trim}
                type='password'
                v-model={[this.loginForm.password, 'value']}
                placeholder={this.t('login.password_tips')}
                onKeydown={withKeys(this.handleLogin, ['enter'])}
              />
            </NFormItem>
          </NForm>
          <NButton
            type='info'
            disabled={!this.loginForm.username || !this.loginForm.password}
            style={{ width: '100%' }}
            onClick={this.handleLogin}
          >
            {this.t('login.login')}
          </NButton>
        </div>
      </NSpace>
    )
  }
})

export default Login
