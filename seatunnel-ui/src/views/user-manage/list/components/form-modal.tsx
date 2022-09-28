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

import { defineComponent, getCurrentInstance, toRefs, watch } from 'vue'
import {
  NForm,
  NFormItem,
  NInput,
  NRadioGroup,
  NRadio,
  NIcon,
  NSpace,
  NTooltip
} from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { BulbOutlined } from '@vicons/antd'
import { useFormModal } from './use-form-modal'
import Modal from '@/components/modal'
import type { PropType } from 'vue'

const props = {
  showModal: {
    type: Boolean as PropType<boolean>,
    default: false
  },
  status: {
    type: Number as PropType<number>,
    default: 0
  },
  row: {
    type: Object as PropType<any>,
    default: {}
  }
}

const FormModal = defineComponent({
  props,
  emits: ['cancelModal', 'confirmModal'],
  setup(props, ctx) {
    const { t } = useI18n()
    const { state, handleValidate, clearForm } = useFormModal(props, ctx)
    const trim = getCurrentInstance()?.appContext.config.globalProperties.trim

    const handleCancel = () => {
      ctx.emit('cancelModal', props.showModal)
    }

    const handleConfirm = () => {
      handleValidate(props.status)
    }

    watch(
      () => props.showModal,
      () => {
        clearForm()
        if (props.status === 1) {
          state.model.id = props.row.id
          state.model.username = props.row.name
          state.model.status = props.row.status
        }
        state.rules.password.required = props.row.id === undefined
      }
    )

    return { t, ...toRefs(state), trim, handleCancel, handleConfirm }
  },
  render() {
    return (
      <Modal
        title={
          this.status === 0
            ? this.t('user_manage.create')
            : this.t('user_manage.edit')
        }
        show={this.showModal}
        onCancel={this.handleCancel}
        onConfirm={this.handleConfirm}
        confirmDisabled={
          !this.model.username || (this.status === 0 && !this.model.password)
        }
      >
        {{
          default: () => (
            <NForm model={this.model} rules={this.rules} ref='userManageForm'>
              <NFormItem label={this.t('user_manage.username')} path='username'>
                <NSpace align='center'>
                  <NInput
                    clearable
                    maxlength='50'
                    show-count
                    allowInput={this.trim}
                    style={{ width: '510px' }}
                    v-model={[this.model.username, 'value']}
                  />
                  <NTooltip placement='right' trigger='hover'>
                    {{
                      default: () => (
                        <span>{this.t('user_manage.username_tips')}</span>
                      ),
                      trigger: () => (
                        <NIcon size='20' style={{ cursor: 'pointer' }}>
                          <BulbOutlined />
                        </NIcon>
                      )
                    }}
                  </NTooltip>
                </NSpace>
              </NFormItem>
              <NFormItem label={this.t('user_manage.password')} path='password'>
                <NSpace align='center'>
                  <NInput
                    clearable
                    type='password'
                    maxlength='6'
                    show-count
                    allowInput={this.trim}
                    style={{ width: '510px' }}
                    v-model={[this.model.password, 'value']}
                  />
                  <NTooltip placement='right' trigger='hover'>
                    {{
                      default: () => (
                        <span>{this.t('user_manage.password_tips')}</span>
                      ),
                      trigger: () => (
                        <NIcon size='20' style={{ cursor: 'pointer' }}>
                          <BulbOutlined />
                        </NIcon>
                      )
                    }}
                  </NTooltip>
                </NSpace>
              </NFormItem>
              <NFormItem label={this.t('user_manage.status')} path='status'>
                <NRadioGroup v-model={[this.model.status, 'value']}>
                  <NRadio value={0}>{this.t('user_manage.enable')}</NRadio>
                  <NRadio value={1}>{this.t('user_manage.disable')}</NRadio>
                </NRadioGroup>
              </NFormItem>
            </NForm>
          )
        }}
      </Modal>
    )
  }
})

export default FormModal
