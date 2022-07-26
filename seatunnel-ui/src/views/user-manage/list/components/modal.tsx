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

import { defineComponent, getCurrentInstance } from 'vue'
import { NForm, NFormItem, NInput, NRadioGroup, NRadio } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import Modal from '@/components/modal'
import type { PropType } from 'vue'

const props = {
  showModalRef: {
    type: Boolean as PropType<boolean>,
    default: false
  },
  statusRef: {
    type: Number as PropType<number>,
    default: 0
  },
  row: {
    type: Object as PropType<any>,
    default: {}
  }
}

const UserManageModal = defineComponent({
  props,
  emits: ['cancelModal', 'confirmModal'],
  setup(props, ctx) {
    const { t } = useI18n()
    const trim = getCurrentInstance()?.appContext.config.globalProperties.trim

    const handleCancel = () => {
      if (props.statusRef === 0) {
      }
      ctx.emit('cancelModal', props.showModalRef)
    }

    const handleConfirm = () => {}

    return { t, trim, handleCancel, handleConfirm }
  },
  render() {
    return (
      <Modal
        title={
          this.statusRef === 0
            ? this.t('user_manage.create')
            : this.t('user_manage.edite')
        }
        show={this.showModalRef}
        onCancel={this.handleCancel}
        onConfirm={this.handleConfirm}
      >
        {{
          default: () => (
            <NForm ref='userManageForm'>
              <NFormItem label={this.t('user_manage.username')} path=''>
                <NInput
                  allowInput={this.trim}
                />
              </NFormItem>
              <NFormItem label={this.t('user_manage.password')} path=''>
                <NInput
                  allowInput={this.trim}
                />
              </NFormItem>
              <NFormItem label={this.t('user_manage.email')} path=''>
                <NInput
                  allowInput={this.trim}
                />
              </NFormItem>
              <NFormItem label={this.t('user_manage.state')} path=''>
                <NRadioGroup>
                  <NRadio value={0}>{this.t('user_manage.active')}</NRadio>
                  <NRadio value={1}>{this.t('user_manage.inactive')}</NRadio>
                </NRadioGroup>
              </NFormItem>
            </NForm>
          )
        }}
      </Modal>
    )
  }
})

export default UserManageModal