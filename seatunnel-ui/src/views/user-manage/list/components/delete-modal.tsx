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
import { useI18n } from 'vue-i18n'
import Modal from '@/components/modal'
import type { PropType } from 'vue'

const props = {
  showModal: {
    type: Boolean as PropType<boolean>,
    default: false
  },
  row: {
    type: Object as PropType<any>,
    default: {}
  }
}

const DeleteModal = defineComponent({
  props,
  emits: ['cancelModal', 'confirmModal'],
  setup(props, ctx) {
    const { t } = useI18n()

    const handleCancel = () => {
      ctx.emit('cancelModal', props.showModal)
    }

    const handleConfirm = () => {
      ctx.emit('confirmModal')
    }

    return { t, handleCancel, handleConfirm }
  },
  render() {
    return (
      <Modal
        title={this.t('user_manage.delete')}
        show={this.showModal}
        onCancel={this.handleCancel}
        onConfirm={this.handleConfirm}
      >
        {{
          default: () => <span>{this.t('user_manage.user_delete_tips')}</span>
        }}
      </Modal>
    )
  }
})

export default DeleteModal
