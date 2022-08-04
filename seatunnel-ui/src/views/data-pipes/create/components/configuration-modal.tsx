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

import { defineComponent, onMounted, toRefs } from 'vue'
import { useI18n } from 'vue-i18n'
import { NButton, NDataTable, NForm, NFormItem, NInput, NSpace } from 'naive-ui'
import { useConfigurationModal } from './use-configuration-modal'
import Modal from '@/components/modal'
import type { PropType } from 'vue'

const props = {
  showModal: {
    type: Boolean as PropType<boolean>,
    default: false
  },
  type: {
    type: String as PropType<'engine-parameter' | 'self-defined-parameter'>
  }
}

const ConfigurationModal = defineComponent({
  props,
  emits: ['cancelModal', 'confirmModal'],
  setup(props, ctx) {
    const { t } = useI18n()
    const { state, createColumns, handleValidate } = useConfigurationModal()

    const handleCancel = () => {
      ctx.emit('cancelModal', props.showModal)
    }

    const handleConfirm = () => {}

    const handleClickAdd = () => {
      handleValidate()
    }

    onMounted(() => {
      createColumns(state)
    })

    return { t, ...toRefs(state), handleCancel, handleConfirm, handleClickAdd }
  },
  render($props: any) {
    return (
      <Modal
        title={
          this.t(
            'data_pipes.' +
              ($props.type === 'engine-parameter'
                ? 'engine_parameter'
                : 'self_defined_parameter')
          ) +
          ' ' +
          this.t('data_pipes.configuration')
        }
        show={this.showModal}
        onCancel={this.handleCancel}
        onConfirm={this.handleConfirm}
      >
        <NSpace vertical>
          <NForm model={this.model} rules={this.rules} ref='parameterForm'>
            <NFormItem label={this.t('data_pipes.key')} path='key'>
              <NInput v-model={[this.model.key, 'value']} clearable />
            </NFormItem>
            <NFormItem label={this.t('data_pipes.value')} path='value'>
              <NInput v-model={[this.model.value, 'value']} clearable />
            </NFormItem>
            <NButton class='w-full' onClick={this.handleClickAdd}>
              {this.t('data_pipes.add')}
            </NButton>
          </NForm>
          <NDataTable
            loading={this.loading}
            columns={this.columns}
            data={this.tableData}
          />
        </NSpace>
      </Modal>
    )
  }
})

export default ConfigurationModal
