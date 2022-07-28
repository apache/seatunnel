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

import { defineComponent, toRefs, onMounted } from 'vue'
import { NSpace, NCard, NButton, NDataTable, NPagination } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import { useTable } from './use-table'
import FormModal from './components/form-modal'
import DeleteModal from './components/delete-modal'

const UserManageList = defineComponent({
  setup() {
    const { t } = useI18n()
    const { state, createColumns } = useTable()

    const handleFormModal = () => {
      state.showFormModal = true
      state.status = 0
    }

    const handleCancelFormModal = () => {
      state.showFormModal = false
    }

    const handleConfirmFormModal = () => {
      state.showFormModal = false
    }

    const handleCancelDeleteModal = () => {
      state.showDeleteModal = false
    }

    const handleConfirmDeleteModal = () => {
      state.showDeleteModal = false
    }

    onMounted(() => {
      createColumns(state)
    })

    return {
      t,
      ...toRefs(state),
      handleFormModal,
      handleCancelFormModal,
      handleConfirmFormModal,
      handleCancelDeleteModal,
      handleConfirmDeleteModal
    }
  },
  render() {
    return (
      <NSpace vertical>
        <NCard title={this.t('user_manage.user_manage')}>
          {{
            'header-extra': () => (
              <NButton onClick={this.handleFormModal}>
                {this.t('user_manage.create')}
              </NButton>
            )
          }}
        </NCard>
        <NCard>
          <NSpace vertical>
            <NDataTable
              loading={this.loading}
              columns={this.columns}
              data={this.tableData}
            />
            <NSpace justify='center'>
              <NPagination
                v-model:page={this.page}
                v-model:page-size={this.pageSize}
                page-count={this.totalPage}
                show-size-picker
                page-sizes={[10, 30, 50]}
                show-quick-jumper
              />
            </NSpace>
          </NSpace>
        </NCard>
        <FormModal
          showModal={this.showFormModal}
          status={this.status}
          row={this.row}
          onCancelModal={this.handleCancelFormModal}
          onConfirmModal={this.handleConfirmFormModal}
        />
        <DeleteModal
          showModal={this.showDeleteModal}
          row={this.row}
          onCancelModal={this.handleCancelDeleteModal}
          onConfirmModal={this.handleConfirmDeleteModal}
        />
      </NSpace>
    )
  }
})

export default UserManageList
