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
import { defineComponent, h, ref } from 'vue'
import { NDataTable, NDrawer, NDrawerContent } from 'naive-ui'
import { useI18n } from 'vue-i18n'
import type { DataTableColumns } from 'naive-ui'
import { NButton } from 'naive-ui'
import { NLayout, NLayoutContent } from 'naive-ui'
import { managerService } from '@/service/manager'
import type { Monitor, WorkerOverview } from '@/service/manager/types'
import { useRoute } from 'vue-router'
import Configuration from '@/components/configuration'

// A Workers/Master table row: the raw system-monitoring-information fields plus, when the
// resource manager has a matching live WorkerProfile, its slot/attribute projection. The slot
// fields are optional because a monitored node and a registered resource-manager worker are two
// independent sources joined client-side by host+port; one can be present without the other
// (e.g. right after a node joins, or for a non-worker node type in the future).
type WorkerRow = Monitor & Partial<Pick<WorkerOverview, 'totalSlot' | 'usedSlot' | 'attributes'>>

export default defineComponent({
  setup() {
    const { t } = useI18n()
    const route = useRoute()
    const monitors = ref([] as WorkerRow[])
    const drawerShow = ref(false)
    const selectedRow = ref({} as WorkerRow)

    const fetch = async () => {
      const isMaster = route?.path.endsWith('/master') || false
      const [monitorRes, workerOverviews] = await Promise.all([
        managerService.getMonitors(),
        // The resource manager only knows about workers, and may be briefly unreachable
        // right after startup; never let that block rendering the monitoring table itself.
        managerService.getWorkerOverview().catch(() => [] as WorkerOverview[])
      ])
      const overviewByAddress = new Map<string, WorkerOverview>()
      workerOverviews.forEach((worker) => {
        overviewByAddress.set(`${worker.host}:${worker.port}`, worker)
      })

      monitors.value = (monitorRes || [])
        .filter((row) => row.isMaster === String(isMaster))
        .map((row) => {
          const worker = overviewByAddress.get(`${row.host}:${row.port}`)
          return worker
            ? {
                ...row,
                totalSlot: worker.totalSlot,
                usedSlot: worker.usedSlot,
                attributes: worker.attributes
              }
            : row
        })
    }
    fetch()

    const viewDetail = (row: WorkerRow) => {
      selectedRow.value = row
      drawerShow.value = true
    }

    function createColumns(): DataTableColumns<WorkerRow> {
      return [
        {
          title: 'Host',
          key: 'host'
        },
        {
          title: 'Port',
          key: 'port'
        },
        {
          title: 'Role',
          key: 'isMaster',
          render: (row) => (row.isMaster === 'true' ? 'Master' : 'Worker')
        },
        {
          title: 'CPU Load',
          key: 'load.systemAverage'
        },
        {
          title: 'Heap MEM Used',
          key: 'heap.memory.used'
        },
        {
          title: 'Heap MEM Max',
          key: 'heap.memory.max'
        },
        {
          title: 'Physical MEM',
          key: 'physical.memory.total'
        },
        {
          title: 'GC (minor/major)',
          key: 'gc',
          render: (row) => `${row['minor.gc.count']}/${row['major.gc.count']}`
        },
        {
          title: 'Threads',
          key: 'thread.count'
        },
        {
          title: 'Slots (used/total)',
          key: 'slots',
          render: (row) => (row.totalSlot === undefined ? '-' : `${row.usedSlot}/${row.totalSlot}`)
        },
        {
          title: 'Action',
          key: 'actions',
          render: (row) => {
            return h(
              NButton,
              {
                strong: true,
                tertiary: true,
                size: 'small',
                onClick: () => viewDetail(row)
              },
              { default: () => 'View' }
            )
          }
        }
      ]
    }

    const columns = createColumns()
    return () => (
      <NLayout>
        <NLayoutContent>
          <div class="w-full bg-white p-6 border border-gray-100 rounded-xl">
            <h2 class="font-bold text-2xl pb-6">{t('managers.managers')}</h2>
            <NDataTable
              columns={columns}
              data={monitors.value}
              pagination={false}
              bordered={false}
            />
          </div>
          <NDrawer
            show={drawerShow.value}
            width={'40%'}
            closeOnEsc
            onUpdateShow={(show: boolean) => (drawerShow.value = show)}
          >
            <NDrawerContent title={`${selectedRow.value.host}:${selectedRow.value.port}`} closable>
              <Configuration data={selectedRow.value}></Configuration>
            </NDrawerContent>
          </NDrawer>
        </NLayoutContent>
      </NLayout>
    )
  }
})
