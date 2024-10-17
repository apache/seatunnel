import { Graph, Path, Cell } from '@antv/x6'
import { Selection } from '@antv/x6-plugin-selection'
import { register } from '@antv/x6-vue-shape'
import { computed, defineComponent, onMounted, type PropType } from 'vue'
import './index.scss'
import type { Job, Metrics, Vertex } from '@/service/job/types'
import { NDataTable, NTag, type DataTableColumns } from 'naive-ui'

interface NodeStatus {
  id: string
  status: Job['jobStatus'] | 'default'
  label?: string
}

const AlgoNode = (props: any) => {
  const { node } = props
  const data = node?.getData() as NodeStatus
  const { label, status = 'default' } = data

  return (
    <div class={`node ${status}`}>
      <span class="label">{label}</span>
      <span class="status">{status}</span>
    </div>
  )
}

const nodeWidth = 300
register({
  shape: 'dag-node',
  width: nodeWidth,
  height: 36,
  component: AlgoNode,
  ports: {
    groups: {
      left: {
        position: 'left',
        attrs: {
          circle: {
            r: 4,
            magnet: true,
            stroke: '#C2C8D5',
            strokeWidth: 1,
            fill: '#fff'
          }
        }
      },
      right: {
        position: 'right',
        attrs: {
          circle: {
            r: 4,
            magnet: true,
            stroke: '#C2C8D5',
            strokeWidth: 1,
            fill: '#fff'
          }
        }
      }
    }
  }
})

Graph.registerEdge(
  'dag-edge',
  {
    inherit: 'edge',
    attrs: {
      line: {
        stroke: '#C2C8D5',
        strokeWidth: 1,
        targetMarker: null
      }
    }
  },
  true
)

Graph.registerConnector(
  'algo-connector',
  (s, e) => {
    const offset = 4
    const delta = Math.abs(e.x - s.x)
    const control = Math.floor((delta / 3) * 2)

    const v1 = { y: s.y, x: s.x + offset + control }
    const v2 = { y: e.y, x: e.x - offset - control }

    return Path.normalize(
      `M ${s.x} ${s.y}
       L ${s.x + offset} ${s.y}
       C ${v1.x} ${v1.y} ${v2.x} ${v2.y} ${e.x - offset} ${e.y}
       L ${e.x} ${e.y}
      `
    )
  },
  true
)

export default defineComponent({
  props: {
    job: {
      type: Object as PropType<Job>,
      required: true
    }
  },
  setup(props) {
    onMounted(() => {
      const graph: Graph = new Graph({
        container: document.getElementById('container')!,
        panning: {
          enabled: true,
          eventTypes: ['leftMouseDown', 'mouseWheel']
        },
        mousewheel: {
          enabled: true,
          modifiers: 'ctrl',
          factor: 1.1,
          maxScale: 1.5,
          minScale: 0.5
        },
        highlighting: {
          magnetAdsorbed: {
            name: 'stroke',
            args: {
              attrs: {
                fill: '#fff',
                stroke: '#31d0c6',
                strokeWidth: 4
              }
            }
          }
        },
        connecting: {
          snap: true,
          allowBlank: false,
          allowLoop: false,
          highlight: true,
          connector: 'algo-connector',
          connectionPoint: 'anchor',
          anchor: 'center',
          validateMagnet({ magnet }) {
            return magnet.getAttribute('port-group') !== 'left'
          },
          createEdge() {
            return graph.createEdge({
              shape: 'dag-edge',
              attrs: {
                line: {
                  strokeDasharray: '5 5'
                }
              },
              zIndex: -1
            })
          }
        }
      })
      graph.use(
        new Selection({
          multiple: true,
          rubberEdge: true,
          rubberNode: true,
          modifiers: 'shift',
          rubberband: true
        })
      )

      graph.on('edge:connected', ({ edge }) => {
        edge.attr({
          line: {
            strokeDasharray: ''
          }
        })
      })

      graph.on('node:change:data', ({ node }) => {
        const edges = graph.getIncomingEdges(node)
        const { status } = node.getData() as NodeStatus
        edges?.forEach((edge) => {
          if (status === 'RUNNING') {
            edge.attr('line/strokeDasharray', 5)
            edge.attr('line/style/animation', 'running-line 30s infinite linear')
          } else {
            edge.attr('line/strokeDasharray', '')
            edge.attr('line/style/animation', '')
          }
        })
      })

      type Port = { id: string; group: string }
      type Point = Vertex & { next: Vertex[]; row?: Point[]; ports?: Port[] }
      const isConnected = (source: Point, target: Point) => {
        const edgeMap = props?.job.jobDag.pipelineEdges || {}
        for (const key of Object.keys(edgeMap)) {
          for (const { inputVertexId, targetVertexId } of edgeMap[key]) {
            if (
              inputVertexId === String(source.vertexId) &&
              targetVertexId === String(target.vertexId)
            ) {
              return true
            }
          }
        }
        return false
      }
      const init = () => {
        const matrix = [] as Point[][]

        const findPrevious = (item: Point) => {
          for (const row of matrix) {
            for (const point of row) {
              if (isConnected(point, item)) {
                return point
              }
            }
          }
        }
        const points =
          props?.job?.jobDag?.vertexInfoMap?.map((item) => ({ ...item, next: [] }) as Point) || []
        points.forEach((point) => {
          const prevous = findPrevious(point)
          if (!prevous) {
            point.row = [point]
            matrix.push(point.row)
            return
          }
          if (prevous.next.length) {
            prevous.next.push(point)
            point.row = [point]
            matrix.push(point.row)
          } else {
            point.row = prevous.row
            prevous.next = [point]
            prevous.row!.push(point)
          }
        })

        const items: Cell.Metadata[] = []

        const offsetY = 100
        const offsetX = nodeWidth + 100
        matrix.forEach((row, rowNumber) => {
          row.forEach((item, colNumber) => {
            const id = 'node-' + String(item.vertexId)
            const data: NodeStatus = {
              id: String(item.vertexId),
              label: item.vertexName,
              status: props?.job?.jobStatus || 'default'
            }
            const ports = [] as Port[]
            if (colNumber !== 0) {
              ports.push({
                id: `${id}-left`,
                group: 'left'
              })
            }
            if (colNumber !== row.length - 1) {
              ports.push({
                id: `${id}-right`,
                group: 'right'
              })
            }
            items.push({
              id,
              shape: 'dag-node',
              x: colNumber * offsetX,
              y: rowNumber * offsetY,
              data,
              ports
            })
          })
        })

        const edgeMap = props?.job.jobDag.pipelineEdges || {}
        let zIndex = 0
        for (const id of Object.keys(edgeMap)) {
          const edges = edgeMap[id]
          for (const edge of edges) {
            items.push({
              id: `edge-${id}`,
              shape: 'dag-edge',
              source: {
                cell: `node-${edge.inputVertexId}`,
                port: `node-${edge.inputVertexId}-right`
              },
              target: {
                cell: `node-${edge.targetVertexId}`,
                port: `node-${edge.targetVertexId}-left`
              },
              zIndex: zIndex++
            })
          }
        }
        const cells: Cell[] = []
        items.forEach((item) => {
          if (item.shape === 'dag-node') {
            cells.push(graph.createNode(item))
          } else {
            cells.push(graph.createEdge(item))
          }
        })
        graph.resetCells(cells)
      }

      // 显示节点状态
      const showNodeStatus = async (statusList: NodeStatus[][]) => {
        const status = statusList[Math.floor(Math.random() * statusList.length)]
        status?.forEach((item) => {
          const { id, status } = item
          const node = graph.getCellById(id)
          const data = node.getData() as NodeStatus
          node.setData({
            ...data,
            status
          })
        })
        if (!status) return
        setTimeout(() => {
          showNodeStatus(statusList)
        }, 5000)
      }

      setTimeout(() => {
        init()
        graph.centerContent()
      }, 2000)
    })

    const vertexs = computed(() => props?.job?.jobDag?.vertexInfoMap || [])
    const metrics = computed(() => props?.job?.metrics || {})

    const sourceCell = (
      row: Vertex,
      key:
        | 'TableSourceReceivedBytes'
        | 'TableSourceReceivedCount'
        | 'TableSourceReceivedQPS'
        | 'TableSourceReceivedBytesPerSeconds'
    ) =>
      row.type === 'source' &&
      row.tablePaths.reduce((s, path) => s + Number(metrics.value[key][path]), 0)
    const sinkCell = (
      row: Vertex,
      key:
        | 'TableSinkWriteBytes'
        | 'TableSinkWriteCount'
        | 'TableSinkWriteQPS'
        | 'TableSinkWriteBytesPerSeconds'
    ) =>
      row.type === 'sink' &&
      row.tablePaths.reduce((s, path) => s + Number(metrics.value[key][path]), 0)
    const columns: DataTableColumns<Vertex> = [
      {
        title: 'Name',
        key: 'vertexName'
      },
      {
        title: 'Received Bytes',
        key: 'key',
        render: (row) => sourceCell(row, 'TableSourceReceivedBytes')
      },
      {
        title: 'Write Bytes',
        key: 'key',
        render: (row) => sinkCell(row, 'TableSinkWriteBytes')
      },
      {
        title: 'Received Count',
        key: 'key',
        render: (row) => sourceCell(row, 'TableSourceReceivedCount')
      },
      {
        title: 'Write Count',
        key: 'key',
        render: (row) => sinkCell(row, 'TableSinkWriteCount')
      },
      {
        title: 'Received QPS',
        key: 'key',
        render: (row) => sourceCell(row, 'TableSourceReceivedQPS')
      },
      {
        title: 'Write QPS',
        key: 'key',
        render: (row) => sinkCell(row, 'TableSinkWriteQPS')
      },
      {
        title: 'Received Bytes PerSecond',
        key: 'key',
        render: (row) => sourceCell(row, 'TableSourceReceivedBytesPerSeconds')
      },
      {
        title: 'Write Bytes PerSecond',
        key: 'key',
        render: (row) => sinkCell(row, 'TableSinkWriteBytesPerSeconds')
      }
    ]

    return () => (
      <div>
        <div id="container" style="height: 500px"></div>
        <NDataTable
          columns={columns}
          data={vertexs.value}
          pagination={false}
          scrollX="auto"
          bordered
          striped
        />
      </div>
    )
  }
})
