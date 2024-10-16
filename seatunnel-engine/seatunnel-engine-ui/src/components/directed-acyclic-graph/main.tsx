import { Graph, Path, Cell } from '@antv/x6'
import { Selection } from '@antv/x6-plugin-selection'
import { register } from '@antv/x6-vue-shape'
import { computed, defineComponent, defineProps, type PropType } from 'vue'
import './main.scss'
import JsonData from './data.json'
import type { Job, Metrics, Vertex } from '@/service/job/types'
import { NDataTable, type DataTableColumns } from 'naive-ui'

interface NodeStatus {
  id: string
  status: 'default' | 'success' | 'failed' | 'running'
  label?: string
}

const image = {
  logo: 'https://gw.alipayobjects.com/mdn/rms_43231b/afts/img/A*evDjT5vjkX0AAAAAAAAAAAAAARQnAQ',
  success: 'https://gw.alipayobjects.com/mdn/rms_43231b/afts/img/A*6l60T6h8TTQAAAAAAAAAAAAAARQnAQ',
  failed: 'https://gw.alipayobjects.com/mdn/rms_43231b/afts/img/A*SEISQ6My-HoAAAAAAAAAAAAAARQnAQ',
  running: 'https://gw.alipayobjects.com/mdn/rms_43231b/afts/img/A*t8fURKfgSOgAAAAAAAAAAAAAARQnAQ'
}

const AlgoNode = (props: any) => {
  const { node } = props
  const data = node?.getData() as NodeStatus
  const { label, status = 'default' } = data

  return (
    <div class={`node ${status}`}>
      <img src={image.logo} alt="logo" />
      <span class="label">{label}</span>
      <span class="status">
        {status === 'success' && <img src={image.success} alt="success" />}
        {status === 'failed' && <img src={image.failed} alt="failed" />}
        {status === 'running' && <img src={image.running} alt="running" />}
      </span>
    </div>
  )
}

register({
  shape: 'dag-node',
  width: 180,
  height: 36,
  component: AlgoNode,
  ports: {
    groups: {
      top: {
        position: 'top',
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
      bottom: {
        position: 'bottom',
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
    const deltaY = Math.abs(e.y - s.y)
    const control = Math.floor((deltaY / 3) * 2)

    const v1 = { x: s.x, y: s.y + offset + control }
    const v2 = { x: e.x, y: e.y - offset - control }

    return Path.normalize(
      `M ${s.x} ${s.y}
       L ${s.x} ${s.y + offset}
       C ${v1.x} ${v1.y} ${v2.x} ${v2.y} ${e.x} ${e.y - offset}
       L ${e.x} ${e.y}
      `
    )
  },
  true
)

const nodeStatusList: NodeStatus[][] = [
  [
    {
      id: '1',
      status: 'running'
    },
    {
      id: '2',
      status: 'default'
    },
    {
      id: '3',
      status: 'default'
    },
    {
      id: '4',
      status: 'default'
    }
  ],
  [
    {
      id: '1',
      status: 'success'
    },
    {
      id: '2',
      status: 'running'
    },
    {
      id: '3',
      status: 'default'
    },
    {
      id: '4',
      status: 'default'
    }
  ],
  [
    {
      id: '1',
      status: 'success'
    },
    {
      id: '2',
      status: 'success'
    },
    {
      id: '3',
      status: 'running'
    },
    {
      id: '4',
      status: 'running'
    }
  ],
  [
    {
      id: '1',
      status: 'success'
    },
    {
      id: '2',
      status: 'success'
    },
    {
      id: '3',
      status: 'success'
    },
    {
      id: '4',
      status: 'failed'
    }
  ]
]

export default defineComponent({
  props: {
    job: {
      type: Object as PropType<Job>,
      required: true
    }
  },
  mounted() {
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
          return magnet.getAttribute('port-group') !== 'top'
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
        if (status === 'running') {
          edge.attr('line/strokeDasharray', 5)
          edge.attr('line/style/animation', 'running-line 30s infinite linear')
        } else {
          edge.attr('line/strokeDasharray', '')
          edge.attr('line/style/animation', '')
        }
      })
    })

    // 初始化节点/边
    const init = (data: Cell.Metadata[]) => {
      const cells: Cell[] = []
      data.forEach((item) => {
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
      const status = statusList.shift()
      status?.forEach((item) => {
        const { id, status } = item
        const node = graph.getCellById(id)
        const data = node.getData() as NodeStatus
        node.setData({
          ...data,
          status
        })
      })
      setTimeout(() => {
        showNodeStatus(statusList)
      }, 3000)
    }
    // fetch('/data/dag.json')
    //   .then((response) => response.json())
    //   .then((data) => {
    //     init(data)
    //     showNodeStatus(nodeStatusList)
    //     graph.centerContent()
    //   })
    setTimeout(() => {
      const data = JsonData
      init(data)
      showNodeStatus(nodeStatusList)
      graph.centerContent()
    }, 5000)
  },
  setup(props) {
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
      },
      {
        title: 'Create Time',
        key: 'createTime',
        render: () => props?.job?.createTime
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
