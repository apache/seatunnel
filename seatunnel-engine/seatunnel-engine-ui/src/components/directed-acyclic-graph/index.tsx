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
import { Graph, Node, Path, Edge, Platform, StringExt } from '@antv/x6'
import { Selection } from '@antv/x6-plugin-selection'
import { register } from '@antv/x6-vue-shape'
import { NTooltip, NDropdown } from 'naive-ui'

export default defineComponent({
  setup() {
    // 节点类型
    enum NodeType {
      INPUT = 'INPUT', // 数据输入
      FILTER = 'FILTER', // 数据过滤
      JOIN = 'JOIN', // 数据连接
      UNION = 'UNION', // 数据合并
      AGG = 'AGG', // 数据聚合
      OUTPUT = 'OUTPUT' // 数据输出
    }

    // 元素校验状态
    enum CellStatus {
      DEFAULT = 'default',
      SUCCESS = 'success',
      ERROR = 'error'
    }

    // 节点位置信息
    interface Position {
      x: number
      y: number
    }

    // 加工类型列表
    const PROCESSING_TYPE_LIST = [
      {
        type: 'FILTER',
        name: '数据筛选'
      },
      {
        type: 'JOIN',
        name: '数据连接'
      },
      {
        type: 'UNION',
        name: '数据合并'
      },
      {
        type: 'AGG',
        name: '数据聚合'
      },

      {
        type: 'OUTPUT',
        name: '数据输出'
      }
    ]

    // 不同节点类型的icon
    const NODE_TYPE_LOGO = {
      INPUT:
        'https://mdn.alipayobjects.com/huamei_f4t1bn/afts/img/A*RXnuTpQ22xkAAAAAAAAAAAAADtOHAQ/original', // 数据输入
      FILTER:
        'https://mdn.alipayobjects.com/huamei_f4t1bn/afts/img/A*ZJ6qToit8P4AAAAAAAAAAAAADtOHAQ/original', // 数据筛选
      JOIN: 'https://mdn.alipayobjects.com/huamei_f4t1bn/afts/img/A*EHqyQoDeBvIAAAAAAAAAAAAADtOHAQ/original', // 数据连接
      UNION:
        'https://mdn.alipayobjects.com/huamei_f4t1bn/afts/img/A*k4eyRaXv8gsAAAAAAAAAAAAADtOHAQ/original', // 数据合并
      AGG: 'https://mdn.alipayobjects.com/huamei_f4t1bn/afts/img/A*TKG8R6nfYiAAAAAAAAAAAAAADtOHAQ/original', // 数据聚合
      OUTPUT:
        'https://mdn.alipayobjects.com/huamei_f4t1bn/afts/img/A*zUgORbGg1HIAAAAAAAAAAAAADtOHAQ/original' // 数据输出
    }

    /**
     * 根据起点初始下游节点的位置信息
     * @param node 起始节点
     * @param graph
     * @returns
     */
    const getDownstreamNodePosition = (node: Node, graph: Graph, dx = 250, dy = 100) => {
      // 找出画布中以该起始节点为起点的相关边的终点id集合
      const downstreamNodeIdList: string[] = []
      graph.getEdges().forEach((edge) => {
        const originEdge = edge.toJSON()?.data
        if (originEdge.source === node.id) {
          downstreamNodeIdList.push(originEdge.target)
        }
      })
      // 获取起点的位置信息
      const position = node.getPosition()
      let minX = Infinity
      let maxY = -Infinity
      graph.getNodes().forEach((graphNode) => {
        if (downstreamNodeIdList.indexOf(graphNode.id) > -1) {
          const nodePosition = graphNode.getPosition()
          // 找到所有节点中最左侧的节点的x坐标
          if (nodePosition.x < minX) {
            minX = nodePosition.x
          }
          // 找到所有节点中最x下方的节点的y坐标
          if (nodePosition.y > maxY) {
            maxY = nodePosition.y
          }
        }
      })

      return {
        x: minX !== Infinity ? minX : position.x + dx,
        y: maxY !== -Infinity ? maxY + dy : position.y
      }
    }

    // 根据节点的类型获取ports
    const getPortsByType = (type: NodeType, nodeId: string) => {
      let ports = []
      switch (type) {
        case NodeType.INPUT:
          ports = [
            {
              id: `${nodeId}-out`,
              group: 'out'
            }
          ]
          break
        case NodeType.OUTPUT:
          ports = [
            {
              id: `${nodeId}-in`,
              group: 'in'
            }
          ]
          break
        default:
          ports = [
            {
              id: `${nodeId}-in`,
              group: 'in'
            },
            {
              id: `${nodeId}-out`,
              group: 'out'
            }
          ]
          break
      }
      return ports
    }

    /**
     * 创建节点并添加到画布
     * @param type 节点类型
     * @param graph
     * @param position 节点位置
     * @returns
     */
    export const createNode = (type: NodeType, graph: Graph, position?: Position) => {
      if (!graph) {
        return {}
      }
      let newNode = {}
      const sameTypeNodes = graph.getNodes().filter((item) => item.getData()?.type === type)
      const typeName = PROCESSING_TYPE_LIST?.find((item) => item.type === type)?.name
      const id = StringExt.uuid()
      const node = {
        id,
        shape: 'data-processing-dag-node',
        x: position?.x,
        y: position?.y,
        ports: getPortsByType(type, id),
        data: {
          name: `${typeName}_${sameTypeNodes.length + 1}`,
          type
        }
      }
      newNode = graph.addNode(node)
      return newNode
    }

    /**
     * 创建边并添加到画布
     * @param source
     * @param target
     * @param graph
     */
    const createEdge = (source: string, target: string, graph: Graph) => {
      const edge = {
        id: StringExt.uuid(),
        shape: 'data-processing-curve',
        source: {
          cell: source,
          port: `${source}-out`
        },
        target: {
          cell: target,
          port: `${target}-in`
        },
        zIndex: -1,
        data: {
          source,
          target
        }
      }
      if (graph) {
        graph.addEdge(edge)
      }
    }

    class DataProcessingDagNode extends defineComponent<{
      node: Node
    }> {
      state = {
        plusActionSelected: false
      }

      // 创建下游的节点和边
      createDownstream = (type: NodeType) => {
        const { node } = this.props
        const { graph } = node.model || {}
        if (graph) {
          // 获取下游节点的初始位置信息
          const position = getDownstreamNodePosition(node, graph)
          // 创建下游节点
          const newNode = createNode(type, graph, position)
          const source = node.id
          const target = newNode.id
          // 创建该节点出发到下游节点的边
          createEdge(source, target, graph)
        }
      }

      // 点击添加下游+号
      clickPlusDragMenu = (type: NodeType) => {
        this.createDownstream(type)
        this.setState({
          plusActionSelected: false
        })
      }

      //  获取+号下拉菜单
      getPlusDagMenu = () => {
        return (
          <ul>
            {PROCESSING_TYPE_LIST.map((item) => {
              const content = (
                // eslint-disable-next-line
                <a onClick={() => this.clickPlusDragMenu(item.type)}>
                  <i
                    className="node-mini-logo"
                    style={{ backgroundImage: `url(${NODE_TYPE_LOGO[item.type]})` }}
                  />

                  <span>{item.name}</span>
                </a>
              )
              return (
                <li className="each-sub-menu" key={item.type}>
                  {content}
                </li>
              )
            })}
          </ul>
        )
      }

      // 添加下游菜单的打开状态变化
      onPlusDropdownOpenChange = (value: boolean) => {
        this.setState({
          plusActionSelected: value
        })
      }

      // 鼠标进入矩形主区域的时候显示连接桩
      onMainMouseEnter = () => {
        const { node } = this.props
        // 获取该节点下的所有连接桩
        const ports = node.getPorts() || []
        ports.forEach((port) => {
          node.setPortProp(port.id, 'attrs/circle', {
            fill: '#fff',
            stroke: '#85A5FF'
          })
        })
      }

      // 鼠标离开矩形主区域的时候隐藏连接桩
      onMainMouseLeave = () => {
        const { node } = this.props
        // 获取该节点下的所有连接桩
        const ports = node.getPorts() || []
        ports.forEach((port) => {
          node.setPortProp(port.id, 'attrs/circle', {
            fill: 'transparent',
            stroke: 'transparent'
          })
        })
      }

      render() {
        const { plusActionSelected } = this.state
        const { node } = this.props
        const data = node?.getData()
        const { name, type, status, statusMsg } = data

        return (
          <div className="data-processing-dag-node">
            <div
              className="main-area"
              onMouseEnter={this.onMainMouseEnter}
              onMouseLeave={this.onMainMouseLeave}
            >
              <div className="main-info">
                {/* 节点类型icon */}
                <i
                  className="node-logo"
                  style={{ backgroundImage: `url(${NODE_TYPE_LOGO[type]})` }}
                />
                <NTooltip title={name} mouseEnterDelay={0.8}>
                  <div className="ellipsis-row node-name">{name}</div>
                </NTooltip>
              </div>

              {/* 节点状态信息 */}
              <div className="status-action">
                {status === CellStatus.ERROR && (
                  <NTooltip title={statusMsg}>
                    <i className="status-icon status-icon-error" />
                  </NTooltip>
                )}
                {status === CellStatus.SUCCESS && <i className="status-icon status-icon-success" />}

                {/* 节点操作菜单 */}
                <div className="more-action-container">
                  <i className="more-action" />
                </div>
              </div>
            </div>

            {/* 添加下游节点 */}
            {type !== NodeType.OUTPUT && (
              <div className="plus-dag">
                <NDropdown
                  dropdownRender={this.getPlusDagMenu}
                  overlayClassName="processing-node-menu"
                  trigger={['click']}
                  placement="bottom"
                  open={plusActionSelected}
                  onOpenChange={this.onPlusDropdownOpenChange}
                >
                  <i
                    className={`plus-action${plusActionSelected ? ' plus-action-selected' : ''}`}
                  />
                </NDropdown>
              </div>
            )}
          </div>
        )
      }
    }

    register({
      shape: 'data-processing-dag-node',
      width: 212,
      height: 48,
      component: DataProcessingDagNode,
      // port默认不可见
      ports: {
        groups: {
          in: {
            position: 'left',
            attrs: {
              circle: {
                r: 4,
                magnet: true,
                stroke: 'transparent',
                strokeWidth: 1,
                fill: 'transparent'
              }
            }
          },

          out: {
            position: {
              name: 'right',
              args: {
                dx: -32
              }
            },

            attrs: {
              circle: {
                r: 4,
                magnet: true,
                stroke: 'transparent',
                strokeWidth: 1,
                fill: 'transparent'
              }
            }
          }
        }
      }
    })

    // 注册连线
    Graph.registerConnector(
      'curveConnector',
      (sourcePoint, targetPoint) => {
        const hgap = Math.abs(targetPoint.x - sourcePoint.x)
        const path = new Path()
        path.appendSegment(Path.createSegment('M', sourcePoint.x - 4, sourcePoint.y))
        path.appendSegment(Path.createSegment('L', sourcePoint.x + 12, sourcePoint.y))
        // 水平三阶贝塞尔曲线
        path.appendSegment(
          Path.createSegment(
            'C',
            sourcePoint.x < targetPoint.x ? sourcePoint.x + hgap / 2 : sourcePoint.x - hgap / 2,
            sourcePoint.y,
            sourcePoint.x < targetPoint.x ? targetPoint.x - hgap / 2 : targetPoint.x + hgap / 2,
            targetPoint.y,
            targetPoint.x - 6,
            targetPoint.y
          )
        )
        path.appendSegment(Path.createSegment('L', targetPoint.x + 2, targetPoint.y))

        return path.serialize()
      },
      true
    )

    Edge.config({
      markup: [
        {
          tagName: 'path',
          selector: 'wrap',
          attrs: {
            fill: 'none',
            cursor: 'pointer',
            stroke: 'transparent',
            strokeLinecap: 'round'
          }
        },
        {
          tagName: 'path',
          selector: 'line',
          attrs: {
            fill: 'none',
            pointerEvents: 'none'
          }
        }
      ],
      connector: { name: 'curveConnector' },
      attrs: {
        wrap: {
          connection: true,
          strokeWidth: 10,
          strokeLinejoin: 'round'
        },
        line: {
          connection: true,
          stroke: '#A2B1C3',
          strokeWidth: 1,
          targetMarker: {
            name: 'classic',
            size: 6
          }
        }
      }
    })

    Graph.registerEdge('data-processing-curve', Edge, true)

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
        sourceAnchor: {
          name: 'left',
          args: {
            dx: Platform.IS_SAFARI ? 4 : 8
          }
        },
        targetAnchor: {
          name: 'right',
          args: {
            dx: Platform.IS_SAFARI ? 4 : -8
          }
        },
        createEdge() {
          return graph.createEdge({
            shape: 'data-processing-curve',
            attrs: {
              line: {
                strokeDasharray: '5 5'
              }
            },
            zIndex: -1
          })
        },
        // 连接桩校验
        validateConnection({ sourceMagnet, targetMagnet }) {
          // 只能从输出链接桩创建连接
          if (!sourceMagnet || sourceMagnet.getAttribute('port-group') === 'in') {
            return false
          }
          // 只能连接到输入链接桩
          if (!targetMagnet || targetMagnet.getAttribute('port-group') !== 'in') {
            return false
          }
          return true
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

    // 节点状态列表
    const nodeStatusList = [
      {
        id: 'node-0',
        status: 'success'
      },
      {
        id: 'node-1',
        status: 'success'
      },
      {
        id: 'node-2',
        status: 'success'
      },
      {
        id: 'node-3',
        status: 'success'
      },
      {
        id: 'node-4',
        status: 'error',
        statusMsg: '错误信息示例'
      }
    ]

    // 边状态列表
    const edgeStatusList = [
      {
        id: 'edge-0',
        status: 'success'
      },
      {
        id: 'edge-1',
        status: 'success'
      },
      {
        id: 'edge-2',
        status: 'success'
      },
      {
        id: 'edge-3',
        status: 'success'
      }
    ]

    // 显示节点状态
    const showNodeStatus = () => {
      nodeStatusList.forEach((item) => {
        const { id, status, statusMsg } = item
        const node = graph.getCellById(id)
        const data = node.getData() as CellStatus
        node.setData({
          ...data,
          status,
          statusMsg
        })
      })
    }

    // 开启边的运行动画
    const excuteAnimate = () => {
      graph.getEdges().forEach((edge) => {
        edge.attr({
          line: {
            stroke: '#3471F9'
          }
        })
        edge.attr('line/strokeDasharray', 5)
        edge.attr('line/style/animation', 'running-line 30s infinite linear')
      })
    }

    // 关闭边的动画
    const stopAnimate = () => {
      graph.getEdges().forEach((edge) => {
        edge.attr('line/strokeDasharray', 0)
        edge.attr('line/style/animation', '')
      })
      edgeStatusList.forEach((item) => {
        const { id, status } = item
        const edge = graph.getCellById(id)
        if (status === 'success') {
          edge.attr('line/stroke', '#52c41a')
        }
        if (status === 'error') {
          edge.attr('line/stroke', '#ff4d4f')
        }
      })
      // 默认选中一个节点
      graph.select('node-2')
    }

    fetch('/data/data-processing-dag.json')
      .then((response) => response.json())
      .then((data) => {
        graph.fromJSON(data)
        const zoomOptions = {
          padding: {
            left: 10,
            right: 10
          }
        }
        graph.zoomToFit(zoomOptions)
        setTimeout(() => {
          excuteAnimate()
        }, 2000)
        setTimeout(() => {
          showNodeStatus()
          stopAnimate()
        }, 3000)
      })

    return () => (
      <div class="w-full bg-white px-12 pt-6 pb-12 border border-gray-100 rounded-xl">
        <div id="container"></div>
      </div>
    )
  }
})
