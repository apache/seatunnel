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

export default {
  id: 'ID',
  createTime: '开始时间',
  duration: '运行时间',
  tabs: {
    overview: '概览',
    exception: '异常',
    configuration: '配置',
    log: '日志'
  },
  table: {
    name: '名称',
    receivedBytes: '读取字节数',
    writeBytes: '写入字节数',
    receivedCount: '读取条数',
    writeCount: '写入条数',
    receivedQps: '读取 QPS',
    writeQps: '写入 QPS',
    receivedBytesPerSecond: '读取字节/秒',
    writeBytesPerSecond: '写入字节/秒'
  },
  observability: {
    time: '时间',
    // Source
    sourceReadRatio: 'Source 读取占比',
    sourceIdleRatio: 'Source 空闲占比',
    // Transform
    transformBusyRatio: 'Transform 忙碌占比',
    processMsPerRecord: '处理耗时（毫秒/条）',
    recordsIn: '输入条数',
    recordsOut: '输出条数',
    // Sink
    sinkBusyRatio: 'Sink 忙碌占比',
    writeMsPerRecord: '写入耗时（毫秒/条）',
    // Edge
    bpRatio: '下游等待占比',
    queueFillRatio: '队列填充率'
  },
  runtime: {
    graphSize: '图规模',
    vertices: '个节点',
    edges: '条边',
    largeGraphActive: '已启用大图摘要',
    normalGraph: '普通规模图',
    checkpoint: '检查点',
    checkpointUnavailable: '检查点概览不可用',
    completed: '已完成',
    inProgress: '进行中',
    latestCompleted: '最近完成',
    latestFailed: '最近失败',
    latestSavepoint: '最近 Savepoint',
    updatedAt: '更新时间',
    runtimeError: '运行错误',
    noRuntimeError: '暂无运行错误',
    largeGraphSummary:
      '检测到大规模运行图。优先查看节点和下游输入热点摘要，再按需缩放拓扑定位具体节点。',
    topBusyVertices: '最忙节点',
    topBlockedEdges: '最堵输入',
    vertex: '节点',
    type: '类型',
    busyRatio: '忙碌占比',
    from: '来源',
    to: '目标',
    downstreamWait: '下游等待',
    queueFill: '队列填充',
    queueSize: '队列大小',
    unavailable: '无数据'
  }
}
