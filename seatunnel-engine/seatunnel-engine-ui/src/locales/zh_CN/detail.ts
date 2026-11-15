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
  liveMetrics: {
    pinnedTitle: '已固定实时指标',
    pinnedHint: '会话级 · 最多 {limit} 条 · 复用 Overview 轮询',
    emptyPinned: '在节点/边抽屉中 Pin 指标后，可在此持续观察。',
    pin: '固定',
    unpin: '取消固定',
    pinLimit: '已达 Pin 上限（{limit}），请先取消一条。',
    chartEmpty: '当前窗口暂无时序数据',
    unitRatio: '占比',
    unitDuration: '耗时（毫秒/条）',
    unitCount: '条数'
  }
}
