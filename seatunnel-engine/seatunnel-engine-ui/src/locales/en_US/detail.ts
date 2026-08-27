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
  createTime: 'Create Time',
  duration: 'Duration',
  tabs: {
    overview: 'Overview',
    exception: 'Exception',
    configuration: 'Configuration',
    log: 'Log',
  },
  table: {
    name: 'Name',
    receivedBytes: 'Received Bytes',
    writeBytes: 'Write Bytes',
    receivedCount: 'Received Count',
    writeCount: 'Write Count',
    receivedQps: 'Received QPS',
    writeQps: 'Write QPS',
    receivedBytesPerSecond: 'Received Bytes PerSecond',
    writeBytesPerSecond: 'Write Bytes PerSecond',
  },
  observability: {
    time: 'Time',
    // Source
    sourceReadRatio: 'Source Read Ratio',
    sourceIdleRatio: 'Source Idle Ratio',
    // Transform
    transformBusyRatio: 'Transform Busy Ratio',
    processMsPerRecord: 'Process (ms/record)',
    recordsIn: 'Records In',
    recordsOut: 'Records Out',
    // Sink
    sinkBusyRatio: 'Sink Busy Ratio',
    writeMsPerRecord: 'Write (ms/record)',
    // Edge
    bpRatio: 'Downstream Wait Ratio',
    queueFillRatio: 'Queue Fill Ratio',
  },
}
