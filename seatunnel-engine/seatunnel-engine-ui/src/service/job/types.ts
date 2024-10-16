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
export type Path = string
export interface Vertex {
  vertexId: number
  type: 'source' | 'sink'
  vertexName: string
  tablePaths: Path[]
}
export interface Edge {
  inputVertexId: string
  targetVertexId: string
}
export interface Metrics {
  SinkWriteCount: string
  SinkWriteBytesPerSeconds: string
  SinkWriteQPS: string
  SourceReceivedBytes: string
  SourceReceivedBytesPerSeconds: string
  SourceReceivedCount: string
  SourceReceivedQPS: string
  SinkWriteBytes: string
  TableSourceReceivedBytes: Record<Path, string>
  TableSourceReceivedCount: Record<Path, string>
  TableSourceReceivedQPS: Record<Path, string>
  TableSourceReceivedBytesPerSeconds: Record<Path, string>
  TableSinkWriteBytes: Record<Path, string>
  TableSinkWriteCount: Record<Path, string>
  TableSinkWriteQPS: Record<Path, string>
  TableSinkWriteBytesPerSeconds: Record<Path, string>
}
export interface EnvOptions {
  'checkpoint.interval': string
  'job.mode': string
  parallelism: string
}
export interface Job {
  jobId: string
  jobName: string
  jobStatus: 'RUNNING' | 'FINISHED'
  errorMsg: string
  createTime: string
  finishTime: string
  envOptions?: EnvOptions
  jobDag: {
    jobId: string
    pipelineEdges: Record<string, Edge[]>
    vertexInfoMap: Vertex[]
    envOptions?: EnvOptions
  }
  metrics: Metrics
  pluginJarsUrls: []
}
export type JobFinishedState = 'FINISHED' | 'CANCELED' | 'FAILED' | 'UNKNOWABLE'
