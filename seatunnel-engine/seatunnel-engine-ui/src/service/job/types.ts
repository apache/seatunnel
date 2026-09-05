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
  type: 'source' | 'sink' | 'transform'
  vertexName: string
  tablePaths: Path[]
}
export interface Edge {
  inputVertexId: string
  targetVertexId: string
}
export type MetricMapKey = {
  [K in keyof Metrics]: Metrics[K] extends Record<string, string> ? K : never
}[keyof Metrics]

export interface Metrics {
  SinkWriteCount: string
  SinkWriteBytesPerSeconds: string
  SinkWriteQPS: string
  SourceReceivedBytes: string
  SourceReceivedBytesPerSeconds: string
  SourceReceivedCount: string
  SourceReceivedQPS: string
  SinkWriteBytes: string
  FlushSignalTotal: string
  FlushSignalQueueSuccessTotal: string
  FlushSignalQueueFailureTotal: string
  FlushSignalSinkSuccessTotal: string
  FlushSignalSinkFailureTotal: string
  FlushSignalQPSPerVertex: Record<Path, string>
  FlushSignalSinkQPSPerVertex: Record<Path, string>
  FlushSignalTotalPerVertex: Record<Path, string>
  FlushSignalQueueSuccessTotalPerVertex: Record<Path, string>
  FlushSignalQueueFailureTotalPerVertex: Record<Path, string>
  FlushSignalSinkSuccessTotalPerVertex: Record<Path, string>
  FlushSignalSinkFailureTotalPerVertex: Record<Path, string>
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
export type JobStatus =
  | 'INITIALIZING'
  | 'CREATED'
  | 'SCHEDULED'
  | 'RUNNING'
  | 'FAILING'
  | 'FAILED'
  | 'DOING_SAVEPOINT'
  | 'SAVEPOINT_DONE'
  | 'CANCELING'
  | 'CANCELED'
  | 'FINISHED'
  | 'UNKNOWABLE'
export interface Job {
  jobId: string
  jobName: string
  jobStatus: JobStatus
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

export interface JobPage {
  total: number
  data: Job[]
}

export type ConfigFormat = 'json' | 'hocon' | 'sql'

export interface SubmitJobRequest {
  config: string
  format: ConfigFormat
  jobName?: string
  jobId?: string
  isStartWithSavePoint?: boolean
}

export interface SubmitJobFileRequest {
  file: File
  jobName?: string
  jobId?: string
  isStartWithSavePoint?: boolean
}

export interface SubmitJobResponse {
  jobId: string | number
  jobName: string
}

export interface StopJobRequest {
  jobId: string | number
  isStopWithSavePoint?: boolean
  force?: boolean
}

export interface StopJobResponse {
  jobId: string | number
}

export interface CheckpointCounts {
  triggered: number
  completed: number
  failed: number
  inProgress: number
  restored: number
}

export interface CheckpointInfo {
  checkpointId: number
  checkpointType: string
  status?: string
  triggerTimestamp?: number
  completedTimestamp?: number
  durationMillis?: number
  stateSize?: number
  failureReason?: string
  acknowledged?: number
  total?: number
}

export interface CheckpointHistoryRecord {
  pipelineId: number
  checkpoint: CheckpointInfo
}

export interface CheckpointPipeline {
  pipelineId: number
  counts: CheckpointCounts
  latestCompleted?: CheckpointInfo | null
  latestFailed?: CheckpointInfo | null
  latestSavepoint?: CheckpointInfo | null
  inProgress?: CheckpointInfo[]
  history?: CheckpointHistoryRecord[]
}

export interface CheckpointOverview {
  jobId: string
  updatedAt: number
  pipelines: CheckpointPipeline[]
}
