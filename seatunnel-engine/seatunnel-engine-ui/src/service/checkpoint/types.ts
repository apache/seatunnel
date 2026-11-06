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

export interface CheckpointCounts {
  triggered: number
  completed: number
  failed: number
  inProgress: number
  restored: number
}

export interface CheckpointInfo {
  checkpointId?: number
  checkpointType?: string
  status?: string
  triggerTimestamp?: number
  completedTimestamp?: number
  durationMillis?: number
  stateSize?: number
  failureReason?: string
}

export interface InProgressCheckpoint {
  checkpointId: number
  checkpointType?: string
  triggerTimestamp: number
  acknowledged: number
  total: number
}

export interface CheckpointHistoryEntry {
  pipelineId: number
  checkpoint: CheckpointInfo
}

export interface PipelineCheckpointOverview {
  pipelineId: number
  counts: CheckpointCounts
  latestCompleted: CheckpointInfo
  latestFailed: CheckpointInfo
  latestSavepoint: CheckpointInfo
  inProgress: InProgressCheckpoint[]
  history: CheckpointHistoryEntry[]
}

export interface CheckpointOverviewResponse {
  jobId: string
  updatedAt?: number
  pipelines?: PipelineCheckpointOverview[]
}
