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

import { get } from '@/service/service'
import type { Job } from './types'

export const getRunningJobs = () => get<Job[]>('/running-jobs')
export const getFinishedJobs = () => get<Job[]>(`/finished-jobs`)
export const getJobInfo = (jobId: string) => get<Job>(`/job-info/${jobId}`)
export const getRunningJobInfo = (jobId: string) => get<Job>(`/running-job/${jobId}`)

export const JobsService = {
  getRunningJobs,
  getFinishedJobs,
  getJobInfo,
  getRunningJobInfo
}
