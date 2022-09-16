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

import { axios } from '@/service/service'
import type {
  TaskList,
  TaskJobList,
  TaskRecycle,
  TaskExecute
} from '@/service/task/types'

export function taskInstanceList(params: TaskList): any {
  return axios({
    url: '/task/instance',
    method: 'get',
    params
  })
}

export function taskJobList(params: TaskList): any {
  return axios({
    url: '/task/job',
    method: 'get',
    params
  })
}

export function taskExecute(scriptId: number, data: TaskExecute): any {
  return axios({
    url: `/task/${scriptId}/execute`,
    method: 'post',
    data
  })
}

export function taskRecycle(scriptId: number): any {
  return axios({
    url: `/task/${scriptId}/recycle`,
    method: 'patch'
  })
}

export function taskInstanceDetail(taskInstanceId: number): any {
  return axios({
    url: `/task/${taskInstanceId}`,
    method: 'get'
  })
}

export function taskInstanceKill(taskInstanceId: number): any {
  return axios({
    url: `/task/${taskInstanceId}`,
    method: 'post'
  })
}