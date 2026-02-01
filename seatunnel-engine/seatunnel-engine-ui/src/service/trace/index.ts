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

import axios from 'axios'
import type { AxiosInstance } from 'axios'
import type { TraceDetail, TraceSummary } from '@/service/trace/types'

export interface TraceListResponse {
  count: number
  items: TraceSummary[]
}

const getTraceCollectorClient = (): AxiosInstance => {
  const baseURL = import.meta.env.VITE_TRACE_COLLECTOR_BASE || ''
  if (!baseURL) {
    throw new Error('Trace collector is not configured (VITE_TRACE_COLLECTOR_BASE)')
  }
  const token = import.meta.env.VITE_TRACE_COLLECTOR_TOKEN || ''
  const client = axios.create({
    timeout: 6000,
    baseURL
  })
  client.interceptors.request.use((config) => {
    if (token) {
      config.headers = config.headers || {}
      config.headers['X-Seatunnel-Token'] = token
    }
    return config
  })
  return client
}

export const listTraces = (params: {
  jobId?: string
  tableId?: string
  fromMs?: number
  toMs?: number
  limit?: number
  offset?: number
}) => getTraceCollectorClient().get<TraceListResponse>('/api/v1/traces', { params }).then((r) => r.data)

export const getTrace = (traceId: string, params?: { sinkTaskId?: string }) =>
  getTraceCollectorClient()
    .get<TraceDetail>(`/api/v1/traces/${traceId}`, { params })
    .then((r) => r.data)

export const TraceService = {
  listTraces,
  getTrace
}

