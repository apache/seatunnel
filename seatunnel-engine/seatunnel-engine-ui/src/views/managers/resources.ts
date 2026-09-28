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

import type { Monitor, WorkerResource } from '@/service/manager/types'

export interface NodeResources {
  address: string
  monitor?: Monitor
  resource?: WorkerResource
}

// Hazelcast brackets IPv6 addresses; monitoring returns host and port separately.
export function addressKey(address: string): string {
  const separator = address.lastIndexOf(':')
  if (separator < 0) return address
  const host = address.slice(0, separator).replace(/^\[|\]$/g, '')
  const port = address.slice(separator + 1)
  if (host.includes(':')) {
    try {
      return `${new URL(`http://[${host}]:${port}`).hostname}:${port}`
    } catch {
      return `[${host}]:${port}`
    }
  }
  return `${host.toLowerCase()}:${port}`
}

export function joinResources(
  monitors: Monitor[],
  workers: WorkerResource[],
  master: boolean
): NodeResources[] {
  monitors = monitors.filter(
    (monitor) =>
      monitor &&
      typeof monitor.host === 'string' &&
      (typeof monitor.port === 'string' || typeof monitor.port === 'number')
  )
  const rows = new Map<string, NodeResources>()
  for (const monitor of monitors) {
    const address = addressKey(`${monitor.host}:${monitor.port}`)
    if (monitor.isMaster === String(master)) rows.set(address, { address, monitor })
  }
  if (!master) {
    const byAddress = new Map(
      monitors.map((monitor) => [addressKey(`${monitor.host}:${monitor.port}`), monitor])
    )
    for (const resource of workers) {
      if (!resource || typeof resource.address !== 'string') continue
      const address = addressKey(resource.address)
      // A mixed-role member can be both the master and a registered worker.
      rows.set(address, { address, monitor: byAddress.get(address), resource })
    }
  }
  return Array.from(rows.values())
}

export function numberValue(value: number | null | undefined): string {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? String(value) : '—'
}

export function ratioValue(value: number | null | undefined): string {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1
    ? `${(value * 100).toFixed(1)}%`
    : '—'
}

export function bytesValue(value: number | null | undefined): string {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return '—'
  return `${(value / 1024 / 1024).toFixed(1)} MiB`
}
