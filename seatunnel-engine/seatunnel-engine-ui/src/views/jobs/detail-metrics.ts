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

import type { Vertex } from '@/service/job/types'

const VERTEX_IDENTIFIER_PATTERN = /((?:Sink|Source|Transform)\[\d+\])/

type MetricMap = Record<string, string> | undefined
type MetricVertex = Pick<Vertex, 'vertexName' | 'tablePaths'>

export const extractVertexIdentifier = (vertexName?: string): string | undefined => {
    return vertexName?.match(VERTEX_IDENTIFIER_PATTERN)?.[1]
}

const resolveMetricKey = (
    metricMap: MetricMap,
    vertex: MetricVertex,
    path: string
): string | undefined => {
    if (!metricMap) return undefined

    const identifier = extractVertexIdentifier(vertex.vertexName)

    if (identifier) {
        const prefixedKey = `${identifier}.${path}`
        if (metricMap[prefixedKey] !== undefined) return prefixedKey
    }

    if (metricMap[path] !== undefined) return path

    const suffix = `.${path}`
    const suffixedKeys = Object.keys(metricMap).filter((key) => key.endsWith(suffix))

    if (identifier) {
        const sameVertexKey = suffixedKeys.find((key) => key.startsWith(`${identifier}.`))
        if (sameVertexKey) return sameVertexKey
    }

    return suffixedKeys.length === 1 ? suffixedKeys[0] : undefined
}

export const readVertexMetricValue = (
    metricMap: MetricMap,
    vertex: MetricVertex,
    path: string
): number => {
    if (!metricMap) return 0
    const metricKey = resolveMetricKey(metricMap, vertex, path)
    if (!metricKey) return 0
    const value = Number(metricMap[metricKey])
    return Number.isFinite(value) ? value : 0
}

export const collectVertexMetrics = (
    metricName: string,
    metricMap: MetricMap,
    vertex: MetricVertex
): Record<string, string> => {
    const metrics: Record<string, string> = {}
    if (!metricMap) return metrics

    vertex.tablePaths.forEach((path) => {
        const metricKey = resolveMetricKey(metricMap, vertex, path)
        if (metricKey !== undefined) {
            metrics[`${metricName}.${path}`] = metricMap[metricKey]
        }
    })
    return metrics
}