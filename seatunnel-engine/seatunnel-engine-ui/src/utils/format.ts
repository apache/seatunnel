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

export function formatPercentFromRatio(ratio: unknown, digits = 2): string {
  const value =
    typeof ratio === 'number'
      ? ratio
      : ratio === null || ratio === undefined
        ? Number.NaN
        : Number(ratio)

  if (!Number.isFinite(value)) {
    return ''
  }

  const percent = value * 100
  if (percent !== 0 && Math.abs(percent) < 0.01) {
    return '<0.01%'
  }

  const formatted = percent.toFixed(digits).replace(/\.?0+$/, '')
  return `${formatted}%`
}

