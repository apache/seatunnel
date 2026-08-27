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

import type { JobStatus } from '@/service/job/types'
import { useThemeVars } from 'naive-ui'

export const getColorFromStatus = (status: JobStatus) => {
  const colors = useThemeVars().value
  switch (status) {
    case 'RUNNING':
      return { textColor: colors.successColor, color: colors.successColor + '1a' }
    case 'INITIALIZING':
    case 'CREATED':
    case 'SCHEDULED':
    case 'DOING_SAVEPOINT':
    case 'SAVEPOINT_DONE':
      return { textColor: colors.infoColor + '8c', color: colors.infoColor + '0f' }
    case 'FINISHED':
      return { textColor: colors.infoColor, color: colors.infoColor + '1a' }
    case 'CANCELING':
    case 'CANCELED':
      return { textColor: colors.warningColor, color: colors.warningColor + '1a' }
    case 'FAILING':
    case 'FAILED':
      return { textColor: colors.errorColor, color: colors.errorColor + '1a' }
    default:
      return undefined
  }
}
