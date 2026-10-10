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

export type PluginType = 'source' | 'sink' | 'transform'

export interface OptionMetadata {
  key: string
  type: string
  defaultValue?: unknown
  description?: string
  fallbackKeys?: string[]
  optionValues?: unknown[] | null
}

export interface RequiredOptionRule {
  ruleType: string
  options: OptionMetadata[]
  expression?: string
  expressionTree?: unknown
}

export interface ConditionRule {
  expression?: string
  expressionTree?: unknown
  optionRule: {
    optionalOptions?: OptionMetadata[]
    requiredOptions?: RequiredOptionRule[]
    conditionRules?: ConditionRule[]
    valueConstraints?: ValueConstraint[]
  }
}

export interface ValueConstraint {
  expression?: string
  conditionTree?: unknown
}

export interface OptionRuleResponse {
  engineType: string
  pluginType: PluginType
  pluginName: string
  optionRule: {
    optionalOptions?: OptionMetadata[]
    requiredOptions?: RequiredOptionRule[]
    conditionRules?: ConditionRule[]
    valueConstraints?: ValueConstraint[]
  }
}

export interface HttpServiceStatus {
  httpEnabled: boolean
  httpsEnabled: boolean
  configuredHttpPort: number
  configuredHttpsPort: number
  httpPort: number
  httpsPort: number
  contextPath: string
  dynamicPortEnabled: boolean
  portRange: number
  basicAuthEnabled: boolean
  mutualTlsEnabled: boolean
}
