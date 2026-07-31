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

export default {
  optionRules: {
    title: 'Connector OptionRule',
    pluginType: '插件类型',
    pluginName: '插件名称',
    pluginNamePlaceholder: 'FakeSource、Console、Filter 等',
    load: '加载',
    pluginRequired: '插件名称不能为空。',
    loadFailed: '加载 Connector OptionRule 失败。',
    required: '必填',
    optional: '可选',
    conditionRules: '条件规则',
    valueConstraints: '取值约束',
    section: '分组',
    ruleType: '规则类型',
    expression: '表达式',
    requiredCount: '必填项数量',
    optionalCount: '可选项数量',
    conditionCount: '嵌套条件数量',
    condition: '条件',
    key: '配置项',
    type: '类型',
    defaultValue: '默认值',
    description: '说明'
  },
  httpStatus: {
    title: 'HTTP 服务状态',
    refresh: '刷新',
    loadFailed: '加载 HTTP 服务状态失败。',
    enabled: '已开启',
    disabled: '未开启',
    http: 'HTTP',
    https: 'HTTPS',
    httpPort: 'HTTP 端口',
    httpsPort: 'HTTPS 端口',
    contextPath: 'Context Path',
    dynamicPort: '动态端口',
    basicAuth: 'Basic Auth',
    mutualTls: '双向 TLS'
  }
}
