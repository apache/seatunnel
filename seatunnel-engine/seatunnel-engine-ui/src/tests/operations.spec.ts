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

import { describe, test, expect, vi, afterEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import i18n from '@/locales'
import operations from '@/views/operations'
import { operationsService } from '@/service/operations'

describe('operations', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  test('Operations component', async () => {
    vi.spyOn(operationsService, 'getOptionRules').mockResolvedValue({
      engineType: 'seatunnel',
      pluginType: 'source',
      pluginName: 'FakeSource',
      optionRule: {
        optionalOptions: [
          {
            key: 'row.num',
            type: 'java.lang.Integer',
            defaultValue: 5,
            description: 'row count'
          }
        ],
        requiredOptions: [],
        conditionRules: [
          {
            expression: 'format = json',
            optionRule: {
              optionalOptions: [],
              requiredOptions: [
                {
                  ruleType: 'ABSOLUTELY_REQUIRED',
                  options: [
                    {
                      key: 'schema',
                      type: 'java.lang.String',
                      description: 'schema'
                    }
                  ]
                }
              ]
            }
          }
        ],
        valueConstraints: [
          {
            expression: 'row.num > 0',
            conditionTree: {
              option: {
                key: 'row.num',
                type: 'java.lang.Integer'
              },
              expectValue: 0,
              compareOperator: '>'
            }
          }
        ]
      }
    })
    vi.spyOn(operationsService, 'getHttpServiceStatus').mockResolvedValue({
      httpEnabled: true,
      httpsEnabled: false,
      configuredHttpPort: 5801,
      configuredHttpsPort: 58443,
      httpPort: 8080,
      httpsPort: 8443,
      contextPath: '',
      dynamicPortEnabled: false,
      portRange: 100,
      basicAuthEnabled: false,
      mutualTlsEnabled: false
    })

    const wrapper = mount(operations, {
      global: {
        plugins: [i18n]
      }
    })

    await flushPromises()
    expect(wrapper.text()).toContain('Connector Option Rules')
    expect(wrapper.text()).toContain('HTTP Service Status')
    expect(wrapper.text()).toContain('row.num')
    expect(wrapper.text()).toContain('Condition Rules')
    expect(wrapper.text()).toContain('format = json')
    expect(wrapper.text()).toContain('Value Constraints')
    expect(wrapper.text()).toContain('row.num > 0')
    wrapper.unmount()
  })
})
