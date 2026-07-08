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

import { defineComponent, h } from "vue"
import { describe, expect, test } from "vitest"
import { mount } from "@vue/test-utils"
import Configuration from "@/components/configuration"

const NDescriptionsStub = defineComponent({
  name: "NDescriptions",
  setup(_, { slots }) {
    return () => h("div", { "data-testid": "desc" }, slots.default?.())
  }
})

const NDescriptionsItemStub = defineComponent({
  name: "NDescriptionsItem",
  props: ["label"],
  setup(props, { slots }) {
    return () =>
      h("div", { "data-testid": "item" }, [
        h("span", { "data-testid": "label" }, String(props.label)),
        h("span", { "data-testid": "value" }, slots.default?.())
      ])
  }
})

describe("Configuration ratio formatting", () => {
  test("renders *Ratio keys as percentage", () => {
    const wrapper = mount(Configuration, {
      props: {
        data: {
          "observability.sourceReadRatio": 0.5,
          "observability.sourceIdleRatio": 0.25,
          "edge.bpRatio": 1,
          "edge.queueFillRatio": 0.1
        }
      },
      global: {
        stubs: {
          NDescriptions: NDescriptionsStub,
          NDescriptionsItem: NDescriptionsItemStub
        }
      }
    })

    const text = wrapper.text()
    expect(text).toContain("observability.sourceReadRatio")
    expect(text).toContain("50%")
    expect(text).toContain("25%")
    expect(text).toContain("100%")
    expect(text).toContain("10%")
  })
})

