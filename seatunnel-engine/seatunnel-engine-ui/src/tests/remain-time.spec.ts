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

import { getRemainTime } from "@/utils/time"
import { expect, test } from "vitest"

test('calculate the countdown string for 1000 milliseconds', () => {
    expect(getRemainTime(1000)).toBe('1s')
})
test('calculate the countdown string for 1m 1s', () => {
    const time = 1000 * 60 + 1000
    expect(getRemainTime(time)).toBe('1m 1s')
})
test('calculate the countdown string for 1h 1m 1s', () => {
    const time = 1000 + 1000 * 60 + 1000 * 60 * 60
    expect(getRemainTime(time)).toBe('1h 1m 1s')
})
test('calculate the countdown string for 1d 1h 1m 1s', () => {
    const time = 1000 + 1000 * 60 + 1000 * 60 * 60 + 1000 * 60 * 60 * 24
    expect(getRemainTime(time)).toBe('1d 1h 1m 1s')
})
test('calculate the countdown string for 2d 2h 2m 2s', () => {
    const time = 1000 * 2 + 1000 * 60 * 2 + 1000 * 60 * 60 * 2 + 1000 * 60 * 60 * 24 * 2
    expect(getRemainTime(time)).toBe('2d 2h 2m 2s')
})