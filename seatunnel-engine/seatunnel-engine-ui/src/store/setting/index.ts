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

import { defineStore } from 'pinia'
import type { SettingStore, Locales } from './types'

export const useSettingStore = defineStore({
  id: 'setting',
  state: (): SettingStore => ({
    sequenceColumn: false,
    dataUniqueValue: false,
    fillet: 15,
    requestTime: 6000,
    locales: 'en_US',
    primaryColor: '#4678B9'
  }),
  getters: {
    getSequenceColumn(): boolean {
      return this.sequenceColumn
    },
    getDataUniqueValue(): boolean {
      return this.dataUniqueValue
    },
    getFilletValue(): number {
      return this.fillet
    },
    getRequestTimeValue(): number {
      return this.requestTime
    },
    getLocales(): Locales {
      return this.locales
    }
  },
  actions: {
    setSequenceColumn(status: boolean): void {
      this.sequenceColumn = status
    },
    setDataUniqueValue(status: boolean): void {
      this.dataUniqueValue = status
    },
    setFilletValue(status: number): void {
      this.fillet = status
    },
    setRequestTimeValue(status: number): void {
      this.requestTime = status
    },
    setLocales(lang: Locales): void {
      this.locales = lang
    }
  }
})
