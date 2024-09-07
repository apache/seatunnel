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
import type { GlobalThemeOverrides } from '../type'

const darkBlue = {
  common: {
    bodyColor: '#f8f8fc',

    /**************** Brand color */
    primaryColor: '#1890ff',
    primaryColorHover: '#40a9ff',
    primaryColorPressed: '#096dd9',
    primaryColorSuppl: '#1890ff',

    /**************** Function of color */
    infoColor: '#1890ff',
    successColor: '#52c41a',
    warningColor: '#faad14',
    errorColor: '#ff4d4f'
  },
  Layout: {
    headerColor: '#29366c',
    siderColor: '#29366c'
  },
  Menu: {
    itemTextColorHorizontal: '#dbdbdb',
    itemTextColor: '#dbdbdb',
    itemIconColor: '#dbdbdb',
    itemIconColorCollapsed: '#dbdbdb'
  }
} as GlobalThemeOverrides

export default darkBlue
