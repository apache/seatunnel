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

import type { RouteRecordRaw } from 'vue-router'

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    name: 'root',
    redirect: { name: 'overview' },
    component: () => import('@/layouts/main'),
    children: [
      {
        path: 'overview',
        name: 'overview',
        meta: { title: 'overview', showSide: true, activeSide: 'overview' },
        component: () => import('@/views/overview')
      },
      {
        path: 'jobs',
        name: 'jobs',
        meta: { title: 'jobs', showSide: true, activeSide: 'jobs' },
        component: () => import('@/views/jobs')
      },
      {
        path: 'jobs/:jobId',
        name: 'detail',
        meta: { title: 'detail', showSide: true, activeSide: 'jobs' },
        component: () => import('@/views/jobs/detail')
      },
      {
        path: 'managers/workers',
        name: 'managers-workers',
        meta: { title: 'workers', showSide: true, activeSide: 'workers' },
        component: () => import('@/views/managers')
      },
      {
        path: 'managers/master',
        name: 'managers-master',
        meta: { title: 'master', showSide: true, activeSide: 'master' },
        component: () => import('@/views/managers')
      }
    ]
  }
]

export default routes
