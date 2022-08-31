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

import { axios } from '@/service/service'
import type { UserList, UserAdd, UserUpdate } from '@/service/user/types'

export function userDisable(id: number): any {
  return axios({
    url: '/user/disable',
    method: 'put',
    data: { id }
  })
}

export function userEnable(id: number): any {
  return axios({
    url: '/user/enable',
    method: 'put',
    data: { id }
  })
}

export function userList(userListReq: UserList): any {
  return axios({
    url: '/user/list',
    method: 'post',
    data: { userListReq }
  })
}

export function userDelete(id: number): any {
  return axios({
    url: '/user/user',
    method: 'delete',
    data: { id }
  })
}

export function userAdd(addReq: UserAdd): any {
  return axios({
    url: '/user/user',
    method: 'post',
    data: { addReq }
  })
}

export function userUpdate(updateReq: UserUpdate): any {
  return axios({
    url: '/user/user',
    method: 'put',
    data: { updateReq }
  })
}
