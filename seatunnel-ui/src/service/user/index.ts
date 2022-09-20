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
import type { UserList, UserLogin, UserDetail } from '@/service/user/types'

export function userList(params: UserList): any {
  return axios({
    url: '/user',
    method: 'get',
    params
  })
}

export function userAdd(data: UserDetail): any {
  return axios({
    url: '/user',
    method: 'post',
    data
  })
}

export function userLogin(data: UserLogin): any {
  return axios({
    url: '/user/login',
    method: 'post',
    data
  })
}

export function userLogout(): any {
  return axios({
    url: '/user/logout',
    method: 'patch'
  })
}

export function userDelete(userId: number): any {
  return axios({
    url: `/user/${userId}`,
    method: 'delete'
  })
}

export function userUpdate(userId: number, data: UserDetail): any {
  return axios({
    url: `/user/${userId}`,
    method: 'put',
    data
  })
}

export function userDisable(userId: number): any {
  return axios({
    url: `/user/${userId}/disable`,
    method: 'put'
  })
}

export function userEnable(userId: number): any {
  return axios({
    url: `/user/${userId}/enable`,
    method: 'patch'
  })
}
