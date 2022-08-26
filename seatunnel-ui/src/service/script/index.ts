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
import type {
  ScriptList,
  ScriptPublish,
  ScriptAdd,
  ScriptContentUpdate,
  ScriptParamUpdate
} from '@/service/script/types'

export function scriptList(scriptListReq: ScriptList): any {
  return axios({
    url: '/script/list',
    method: 'post',
    data: { scriptListReq }
  })
}

export function scriptPublish(req: ScriptPublish): any {
  return axios({
    url: '/script/publish',
    method: 'put',
    data: { req }
  })
}

export function scriptDelete(id: number): any {
  return axios({
    url: '/script/script',
    method: 'delete',
    data: { id }
  })
}

export function scriptAdd(addEmptyScriptReq: ScriptAdd): any {
  return axios({
    url: '/script/script',
    method: 'post',
    data: { addEmptyScriptReq }
  })
}

export function scriptContent(id: number): any {
  return axios({
    url: '/script/scriptContent',
    method: 'get',
    data: { id }
  })
}

export function scriptContentUpdate(
  updateScriptContentReq: ScriptContentUpdate
): any {
  return axios({
    url: '/script/scriptContent',
    method: 'put',
    data: { updateScriptContentReq }
  })
}

export function scriptParam(id: number): any {
  return axios({
    url: '/script/scriptParam',
    method: 'get',
    data: { id }
  })
}

export function scriptParamUpdate(
  updateScriptParamReq: ScriptParamUpdate
): any {
  return axios({
    url: '/script/scriptParam',
    method: 'put',
    data: { updateScriptParamReq }
  })
}
