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
  managers: '管理者',
  tags: {
    title: 'Worker Tags',
    content: 'Tags',
    placeholder: '每行一个 tag，例如：zone=prod',
    update: '更新 Tags',
    clear: '清空 Tags',
    success: '节点 Tags 已更新。',
    failed: '更新节点 Tags 失败。',
    outcomeUnknown: '无法确认 Tags 是否已更新，请刷新后再重试。',
    loadFailed: '刷新节点信息失败。',
    invalid: '每行 tag 必须使用 key=value 格式。',
    contentRequired: '请至少填写一个 Tag，或使用清空 Tags。',
    duplicate: 'Tag 键不能重复。',
    select: '选择',
    local: '当前节点',
    remote: '远端节点',
    remoteHint: '请打开目标节点自己的 Web UI 更新该 Worker。',
    noWorkerSelected: '未选择 Worker',
    workerRequired: '请先选择一个 Worker。',
    confirm: '确认',
    cancelConfirm: '关闭',
    updateConfirmMessage: '确认更新所选 Worker 的 Tags？',
    clearConfirmMessage: '确认清空所选 Worker 的全部 Tags？'
  }
}
