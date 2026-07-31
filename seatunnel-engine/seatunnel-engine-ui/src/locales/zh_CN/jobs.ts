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
  runningJobs: '运行中',
  finishedJobs: '已结束',
  operations: {
    title: '提交任务',
    textSubmit: '配置文本',
    fileSubmit: '配置文件',
    jobName: '任务名称',
    jobNamePlaceholder: '可选任务名称',
    configFormat: '格式',
    configContent: '配置',
    configPlaceholder: '粘贴 JSON、HOCON 或 SQL 任务配置',
    configFile: '配置文件',
    startWithSavepoint: '恢复启动',
    restoreJobId: '恢复任务 ID',
    restoreJobIdPlaceholder: '已有 savepoint 状态的任务 ID',
    chooseFile: '选择文件',
    reset: '重置',
    submit: '提交',
    configRequired: '任务配置不能为空。',
    fileRequired: '请选择配置文件。',
    restoreJobIdRequired: '开启恢复启动时必须填写任务 ID。',
    submitSuccess: '已提交 {job}。',
    submitFailed: '任务提交失败。'
  },
  actions: {
    view: '查看',
    stop: '停止',
    savepoint: '保存点停止',
    cancel: '取消',
    confirm: '确认',
    cancelConfirm: '关闭',
    stopConfirmMessage: '确认平滑停止 {job}？',
    savepointConfirmMessage: '确认通过保存点停止 {job}？',
    cancelConfirmMessage: '确认强制取消 {job}？',
    stopSuccess: '已发送 {job} 的停止请求。',
    stopFailed: '停止 {job} 失败。',
    savepointSuccess: '已发送 {job} 的保存点停止请求。',
    savepointFailed: '通过保存点停止 {job} 失败。',
    cancelSuccess: '已发送 {job} 的取消请求。',
    cancelFailed: '取消 {job} 失败。',
    refreshFailed: '刷新运行中作业失败。'
  }
}
