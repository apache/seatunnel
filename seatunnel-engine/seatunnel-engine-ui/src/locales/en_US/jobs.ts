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
  runningJobs: 'Running Jobs',
  finishedJobs: 'Finished Jobs',
  operations: {
    title: 'Submit Job',
    textSubmit: 'Config Text',
    fileSubmit: 'Config File',
    jobName: 'Job Name',
    jobNamePlaceholder: 'Optional job name',
    configFormat: 'Format',
    configContent: 'Config',
    configPlaceholder: 'Paste JSON, HOCON, or SQL job config',
    configFile: 'Config File',
    startWithSavepoint: 'Restore',
    restoreJobId: 'Restore Job ID',
    restoreJobIdPlaceholder: 'Existing job ID with savepoint state',
    chooseFile: 'Choose File',
    reset: 'Reset',
    submit: 'Submit',
    configRequired: 'Job config is required.',
    fileRequired: 'Please choose a config file.',
    restoreJobIdRequired: 'Restore job ID is required when restore is enabled.',
    submitSuccess: 'Submitted {job}.',
    submitFailed: 'Failed to submit job.'
  },
  actions: {
    view: 'View',
    stop: 'Stop',
    savepoint: 'Savepoint',
    cancel: 'Cancel',
    confirm: 'Confirm',
    cancelConfirm: 'Dismiss',
    stopConfirmMessage: 'Stop {job} gracefully?',
    savepointConfirmMessage: 'Stop {job} with savepoint?',
    cancelConfirmMessage: 'Force cancel {job}?',
    stopSuccess: 'Stop request sent for {job}.',
    stopFailed: 'Failed to stop {job}.',
    savepointSuccess: 'Savepoint stop request sent for {job}.',
    savepointFailed: 'Failed to stop {job} with savepoint.',
    cancelSuccess: 'Cancel request sent for {job}.',
    cancelFailed: 'Failed to cancel {job}.',
    refreshFailed: 'Failed to refresh running jobs.'
  }
}
