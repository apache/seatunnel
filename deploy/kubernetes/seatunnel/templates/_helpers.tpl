#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

{{/* vim: set filetype=mustache: */}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "seatunnel.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create default docker images' fullname.
*/}}
{{- define "seatunnel.image.fullname.master" -}}
{{- .Values.image.registry }}:{{ .Values.image.tag | default .Chart.AppVersion -}}
{{- end -}}
{{- define "seatunnel.image.fullname.worker" -}}
{{- .Values.image.registry }}:{{ .Values.image.tag | default .Chart.AppVersion -}}
{{- end -}}

{{/*
Create a default common labels.
*/}}
{{- define "seatunnel.common.labels" -}}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}

{{- end -}}

{{/*
Create a master labels.
*/}}
{{- define "seatunnel.master.labels" -}}
app.kubernetes.io/name: {{ include "seatunnel.fullname" . }}-master
app.kubernetes.io/component: master
{{ include "seatunnel.common.labels" . }}
{{- end -}}

{{/*
Create a worker labels.
*/}}
{{- define "seatunnel.worker.labels" -}}
app.kubernetes.io/name: {{ include "seatunnel.fullname" . }}-worker
app.kubernetes.io/component: worker
{{ include "seatunnel.common.labels" . }}
{{- end -}}
