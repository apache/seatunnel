#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

# Client-side Session deployer. It reads the Session name from the embedded
# session.yaml in the master ConfigMap, renders the sample manifests, and calls
# kubectl. It does not start or manage the master process; that remains the
# responsibility of seatunnel-session-cluster.sh.

PRG="$0"
while [ -h "$PRG" ]; do
  ls=$(ls -ld "$PRG")
  link=$(expr "$ls" : '.*-> \(.*\)$')
  if expr "$link" : '/.*' >/dev/null; then
    PRG="$link"
  else
    PRG=$(dirname "$PRG")/"$link"
  fi
done

APP_DIR=$(cd "$(dirname "$PRG")/.." >/dev/null; pwd)
SEATUNNEL_HOME=${SEATUNNEL_HOME:-$APP_DIR}
MANIFESTS=${SEATUNNEL_SESSION_MANIFESTS:-${SEATUNNEL_HOME}/deploy/kubernetes/session}
KUBECTL=${KUBECTL:-kubectl}
NAMESPACE_OVERRIDE=""
SESSION_NAME_OVERRIDE=""
CLUSTER_NAME_OVERRIDE=""
IMAGE_OVERRIDE=""
DRY_RUN=${DRY_RUN:-0}
RENDER_ONLY=${RENDER_ONLY:-0}

usage() {
  cat <<'EOF'
Usage: seatunnel-session.sh [options]

Deploys or renders one SeaTunnel Session Cluster from the sample manifests.

Options:
  --manifests DIR    Manifest template directory
                     (default: $SEATUNNEL_HOME/deploy/kubernetes/session)
  --session-name NAME
                     Override session.name and resource-name prefix
  --cluster-name NAME
                     Override session.cluster-name
  --namespace NAME   Override kubernetes.namespace in session.yaml
  --image IMAGE      Override the master and worker image
  --dry-run          Pass --dry-run=client to kubectl
  -h, --help         Show this help

The Session name and Hazelcast cluster name are read from the embedded
session.yaml in configmap-master.yaml. Each Session in one namespace must use
a unique session.name. RENDER_ONLY=1 prints the rendered YAML without applying.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifests)
      MANIFESTS=$2
      shift 2
      ;;
    --namespace)
      NAMESPACE_OVERRIDE=$2
      shift 2
      ;;
    --session-name)
      SESSION_NAME_OVERRIDE=$2
      shift 2
      ;;
    --cluster-name)
      CLUSTER_NAME_OVERRIDE=$2
      shift 2
      ;;
    --image)
      IMAGE_OVERRIDE=$2
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

CONFIGMAP="${MANIFESTS}/configmap-master.yaml"
if [[ ! -f "${CONFIGMAP}" ]]; then
  echo "Session manifest directory does not contain configmap-master.yaml: ${MANIFESTS}" >&2
  exit 1
fi

read_session_value() {
  local key=$1
  awk -v key="${key}" '
    /^  session.yaml: \|/ { inside = 1; next }
    inside && /^  [^ ]/ { exit }
    inside && $1 == key ":" { print $2; exit }
  ' "${CONFIGMAP}" | tr -d '"'
}

SESSION_NAME=$(read_session_value "name")
CLUSTER_NAME=$(read_session_value "cluster-name")
IMAGE=$(read_session_value "image")
NAMESPACE=$(read_session_value "namespace")
if [[ -n "${SESSION_NAME_OVERRIDE}" ]]; then
  SESSION_NAME=${SESSION_NAME_OVERRIDE}
  if [[ -z "${CLUSTER_NAME_OVERRIDE}" ]]; then
    CLUSTER_NAME=${SESSION_NAME}
  fi
fi
if [[ -n "${CLUSTER_NAME_OVERRIDE}" ]]; then
  CLUSTER_NAME=${CLUSTER_NAME_OVERRIDE}
fi
if [[ -n "${IMAGE_OVERRIDE}" ]]; then
  IMAGE=${IMAGE_OVERRIDE}
fi
if [[ -n "${NAMESPACE_OVERRIDE}" ]]; then
  NAMESPACE=${NAMESPACE_OVERRIDE}
fi

if [[ -z "${SESSION_NAME}" || -z "${CLUSTER_NAME}" || -z "${IMAGE}" ]]; then
  echo "session.name, session.cluster-name, and session.worker.image are required in ${CONFIGMAP}" >&2
  exit 1
fi
NAMESPACE=${NAMESPACE:-seatunnel}
if [[ ! "${SESSION_NAME}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
  || [[ ${#SESSION_NAME} -gt 40 ]]; then
  echo "session.name must be a DNS-1123 label of at most 40 characters" >&2
  exit 1
fi
if [[ ! "${NAMESPACE}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] \
  || [[ ${#NAMESPACE} -gt 63 ]]; then
  echo "kubernetes.namespace must be a DNS-1123 label of at most 63 characters" >&2
  exit 1
fi

render() {
  local file=$1
  perl -pe \
    "s/seatunnel-session-cluster/${SESSION_NAME}/g; s/^([[:space:]]*cluster-name:[[:space:]]*).*/\${1}${CLUSTER_NAME}/; s/^([[:space:]]*clusterName:[[:space:]]*).*/\${1}${CLUSTER_NAME}/; s/namespace: seatunnel/namespace: ${NAMESPACE}/g; s/\\.seatunnel\\.svc\\.cluster\\.local/.${NAMESPACE}.svc.cluster.local/g; s|apache/seatunnel:3.0.1|${IMAGE}|g" \
    "${MANIFESTS}/${file}"
  printf '\n---\n'
}

emit() {
  perl -pe "s/name: seatunnel/name: ${NAMESPACE}/" "${MANIFESTS}/namespace.yaml"
  printf '\n---\n'
  for file in \
    root-configmap.yaml \
    rbac.yaml \
    configmap-master.yaml \
    configmap-worker.yaml \
    services.yaml \
    statefulset-master.yaml; do
    render "${file}"
  done
}

if [[ "${RENDER_ONLY}" == "1" ]]; then
  emit
  exit 0
fi

if [[ "${DRY_RUN}" == "1" ]]; then
  emit | "${KUBECTL}" apply --dry-run=client -f -
else
  emit | "${KUBECTL}" apply -f -
fi
