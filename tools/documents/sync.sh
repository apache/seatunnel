#!/usr/bin/env bash

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

PR_DIR=$1
PR_IMG_DIR="${PR_DIR}/docs/images"
PR_IMG_ICON_DIR="${PR_DIR}/docs/images/icons"
PR_DOC_DIR="${PR_DIR}/docs/en"
PR_SIDEBAR_PATH="${PR_DIR}/docs/sidebars.js"

WEBSITE_DIR=$2
WEBSITE_IMG_DIR="${WEBSITE_DIR}/static/image_en"
WEBSITE_DOC_DIR="${WEBSITE_DIR}/docs"
WEBSITE_ICON_DIR="${WEBSITE_DIR}/docs/images/icons"

DOCUSAURUS_DOC_SIDEBARS_FILE="${WEBSITE_DIR}/sidebars.js"

##############################################################
#
# Rebuild specific directory, if directory exists, will remove
# it before create it, otherwise create it directly. It
# supports one or more parameters.
#
# Arguments:
#
#   <path...>: One or more directories want to rebuild
#
##############################################################
function rebuild_dirs() {
    for dir in "$@"; do
        echo "  ---> Rebuild directory ${dir}"
        if [ -d "${dir}" ]; then
          rm -rf "${dir}"
        fi
        mkdir -p "${dir}"
    done
}

##############################################################
#
# Remove specific exists file. It supports one or more
# parameters.
#
# Arguments:
#
#   <file...>: One or more files want to remove
#
##############################################################
function rm_exists_files() {
    for file in "$@"; do
        echo "  ---> Remove exists ${file}"
        if [ -f "${file}" ]; then
          rm -rf "${file}"
        fi
    done
}

##############################################################
#
# Replace images path in markdown documents, the source path
# in repo `apache/seatunnel` is like `images/<name>.png`
# and we should replace it to `images_en/<name>.png`
#
# Arguments:
#
#   replace_dir: The directory to replace the img path
#
##############################################################
function replace_images_path(){
  replace_dir=$1
  for file_path in "${replace_dir}"/*; do
    if test -f "${file_path}"; then
      if [ "${file_path##*.}"x = "md"x ] || [ "${file_path##*.}"x = "mdx"x ]; then
        echo "  ---> Replace images path to /doc/image_en in ${file_path}"
        if [[ "$OSTYPE" == "darwin"* ]]; then
          sed -E -i '' "s/(\.\.\/)*images/\/image_en/g" "${file_path}"
        else
          sed -E -i "s/(\.\.\/)*images/\/image_en/g" "${file_path}"
        fi
      fi
    else
      replace_images_path "${file_path}"
    fi
  done
}

##############################################################
# Try build the updated document in the PR
##############################################################
function prepare_docs() {
    echo "===>>>: Start documents sync."

    echo "===>>>: Rebuild directory docs, static/image_en."
    rebuild_dirs "${WEBSITE_DOC_DIR}" "${WEBSITE_IMG_DIR}"

    echo "===>>>: Remove exists file sidebars.js."
    rm_exists_files "${DOCUSAURUS_DOC_SIDEBARS_FILE}"

    echo "===>>>: Rsync sidebars.js to ${DOCUSAURUS_DOC_SIDEBARS_FILE}"
    rsync -av "${PR_SIDEBAR_PATH}" "${DOCUSAURUS_DOC_SIDEBARS_FILE}"

    echo "===>>>: Rsync images to ${WEBSITE_IMG_DIR}"
    rsync -av --exclude='/icons' "${PR_IMG_DIR}"/ "${WEBSITE_IMG_DIR}"

    mkdir -p ${WEBSITE_ICON_DIR}
    echo "===>>>: Rsync icons to ${WEBSITE_ICON_DIR}"
    rsync -av "${PR_IMG_ICON_DIR}"/ "${WEBSITE_ICON_DIR}"

    echo "===>>>: Rsync documents to ${WEBSITE_DOC_DIR}"
    rsync -av "${PR_DOC_DIR}"/ "${WEBSITE_DOC_DIR}"

    echo "===>>>: Replace images path in ${WEBSITE_DOC_DIR}"
    replace_images_path "${WEBSITE_DOC_DIR}"

    echo "===>>>: End documents sync"
}

prepare_docs
