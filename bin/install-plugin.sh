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

# This script downloads the connector plug-ins selected in config/plugin_config.

SEATUNNEL_HOME=$(cd "$(dirname "$0")"; cd ../; pwd)

# Connector default version is 3.0.0. You can also choose a custom version.
version=3.0.0
if [ -n "$1" ]; then
    version="$1"
fi

download_method=${SEATUNNEL_PLUGIN_DOWNLOAD_METHOD:-https}
maven_repository=${SEATUNNEL_MAVEN_REPOSITORY:-https://repo.maven.apache.org/maven2}
maven_repository=${maven_repository%/}
temporary_file=
checksum_file=
temporary_directory=

cleanup_temporary_files() {
    if [ -n "$temporary_file" ]; then
        rm -f "$temporary_file"
    fi
    if [ -n "$checksum_file" ]; then
        rm -f "$checksum_file"
    fi
    if [ -n "$temporary_directory" ]; then
        rmdir "$temporary_directory" 2>/dev/null || true
    fi
}

trap cleanup_temporary_files EXIT
trap 'exit 1' HUP INT TERM

case "$download_method" in
    https | maven)
        ;;
    *)
        echo "Error: SEATUNNEL_PLUGIN_DOWNLOAD_METHOD must be 'https' or 'maven'." >&2
        exit 1
        ;;
esac

# Maven resolves unique snapshots, dynamic versions, mirrors, credentials, and repository policies.
case "$version" in
    *-SNAPSHOT | LATEST | RELEASE | *"["* | *"]"* | *"("* | *")"* | *","* | *"+"*)
        download_method=maven
        ;;
esac

if [ "$download_method" = "https" ]; then
    case "$version" in
        *[!A-Za-z0-9._-]* | "")
            echo "Error: invalid connector release version '${version}'." >&2
            exit 1
            ;;
    esac
    case "$maven_repository" in
        https://*)
            ;;
        *)
            echo "Error: SEATUNNEL_MAVEN_REPOSITORY must use HTTPS." >&2
            exit 1
            ;;
    esac
    if ! command -v curl >/dev/null 2>&1; then
        echo "Error: curl is required to download connector plugins over HTTPS." >&2
        exit 1
    fi
    if ! command -v mktemp >/dev/null 2>&1; then
        echo "Error: mktemp is required to create secure temporary files." >&2
        exit 1
    fi
    if ! command -v sha512sum >/dev/null 2>&1 &&
        ! command -v sha1sum >/dev/null 2>&1 &&
        ! command -v shasum >/dev/null 2>&1 &&
        ! command -v openssl >/dev/null 2>&1; then
        echo "Error: a SHA-512 or SHA-1 checksum tool is required." >&2
        exit 1
    fi
fi

echo "Install SeaTunnel connector plugins, version: ${version}, method: ${download_method}"

if [ ! -d "${SEATUNNEL_HOME}/connectors" ]; then
    mkdir -p "${SEATUNNEL_HOME}/connectors"
    echo "Create connectors directory"
fi

download_https() {
    source_url=$1
    output_file=$2
    allow_not_found=${3:-false}

    http_status=$(
        curl --fail --location --retry 3 --connect-timeout 10 \
        --proto '=https' --proto-redir '=https' \
        --write-out '%{http_code}' \
        --output "$output_file" "$source_url"
    )
    curl_exit_code=$?
    if [ "$curl_exit_code" -eq 0 ]; then
        return 0
    fi

    if [ "$allow_not_found" = "true" ] && [ "$http_status" = "404" ]; then
        return 44
    fi
    return "$curl_exit_code"
}

calculate_checksum() {
    checksum_calculation_algorithm=$1
    checksum_input_file=$2

    case "$checksum_calculation_algorithm" in
        sha512)
            if command -v sha512sum >/dev/null 2>&1; then
                sha512sum "$checksum_input_file" | awk '{print $1}'
            elif command -v shasum >/dev/null 2>&1; then
                shasum -a 512 "$checksum_input_file" | awk '{print $1}'
            elif command -v openssl >/dev/null 2>&1; then
                openssl dgst -sha512 "$checksum_input_file" | awk '{print $NF}'
            else
                return 1
            fi
            ;;
        sha1)
            if command -v sha1sum >/dev/null 2>&1; then
                sha1sum "$checksum_input_file" | awk '{print $1}'
            elif command -v shasum >/dev/null 2>&1; then
                shasum -a 1 "$checksum_input_file" | awk '{print $1}'
            elif command -v openssl >/dev/null 2>&1; then
                openssl dgst -sha1 "$checksum_input_file" | awk '{print $NF}'
            else
                return 1
            fi
            ;;
    esac
}

supports_checksum_algorithm() {
    checksum_support_algorithm=$1

    case "$checksum_support_algorithm" in
        sha512)
            command -v sha512sum >/dev/null 2>&1 ||
                command -v shasum >/dev/null 2>&1 ||
                command -v openssl >/dev/null 2>&1
            ;;
        sha1)
            command -v sha1sum >/dev/null 2>&1 ||
                command -v shasum >/dev/null 2>&1 ||
                command -v openssl >/dev/null 2>&1
            ;;
    esac
}

verify_checksum() {
    checksum_artifact_file=$1
    checksum_sidecar_path=$2
    checksum_verification_algorithm=$3

    expected_checksum=$(awk 'NR == 1 {print $1}' "$checksum_sidecar_path" | tr 'A-F' 'a-f')
    actual_checksum=$(
        calculate_checksum "$checksum_verification_algorithm" "$checksum_artifact_file"
    ) || return 1
    actual_checksum=$(printf '%s' "$actual_checksum" | tr 'A-F' 'a-f')

    case "$checksum_verification_algorithm:$expected_checksum" in
        sha512:???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????? | \
        sha1:????????????????????????????????????????)
            ;;
        *)
            return 1
            ;;
    esac

    [ "$expected_checksum" = "$actual_checksum" ]
}

validate_jar() {
    artifact_file=$1
    magic_bytes=$(od -An -tx1 -N2 "$artifact_file" 2>/dev/null | tr -d '[:space:]')
    [ "$magic_bytes" = "504b" ]
}

download_release_plugin() {
    artifact_id=$1
    artifact_name="${artifact_id}-${version}.jar"
    artifact_url="${maven_repository}/org/apache/seatunnel/${artifact_id}/${version}/${artifact_name}"
    target_file="${SEATUNNEL_HOME}/connectors/${artifact_name}"
    temporary_directory=$(
        mktemp -d "${SEATUNNEL_HOME}/connectors/.install-plugin.XXXXXX"
    ) || return 1
    temporary_file="${temporary_directory}/${artifact_name}"
    checksum_file="${temporary_directory}/${artifact_name}.checksum"

    echo "Install connector: ${artifact_id}"
    download_https "$artifact_url" "$temporary_file" || return 1

    if supports_checksum_algorithm sha512; then
        download_https "${artifact_url}.sha512" "$checksum_file" true 2>/dev/null
        sha512_download_result=$?
    else
        sha512_download_result=44
    fi

    if [ "$sha512_download_result" -eq 0 ]; then
        checksum_algorithm=sha512
    elif [ "$sha512_download_result" -eq 44 ] &&
        supports_checksum_algorithm sha1 &&
        download_https "${artifact_url}.sha1" "$checksum_file"; then
        checksum_algorithm=sha1
    else
        echo "Error: no usable checksum is available for '${artifact_id}'." >&2
        return 1
    fi

    if ! verify_checksum "$temporary_file" "$checksum_file" "$checksum_algorithm"; then
        echo "Error: checksum verification failed for '${artifact_id}'." >&2
        return 1
    fi
    if ! validate_jar "$temporary_file"; then
        echo "Error: downloaded file for '${artifact_id}' is not a JAR archive." >&2
        return 1
    fi
    mv "$temporary_file" "$target_file" || return 1
    rm -f "$checksum_file"
    rmdir "$temporary_directory" || return 1
    temporary_file=
    checksum_file=
    temporary_directory=
}

download_plugin_with_maven() {
    artifact_id=$1

    echo "Install connector with Maven: ${artifact_id}"
    "${SEATUNNEL_HOME}/mvnw" dependency:get \
        -Dtransitive=false \
        -DgroupId=org.apache.seatunnel \
        -DartifactId="$artifact_id" \
        -Dversion="$version" \
        -Ddest="${SEATUNNEL_HOME}/connectors"
}

while IFS= read -r line || [ -n "$line" ]; do
    first_char=$(printf '%s' "$line" | cut -c 1)

    if [ "$first_char" != "-" ] && [ "$first_char" != "#" ] && [ -n "$first_char" ]; then
        case "$line" in
            *[!A-Za-z0-9._-]*)
                echo "Error: invalid connector artifact ID '${line}'." >&2
                exit 1
                ;;
        esac

        if [ "$download_method" = "maven" ]; then
            download_plugin_with_maven "$line" || exit 1
        elif ! download_release_plugin "$line"; then
            echo "Error: failed to download connector '${line}'." >&2
            exit 1
        fi
    fi
done < "${SEATUNNEL_HOME}/config/plugin_config"
