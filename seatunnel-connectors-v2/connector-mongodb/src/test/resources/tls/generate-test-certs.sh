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

# Regenerates the test-only TLS material used by the MongoDB sink tls_ca_file tests.
#
#   test-ca.jks       truststore with the test CA certificate
#   untrusted-ca.jks  truststore with an unrelated CA certificate
#   server.jks        key store with a server key and a certificate for DNS:localhost,
#                     signed by the test CA
#   wrong-host-server.jks
#                     key store with a server key and a certificate for
#                     DNS:unrelated-host.invalid only, also signed by the test CA
#
# The stores use the JKS format because the repository file checks skip *.jks as binary.
# All stores use the password "changeit". CA private keys are not kept. Requires openssl and
# keytool. Run from this directory.
set -euo pipefail
DAYS=36500
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

ca() {  # $1 = file prefix, $2 = common name
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days "$DAYS" -subj "/CN=$2" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "$WORK/$1.key" -out "$WORK/$1.crt" 2>/dev/null
}

ca test-ca "SeaTunnel MongoDB Test CA"
ca untrusted-ca "SeaTunnel MongoDB Untrusted Test CA"

server() {  # $1 = output key store, $2 = DNS name
  openssl req -newkey rsa:2048 -nodes -sha256 -subj "/CN=$2" \
    -keyout "$WORK/$1.key" -out "$WORK/$1.csr" 2>/dev/null
  printf 'basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:%s\n' "$2" > "$WORK/$1.ext"
  openssl x509 -req -sha256 -days "$DAYS" -in "$WORK/$1.csr" -CA "$WORK/test-ca.crt" \
    -CAkey "$WORK/test-ca.key" -CAcreateserial -extfile "$WORK/$1.ext" -out "$WORK/$1.crt" 2>/dev/null
  openssl pkcs12 -export -name server -inkey "$WORK/$1.key" -in "$WORK/$1.crt" \
    -certfile "$WORK/test-ca.crt" -passout pass:changeit -out "$WORK/$1.p12"
  rm -f "$1.jks"
  keytool -importkeystore -noprompt -srckeystore "$WORK/$1.p12" -srcstoretype PKCS12 \
    -srcstorepass changeit -destkeystore "$1.jks" -deststoretype JKS -deststorepass changeit \
    -destkeypass changeit >/dev/null 2>&1
}

rm -f test-ca.jks untrusted-ca.jks
for name in test-ca untrusted-ca; do
  keytool -importcert -noprompt -storetype JKS -keystore "$name.jks" -storepass changeit \
    -alias ca -file "$WORK/$name.crt" >/dev/null
done
server server localhost
server wrong-host-server unrelated-host.invalid
