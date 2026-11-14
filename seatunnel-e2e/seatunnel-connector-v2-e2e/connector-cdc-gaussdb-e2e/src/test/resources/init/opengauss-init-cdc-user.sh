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

# Sourced by the openGauss image entrypoint from /docker-entrypoint-initdb.d while the
# bootstrap server is running, after the image created its default users and before the
# final server start. The image stores passwords with sha256 only, and the PostgreSQL JDBC
# driver used by GaussDB-CDC, the JDBC sink and the test itself cannot negotiate openGauss
# sha256 authentication. Switch to md5-compatible hashing first, then create a dedicated
# replication user so its password is stored in a form the PostgreSQL driver can use.
CDC_USER="seatunnel_cdc"
CDC_PASSWORD="openGauss@123"

echo "password_encryption_type = 1" >> "$PGDATA/postgresql.conf"
# Replication sessions are not matched by the "host all" rule; grant the CDC user md5 access.
echo "host replication ${CDC_USER} 0.0.0.0/0 md5" >> "$PGDATA/pg_hba.conf"

gsql -v ON_ERROR_STOP=1 --username "$GS_USER" --password "$GS_PASSWORD" --dbname postgres \
    -c "SELECT pg_reload_conf();"

# password_encryption_type is a SIGHUP parameter; wait until new sessions observe the value so the
# user below is created with an md5-compatible password instead of a sha256-only one.
encryption_type=""
for attempt in $(seq 1 30); do
    encryption_type=$(gsql --username "$GS_USER" --password "$GS_PASSWORD" --dbname postgres \
        -t -A -c "SHOW password_encryption_type;")
    if [ "$encryption_type" = "1" ]; then
        break
    fi
    sleep 1
done
if [ "$encryption_type" != "1" ]; then
    echo "password_encryption_type is still '${encryption_type}' after reload; aborting init" >&2
    exit 1
fi

gsql -v ON_ERROR_STOP=1 --username "$GS_USER" --password "$GS_PASSWORD" --dbname postgres \
    -c "CREATE USER ${CDC_USER} WITH SYSADMIN REPLICATION PASSWORD '${CDC_PASSWORD}';"
