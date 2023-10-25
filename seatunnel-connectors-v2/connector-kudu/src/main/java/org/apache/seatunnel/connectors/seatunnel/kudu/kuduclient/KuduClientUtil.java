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

package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class KuduClientUtil {

    public static KuduClient getKuduClient(String kuduMaster, boolean useKerberos, long timeOutMs) {
        if (useKerberos) {
            try {
                return UserGroupInformation.getLoginUser()
                        .doAs(
                                (PrivilegedExceptionAction<KuduClient>)
                                        () ->
                                                new KuduClient.KuduClientBuilder(kuduMaster)
                                                        .defaultOperationTimeoutMs(timeOutMs)
                                                        .build());
            } catch (IOException | InterruptedException e) {
                throw new KuduConnectorException(KuduConnectorErrorCode.INIT_KUDU_CLIENT_FAILED, e);
            }
        } else {
            KuduClient.KuduClientBuilder kuduClientBuilder =
                    new KuduClient.KuduClientBuilder(kuduMaster);
            kuduClientBuilder.defaultOperationTimeoutMs(timeOutMs);
            return kuduClientBuilder.build();
        }
    }
}
