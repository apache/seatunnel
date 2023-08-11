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
