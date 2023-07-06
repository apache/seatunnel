package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class KuduClientUtils {

    private static void kbAuth(
            String kerberosPrincipal, String kerberosKeytabPath, String krb5ConfPath, boolean debug)
            throws IOException {
        System.setProperty("java.security.krb5.conf", krb5ConfPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        if (debug) System.setProperty("sun.security.krb5.debug", "true");
        Configuration configuration = new Configuration();
        UserGroupInformation.setConfiguration(configuration);
        UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabPath);
    }

    public static KuduClient getKuduClient(
            String kuduMaster,
            String kerberosKeytabPath,
            String kerberosPrincipal,
            String krb5ConfPath,
            long timeOutMs) {
        KuduClient.KuduClientBuilder kuduClientBuilder =
                new KuduClient.KuduClientBuilder(kuduMaster);
        if (StringUtils.isNotBlank(kerberosPrincipal)
                && StringUtils.isNotBlank(kerberosKeytabPath)
                && StringUtils.isNotBlank(krb5ConfPath)) {
            try {
                kbAuth(kerberosPrincipal, kerberosKeytabPath, krb5ConfPath, true);
                return UserGroupInformation.getLoginUser()
                        .doAs((PrivilegedExceptionAction<KuduClient>) kuduClientBuilder::build);
            } catch (IOException | InterruptedException e) {
                throw new KuduConnectorException(KuduConnectorErrorCode.INIT_KUDU_CLIENT_FAILED, e);
            }
        } else {
            kuduClientBuilder.defaultOperationTimeoutMs(timeOutMs);
            return kuduClientBuilder.build();
        }
    }
}
