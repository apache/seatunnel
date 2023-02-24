package org.apache.seatunnel.connectors.seatunnel.adbpostgresql.sink;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.gpdb.model.v20160503.DescribeSQLCollectorPolicyRequest;
import com.aliyuncs.gpdb.model.v20160503.DescribeSQLCollectorPolicyResponse;
import com.aliyuncs.profile.DefaultProfile;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.io.IOException;
import java.io.Serializable;

public class ADBPostgreSQLWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private IAcsClient acsClient;

    public ADBPostgreSQLWriter(String region, String accessKey, String secret) {
        this.acsClient = new IAcsClient(region, accessKey, secret);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        acsClient.send(element.toString());
    }

    @Override
    public void close() throws IOException {

    }

    private static class IAcsClient implements Serializable {

        private String region;

        private String accessKey;

        private String secret;

        private DefaultAcsClient client;

        public IAcsClient(String region, String accessKey, String secret) {
            this.region = region;
            this.accessKey = accessKey;
            this.secret = secret;
        }

        public DescribeSQLCollectorPolicyResponse send(String message) throws IOException {
            if (null == client) {
                DefaultProfile profile = DefaultProfile.getProfile(region, accessKey, secret);
                client = new DefaultAcsClient(profile);
            }
            DescribeSQLCollectorPolicyRequest request = new DescribeSQLCollectorPolicyRequest();
            request.setDBInstanceId(message);
            try {
                DescribeSQLCollectorPolicyResponse response = client.getAcsResponse(request);
                return response;
            } catch (ServerException e) {
                e.printStackTrace();
            } catch (ClientException e) {
                e.printStackTrace();
            }
            return null;
        }

    }
}
