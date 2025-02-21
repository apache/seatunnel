package org.apache.seatunnel.connectors.seatunnel.aerospike.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class AerospikeParameters implements Serializable {
    private String host;
    private int port;
    private String namespace;
    private String set;
    private String username;
    private String password;
    private String keyField;
    private String binName;
    private DataFormatType dataFormatType;
    private Map<String, AerospikeDataType> fieldTypes = new HashMap<>();

    public void buildWithConfig(PluginConfig config) {
        // set host
        this.host = config.getString(AerospikeConfig.HOST.key());
        // set port
        this.port = config.getInt(AerospikeConfig.PORT.key());
        // set namespace
        this.namespace = config.getString(AerospikeConfig.NAMESPACE.key());
        // set set name
        this.set = config.getString(AerospikeConfig.SET.key());
        // set username
        if (config.hasPath(AerospikeConfig.USERNAME.key())) {
            this.username = config.getString(AerospikeConfig.USERNAME.key());
        }
        // set password
        if (config.hasPath(AerospikeConfig.PASSWORD.key())) {
            this.password = config.getString(AerospikeConfig.PASSWORD.key());
        }
        // set key field
        if (config.hasPath("key")) {
            this.keyField = config.getString("key");
        }

        // set bin name
        if (config.hasPath("binName")) {
            this.binName = config.getString("binName");
        }

        // set data format type
        if (config.hasPath("data_format")) {
            this.dataFormatType =
                    DataFormatType.valueOf(config.getString("data_format").toUpperCase());
        } else {
            // STRING_FORMAT
            this.dataFormatType = DataFormatType.STRING_FORMAT;
        }

        // parse schema if exists
        if (config.hasPath("schema.fields")) {
            Config schemaConfig = config.getConfig("schema.fields");
            for (Map.Entry<String, ConfigValue> entry : schemaConfig.entrySet()) {
                String fieldName = entry.getKey();
                String typeStr = entry.getValue().unwrapped().toString().toUpperCase();
                this.fieldTypes.put(fieldName, AerospikeDataType.valueOf(typeStr));
            }
        }
    }

    private AerospikeClient buildClient() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = config.get(AerospikeConfig.USERNAME);
        clientPolicy.password = config.get(AerospikeConfig.PASSWORD);
        clientPolicy.timeout = 200;
        clientPolicy.maxConnsPerNode = 300;

        return new AerospikeClient(clientPolicy, 
            config.get(AerospikeConfig.HOST),
            config.get(AerospikeConfig.PORT));
    }

    public String getKeyField() {
        return keyField;
    }
}
