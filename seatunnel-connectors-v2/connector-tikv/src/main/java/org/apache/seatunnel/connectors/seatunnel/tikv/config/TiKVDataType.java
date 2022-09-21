package org.apache.seatunnel.connectors.seatunnel.tikv.config;

import org.tikv.common.util.Pair;
import org.tikv.common.util.ScanOption;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TiKV access data way
 *
 * @author XuJiaWei
 * @since 2022-09-18 09:44
 */
public enum TiKVDataType {
    /**
     * single key  query
     */
    KEY,
    /**
     * batch key query
     */
    BATCH_GET {
        @Override
        public List<String> get(RawKVClient client, TiKVParameters tikvParameters) {
            return client.batchGet(Collections.singletonList(ByteString.copyFromUtf8(tikvParameters.getKeyField())))
                    .stream()
                    .map(value -> value.getValue().toStringUtf8())
                    .collect(Collectors.toList());
        }
    },
    /**
     * batch scan keys range query
     */
    BATCH_SCAN_KEYS {
        @Override
        public List<String> get(RawKVClient client, TiKVParameters tikvParameters) {
            return client.batchScan(Collections.singletonList(
                            ScanOption.newBuilder()
                                    .setStartKey(ByteString.copyFromUtf8(tikvParameters.getStartKey()))
                                    .setEndKey(ByteString.copyFromUtf8(tikvParameters.getEndKey()))
                                    .setLimit(tikvParameters.getLimit())
                                    .build())
                    )
                    .stream()
                    .flatMap(Collection::stream)
                    .map(value -> value.getValue().toStringUtf8())
                    .collect(Collectors.toList());
        }
    },
    /**
     * scan query
     */
    SCAN {
        @Override
        public List<String> get(RawKVClient client, TiKVParameters tikvParameters) {
            return client.scan(ByteString.copyFromUtf8(tikvParameters.getStartKey()),
                            ByteString.copyFromUtf8(tikvParameters.getEndKey()),
                            tikvParameters.getLimit())
                    .stream()
                    .map(value -> value.getValue().toStringUtf8())
                    .collect(Collectors.toList());
        }
    },
    /**
     * scan prefix query
     */
    SCAN_PREFIX {
        @Override
        public List<String> get(RawKVClient client, TiKVParameters tikvParameters) {
            return client.scanPrefix(ByteString.copyFromUtf8(tikvParameters.getKeyField()))
                    .stream()
                    .map(value -> value.getValue().toStringUtf8())
                    .collect(Collectors.toList());
        }
    },
    /**
     * batch scan query
     */
    BATCH_SCAN {
        @Override
        public List<String> get(RawKVClient client, TiKVParameters tikvParameters) {
            return client.batchScanKeys(Stream.of(ScanOption.newBuilder()
                                            .setStartKey(ByteString.copyFromUtf8(tikvParameters.getStartKey()))
                                            .setEndKey(ByteString.copyFromUtf8(tikvParameters.getEndKey()))
                                            .setLimit(tikvParameters.getLimit())
                                            .build())
                                    .map(scanOption -> Pair.create(scanOption.getStartKey(), scanOption.getEndKey()))
                                    .collect(Collectors.toList()),
                            tikvParameters.getLimit())
                    .stream()
                    .flatMap(Collection::stream)
                    .map(ByteString::toStringUtf8)
                    .collect(Collectors.toList());
        }
    };

    /**
     * default get
     *
     * @param client         RawKVClient
     * @param tikvParameters tiKV parameters
     * @return list of values
     */
    public List<String> get(RawKVClient client, TiKVParameters tikvParameters) {
        return client.get(ByteString.copyFromUtf8(tikvParameters.getKeyField()))
                .map(value -> Collections.singletonList(value.toStringUtf8()))
                .orElse(new ArrayList<>());
    }

    /**
     * set value of key
     *
     * @param client RawKVClient
     * @param key    key
     * @param value  value
     */
    public void set(RawKVClient client, String key, String value) {
        client.put(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value));
    }

    /**
     * by key get TiKVDataType
     *
     * @param dataType get key type
     * @return TiKVDataType
     */
    public TiKVDataType getDataType(String dataType) {
        return Arrays.stream(values())
                .filter(e -> this.name().equalsIgnoreCase(dataType))
                .findFirst()
                .orElse(TiKVDataType.KEY);
    }
}
