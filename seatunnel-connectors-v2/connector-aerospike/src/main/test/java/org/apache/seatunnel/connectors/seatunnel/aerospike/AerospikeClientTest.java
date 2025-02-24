package org.apache.seatunnel.connectors.seatunnel.aerospike;


import com.aerospike.client.*;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.policy.*;
import com.alibaba.fastjson.JSON;

import java.sql.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.aerospike.client.Host.parseHosts;


public class AerospikeClientTest {
    public static final String NAMESPACE = "ns1";
    public static final String SET_NAME = "testSet";
    private static final WritePolicy defaultWritePolicy = new WritePolicy();
    static {
        // 存在的值如何处理
        defaultWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;
        defaultWritePolicy.totalTimeout = 200;
        defaultWritePolicy.socketTimeout = 200;
        defaultWritePolicy.sleepBetweenRetries = 0;
        defaultWritePolicy.maxRetries = 0;
    }

    public static void main(String[] args) {
//        scanAll();
            deleteAll();
//        queryByKey("test_datatype2");
//        insertMap();
//        updateMap();
    }

    public static void insertMap() {
        // Create a list of shapes to add to the report map
//        ArrayList<String> shape = new ArrayList<String>();
//        shape.add("circle");
//        shape.add("flash");
//        shape.add("disc");

// Create the report map
        Map reportMap = new HashMap<String, Object>();
        reportMap.put("age", 1);
        reportMap.put("state", "Michigan");
//        reportMap.put("shape", shape);
        reportMap.put("LongTest",Long.valueOf(100000000));
        reportMap.put("sex", true);
        reportMap.put("ByteTest", Byte.valueOf("1"));
// Format coordinates as a GeoJSON string
//        String geoLoc = "{\"type\":\"Point\", \"coordinates\":[42.2808,83.7430]}";
        //创建一个数组写入到reportMap
        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
        stringObjectHashMap.put("key","s123");
        stringObjectHashMap.put("key2",123);
        reportMap.put("maptest",stringObjectHashMap);
        reportMap.put("array", new String[]{"1", "2", "3"});
        // Create the bins as Bin("binName", value)
        Bin occurred = new Bin("occurred", "20241211");
        Bin reported = new Bin("reported", 20241211);
//        Bin posted = new Bin("posted", 20220601);
       // reportMap defined in the section above
        Bin report = new Bin("report", reportMap);
       // geoLoc defined in the section above
//        Bin location = new Bin("location", Value.getAsGeoJSON(geoLoc));

//        MapOperation.put();

        // Write the record to Aerospike
        AerospikeClient client = connectClient();
        Key key = new Key(NAMESPACE, SET_NAME, "test_datatype2");
        client.put(defaultWritePolicy, key, report);

        // Close the connection to the server
        client.close();
    }


    public static void updateMap() {
        // Create a list of shapes to add to the report map
//        ArrayList<String> shape = new ArrayList<String>();
//        shape.add("circle2");
//        shape.add("flash1");
//        shape.add("disc3");

// Create the report map
        Map reportMap = new HashMap<String, Object>();
        reportMap.put("city", "Ann Arbor_for 需要更新的");
        reportMap.put("new city", "Ann Arbor_for 新增的");
//        reportMap.put("state", "Michigan _for update_update2");
//        reportMap.put("shape", shape);
//        reportMap.put("duration", "5 minutes 77777");
//        reportMap.put("summary", "Large flying disc flashed in the sky above the student union. Craziest thing I've ever seen!");

// Format coordinates as a GeoJSON string
//        String geoLoc = "{\"type\":\"Point\", \"coordinates\":[42.2808,83.8888]}";

        // Create the bins as Bin("binName", value)
//        Bin occurred = new Bin("occurred", 20220601);
        Bin reported = new Bin("reported", 20220601);
        Bin posted = new Bin("posted", 20220601);
        // reportMap defined in the section above
        Bin report = new Bin("report", reportMap);
        // geoLoc defined in the section above
//        Bin location = new Bin("location", Value.getAsGeoJSON(geoLoc));

        // Write the record to Aerospike
        AerospikeClient client = connectClient();
        Key key = new Key("TD_KV_DEFAULT_NAMESPACE", SET_NAME, "test_datatype2");
        client.put(defaultWritePolicy, key, reported, posted, report);



        // Close the connection to the server
        client.close();
    }

    private static void queryByKey(String keyStr) {
        System.out.println("开始查询key: " + keyStr);
        AerospikeClient client = connectClient();
        QueryPolicy policy = new QueryPolicy();
        Key key = new Key("ns1", SET_NAME, keyStr);
        Record record = client.get(policy, key);
        System.out.println(JSON.toJSONString(record.bins.get("report")));
    }

    private static void scanAll() {
        AerospikeClient client = connectClient();
        // 获取所有key列表
        ScanPolicy scanPolicy = new ScanPolicy();
        System.out.println("开始扫描所有");
        client.scanAll(scanPolicy, NAMESPACE, SET_NAME, (key, record) -> {
            System.out.println("key: " + key.toString());
            System.out.println("record: " + JSON.toJSONString(record));
        });
    }

    private static void deleteAll() {
        AerospikeClient client = connectClient();
        // 获取所有key列表
        ScanPolicy scanPolicy = new ScanPolicy();
        client.scanAll(scanPolicy, NAMESPACE, SET_NAME, (key, record) -> {
            System.out.println("key: " + key.toString());
            System.out.println("record: " + JSON.toJSONString(record));
            client.delete(null, key);
        });
    }

    private static AerospikeClient connectClient() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = "";
        clientPolicy.timeout = 200;
        clientPolicy.maxConnsPerNode = 300;
        clientPolicy.password = "";
        return new AerospikeClient(clientPolicy, parseHosts("10.57.34.77:3000", 3000));
    }

}
