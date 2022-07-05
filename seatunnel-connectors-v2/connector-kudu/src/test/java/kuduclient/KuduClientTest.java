package kuduclient;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KuduClientTest {
    private static final Logger logger = LoggerFactory.getLogger(KuduClientTest.class);

    /**
     * 声明全局变量 KuduClient 后期通过它来操作 kudu 表
     */
    private KuduClient kuduClient;

    /**
     * 指定 kuduMaster 地址
     */
    private String kuduMaster;

    /**
     * 指定表名
     */
    private String tableName;

    @Before
    public void init() {
        //初始化操作
        kuduMaster = "192.168.88.110:7051";
        //指定表名
        tableName = "studentlyhresultflink";

        KuduClient.KuduClientBuilder kuduClientBuilder = new
                KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultOperationTimeoutMs(1800000);

        kuduClient = kuduClientBuilder.build();


        logger.info("服务器地址#{}:客户端#{} 初始化成功...", kuduMaster, kuduClient);
    }
    //创建表
    @Test
    public void createTable() throws KuduException {
        //判断表是否存在，不存在就构建
        if (!kuduClient.tableExists(tableName)) {
            //构建创建表的schema信息 --- 就是表的字段和类型
            ArrayList<ColumnSchema> columnSchemas = new ArrayList<>();
            // 指定主键
            //.key(true)
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("age", Type.INT32).build());
            columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("sex", Type.INT32).build());
            Schema schema = new Schema(columnSchemas);
            //指定创建表的相关属性
            CreateTableOptions options = new CreateTableOptions();
            ArrayList<String> partitionList = new ArrayList<>();
            //指定kudu表的分区字段是什么
            // 问题1： 如何根据每一个分区获得每个分区的数据；
            // 问题2： 如何获得每个分区的id 关系；
            partitionList.add("id");
            //按照 id.hashcode % 分区数 = 分区号
           options.addHashPartitions(partitionList, 6);
            options.setNumReplicas(1);
            kuduClient.createTable(tableName, schema, options);
        }

    }
    /**
     * 向表加载数据
     */
    @Test
    public void insertTable() throws KuduException {
        //向表加载数据需要一个 kuduSession 对象
        KuduSession kuduSession = kuduClient.newSession();
//        kuduSession.set
        kuduSession.setTimeoutMillis(100000);
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
        for (int i = 200; i <= 230; i++) {
            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("id", i);
            row.addString("name", "zhangsan-" + i);
            row.addInt("age", 20 + i);
            row.addInt("sex", i % 2);
            // 采取手动刷新
          //  kuduSession.flush();
            //最后实现执行数据的加载操作
            kuduSession.apply(insert);
        }
    }
    /**
     * 查询表的数据结果
     */
    @Test
    public void queryDataSpilt() throws KuduException {
        //构建一个查询的扫描器
        KuduScanner.KuduScannerBuilder kuduScannerBuilder =
                kuduClient.newScannerBuilder(kuduClient.openTable(tableName));
        ArrayList<String> columnsList = new ArrayList<String>();
        columnsList.add("id");
        columnsList.add("name");
        columnsList.add("age");
        columnsList.add("sex");
        kuduScannerBuilder.setProjectedColumnNames(columnsList);
        //返回结果集
        List<ColumnSchema> columns = kuduClient.openTable(tableName).getSchema().getColumns();
        Schema schema = kuduClient.openTable(tableName).getSchema();
        for (ColumnSchema column : columns) {
            Type type = column.getType();
            String name = column.getName();
            System.out.println("列名："+name);

            System.out.println("数据类型 :"+type.getName());
        }
        int lowerBound = 88888887;
        KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("id"),
                KuduPredicate.ComparisonOp.GREATER_EQUAL,
                lowerBound);
        int upperBound = 888888888;
        KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("id"),
                KuduPredicate.ComparisonOp.LESS,
                upperBound);

        KuduScanner kuduScanner = kuduScannerBuilder.addPredicate(lowerPred)
                .addPredicate(upperPred).build();

        //遍历
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult row = rowResults.next();
                int id = row.getInt(0);
                String name = row.getString(1);
                int age = row.getInt(2);
                int sex = row.getInt(3);
                System.out.println(">>>>>>>>>>  id=" + id + " name=" + name + " age=" + age + "sex = " + sex);
            }
            System.out.println("d66666666666666");
        }
    }
    @Test
    public void queryData() throws KuduException {
        //构建一个查询的扫描器
        KuduScanner.KuduScannerBuilder kuduScannerBuilder =
                kuduClient.newScannerBuilder(kuduClient.openTable(tableName));
        ArrayList<String> columnsList = new ArrayList<String>();
        columnsList.add("id");
        columnsList.add("name");
        columnsList.add("age");
        columnsList.add("sex");
        kuduScannerBuilder.setProjectedColumnNames(columnsList);
        //返回结果集
        List<ColumnSchema> columns = kuduClient.openTable(tableName).getSchema().getColumns();
        System.out.println("主键的名字："+kuduClient.openTable(tableName).getSchema().getPrimaryKeyColumns().get(0).getName());

        for (ColumnSchema column : columns) {
            Type type = column.getType();
            String name = column.getName();
            System.out.println("列名："+name);

            System.out.println("数据类型 :"+type.getName());
        }


        KuduScanner kuduScanner = kuduScannerBuilder.build();
        ArrayList<Integer> keyList = new ArrayList<>();
        //遍历
        while (kuduScanner.hasMoreRows()) {
            RowResultIterator rowResults = kuduScanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult row = rowResults.next();
                int id = row.getInt(0);
                keyList.add(id);
                String name = row.getString(1);
                int age = row.getInt(2);
                int sex = row.getInt(3);
                System.out.println(">>>>>>>>>>  id=" + id + " name=" + name + " age=" + age + "sex = " + sex);
            }
            System.out.println("d66666666666666");
        }
        Collections.sort(keyList);
        System.out.println("打印最大的key和最小："+keyList.get(keyList.size()-1)+"最小key:"+keyList.get(0));
    }
    /**
     * 修改表的数据
     */
    @Test
    public void updateData() throws KuduException {
        //修改表的数据需要一个 kuduSession 对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
        //Update update = kuduTable.newUpdate();
        //如果 id 存在就表示修改，不存在就新增
        Upsert upsert = kuduTable.newUpsert();
        PartialRow row = upsert.getRow();
        row.addInt("id", 100);
        row.addString("name", "zhangsan-100");
        row.addInt("age", 100);
        row.addInt("sex", 0);
        //最后实现执行数据的修改操作
        kuduSession.apply(upsert);
    }
    /**
     * 删除数据
     */
    @Test
    public void deleteData() throws KuduException {
        //删除表的数据需要一个 kuduSession 对象
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        //需要使用 kuduTable 来构建 Operation 的子类实例对象
        KuduTable kuduTable = kuduClient.openTable(tableName);
        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();
        row.addInt("id", 100);
        kuduSession.apply(delete);//最后实现执行数据的删除操作
    }

    @Test
    public void dropTable() throws KuduException {
        if (kuduClient.tableExists(tableName)) {
            kuduClient.deleteTable(tableName);
        }
    }






}
