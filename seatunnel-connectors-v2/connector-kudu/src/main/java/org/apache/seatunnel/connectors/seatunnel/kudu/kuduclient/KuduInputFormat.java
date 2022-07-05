package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.seatunnel.api.table.type.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KuduInputFormat implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(KuduInputFormat.class);

    public KuduInputFormat(String kuduMaster,String tableName,String columnsList){
        this.kuduMaster=kuduMaster;
        this.columnsList=Arrays.asList(columnsList.split(","));
        this.tableName=tableName;
    }
    /**
     * Declare the global variable KuduClient and use it to manipulate the Kudu table
     */
    public KuduClient kuduClient;

    /**
     * Specify kuduMaster address
     */
    public   String kuduMaster;
    public   List<String> columnsList;
    public Schema schema;
    public String keyColumn;

    /**
     * Specifies the name of the table
     */
    public   String tableName;
    public List<ColumnSchema>  getColumnsSchemas(){
        KuduClient.KuduClientBuilder kuduClientBuilder = new
                KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultOperationTimeoutMs(1800000);

        kuduClient = kuduClientBuilder.build();
        List<ColumnSchema> columns = null;
        try {
            schema = kuduClient.openTable(tableName).getSchema();
            keyColumn = schema.getPrimaryKeyColumns().get(0).getName();
            columns =schema.getColumns();
        } catch (KuduException e) {
            e.printStackTrace();
        }
        return columns;
    }

    public static SeaTunnelRow getSeaTunnelRowData(RowResult rs, SeaTunnelRowType typeInfo) throws SQLException {

        List<Object> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = typeInfo.getFieldTypes();

        for (int i = 0; i < seaTunnelDataTypes.length; i++) {
            Object seatunnelField;
            SeaTunnelDataType<?> seaTunnelDataType = seaTunnelDataTypes[i];

                if (null == rs.getObject(i)) {
                    seatunnelField = null;
                } else if (BasicType.BOOLEAN_TYPE.equals(seaTunnelDataType)) {
                    seatunnelField = rs.getBoolean(i);
                } else if (BasicType.BYTE_TYPE.equals(seaTunnelDataType)) {
                    seatunnelField = rs.getByte(i);
                } else if (BasicType.SHORT_TYPE.equals(seaTunnelDataType)) {
                    seatunnelField = rs.getShort(i);
                } else if (BasicType.INT_TYPE.equals(seaTunnelDataType)) {
                    seatunnelField = rs.getInt(i);
                } else if (BasicType.LONG_TYPE.equals(seaTunnelDataType)) {
                    seatunnelField = rs.getLong(i);
                } else if (seaTunnelDataType instanceof DecimalType) {
                    Object value = rs.getObject(i);
                    seatunnelField = value instanceof BigInteger ?
                            new BigDecimal((BigInteger) value, 0)
                            : value;
                } else if (BasicType.FLOAT_TYPE.equals(seaTunnelDataType)) {
                    seatunnelField = rs.getFloat(i);
                } else if (BasicType.DOUBLE_TYPE.equals(seaTunnelDataType)) {
                    seatunnelField = rs.getDouble(i);
                } else if (BasicType.STRING_TYPE.equals(seaTunnelDataType)) {
                    seatunnelField = rs.getString(i);
                } else {
                    throw new IllegalStateException("Unexpected value: " + seaTunnelDataType);
                }
                fields.add(seatunnelField);
            }

        return new SeaTunnelRow(fields.toArray());
    }
    // get SeaTunnelRowType
    public SeaTunnelRowType getSeaTunnelRowType(List<ColumnSchema> columnSchemaList) {

        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {

            for (int i = 0; i < columnSchemaList.size(); i++) {
                fieldNames.add(columnSchemaList.get(i).getName());
                seaTunnelDataTypes.add(KuduTypeMapper.mapping(columnSchemaList, i));
            }
        } catch (Exception e) {
            logger .warn("get row type info exception", e);
        }
        return new SeaTunnelRowType(fieldNames.toArray(new String[fieldNames.size()]), seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[seaTunnelDataTypes.size()]));
    }

    public void openInputFormat() {

        KuduClient.KuduClientBuilder kuduClientBuilder = new
                KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultOperationTimeoutMs(1800000);

        kuduClient = kuduClientBuilder.build();

        logger.info("服务器地址#{}:客户端#{} 初始化成功...", kuduMaster, kuduClient);
    }


    /**
     *
     * @param lowerBound The beginning of each slice
     * @param upperBound  End of each slice
     * @return  Get the kuduScanner object for each slice
     */
    public KuduScanner getKuduBuildSplit(int lowerBound,int upperBound){
        KuduScanner kuduScanner = null;
        try {
            KuduScanner.KuduScannerBuilder kuduScannerBuilder =
                    kuduClient.newScannerBuilder(kuduClient.openTable(tableName));

            kuduScannerBuilder.setProjectedColumnNames(columnsList);

            KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
                    schema.getColumn(""+keyColumn),
                    KuduPredicate.ComparisonOp.GREATER_EQUAL,
                    lowerBound);

            KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(
                    schema.getColumn(""+keyColumn),
                    KuduPredicate.ComparisonOp.LESS,
                    upperBound);

             kuduScanner = kuduScannerBuilder.addPredicate(lowerPred)
                    .addPredicate(upperPred).build();
        } catch (KuduException e) {
            e.printStackTrace();
        }
        return kuduScanner;
    }

}
