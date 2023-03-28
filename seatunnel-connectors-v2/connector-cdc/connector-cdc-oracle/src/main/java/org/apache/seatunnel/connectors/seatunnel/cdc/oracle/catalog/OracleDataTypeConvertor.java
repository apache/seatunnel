package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.catalog;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import com.google.auto.service.AutoService;
import oracle.jdbc.OracleType;

import java.util.Map;

@AutoService(DataTypeConvertor.class)
public class OracleDataTypeConvertor implements DataTypeConvertor<OracleType> {

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        OracleDataType oracleDataType = OracleDataTypeParser.parse(connectorDataType);
        return toSeaTunnelType(
                oracleDataType.getOracleType(), oracleDataType.getDataTypeProperties());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            OracleType oracleType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        switch (oracleType) {
            case FLOAT:
                // The float type will be converted to DecimalType(10, -127),
                // which will lose precision in the spark engine
                return new DecimalType(38, 18);
            case NUMBER:
                Integer precision = OracleDataType.getPrecision(dataTypeProperties);
                Integer scale = OracleDataType.getScale(dataTypeProperties);
                if (scale == 0) {
                    if (precision <= 9) {
                        return BasicType.INT_TYPE;
                    }
                    if (precision <= 18) {
                        return BasicType.LONG_TYPE;
                    }
                }
                return new DecimalType(precision, scale);
            case BINARY_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case BINARY_FLOAT:
                return BasicType.FLOAT_TYPE;
            case CHAR:
            case NCHAR:
            case VARCHAR2:
            case LONG:
            case ROWID:
            case NCLOB:
            case CLOB:
                return BasicType.STRING_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case BLOB:
            case RAW:
            case LONG_RAW:
            case BFILE:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            default:
                throw new JdbcConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format("Doesn't support ORACLE type '%s'", oracleType));
        }
    }

    @Override
    public OracleType toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getIdentity() {
        return "Oracle";
    }
}
