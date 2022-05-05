package org.apache.seatunnel.translation.flink.types;

import org.apache.seatunnel.api.table.type.DataType;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class StringConverter implements FlinkTypeConverter<String> {

    @Override
    public TypeInformation<String> convert(DataType dataType) {

        return null;
    }
}
