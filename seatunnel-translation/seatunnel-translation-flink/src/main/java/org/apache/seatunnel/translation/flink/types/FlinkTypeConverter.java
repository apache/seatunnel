package org.apache.seatunnel.translation.flink.types;

import org.apache.seatunnel.api.table.type.DataType;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Convert SeaTunnel {@link DataType} to flink type.
 *
 * @param <T> target flink type
 */
public interface FlinkTypeConverter<T> {

    TypeInformation<T> convert(DataType dataType);

}
