package org.apache.seatunnel.api.table.type;

/**
 * Used to convert the original type to the SeaTunnel {@link DataType}.
 */
public interface Converter<T extends DataType> {

    // todo: need to confirm
    T convert(String originValue);
}
