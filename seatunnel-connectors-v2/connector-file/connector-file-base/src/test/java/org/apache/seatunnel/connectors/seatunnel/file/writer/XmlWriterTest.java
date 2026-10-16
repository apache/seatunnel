/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.XmlWriter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XmlWriterTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "c_date", "c_time", "c_timestamp"},
                    new SeaTunnelDataType[] {
                        BasicType.INT_TYPE,
                        LocalTimeType.LOCAL_DATE_TYPE,
                        LocalTimeType.LOCAL_TIME_TYPE,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE
                    });

    // Zero seconds: LocalDateTime/LocalTime#toString() would drop them
    private static final SeaTunnelRow ROW =
            new SeaTunnelRow(
                    new Object[] {
                        1,
                        LocalDate.of(2026, 1, 2),
                        LocalTime.of(10, 13, 0),
                        LocalDateTime.of(2026, 1, 2, 10, 13, 0)
                    });

    private FileSinkConfig fileSinkConfig;

    @BeforeEach
    public void setUp() {
        fileSinkConfig = mock(FileSinkConfig.class);
        when(fileSinkConfig.getXmlRootTag()).thenReturn("RECORDS");
        when(fileSinkConfig.getXmlRowTag()).thenReturn("RECORD");
        when(fileSinkConfig.getXmlUseAttrFormat()).thenReturn(false);
        when(fileSinkConfig.getEncoding()).thenReturn("UTF-8");
        when(fileSinkConfig.getFieldDelimiter()).thenReturn(",");
        when(fileSinkConfig.getDateFormat()).thenReturn(DateUtils.Formatter.YYYY_MM_DD);
        when(fileSinkConfig.getDatetimeFormat())
                .thenReturn(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
        when(fileSinkConfig.getTimeFormat()).thenReturn(TimeUtils.Formatter.HH_MM_SS);
    }

    @Test
    public void testDefaultFormatsKeepZeroSeconds() throws IOException {
        String xml = write();

        Assertions.assertTrue(xml.contains("<c_date>2026-01-02</c_date>"), xml);
        Assertions.assertTrue(xml.contains("<c_time>10:13:00</c_time>"), xml);
        Assertions.assertTrue(xml.contains("<c_timestamp>2026-01-02 10:13:00</c_timestamp>"), xml);
    }

    @Test
    public void testConfiguredFormatsAreApplied() throws IOException {
        when(fileSinkConfig.getDateFormat()).thenReturn(DateUtils.Formatter.YYYY_MM_DD_SPOT);
        when(fileSinkConfig.getDatetimeFormat())
                .thenReturn(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SLASH);
        when(fileSinkConfig.getTimeFormat()).thenReturn(TimeUtils.Formatter.HH_MM_SS_SSS);

        String xml = write();

        Assertions.assertTrue(xml.contains("<c_date>2026.01.02</c_date>"), xml);
        Assertions.assertTrue(xml.contains("<c_time>10:13:00.000</c_time>"), xml);
        Assertions.assertTrue(xml.contains("<c_timestamp>2026/01/02 10:13:00</c_timestamp>"), xml);
    }

    @Test
    public void testAttributeFormatUsesConfiguredFormats() throws IOException {
        when(fileSinkConfig.getXmlUseAttrFormat()).thenReturn(true);

        String xml = write();

        Assertions.assertTrue(xml.contains("c_time=\"10:13:00\""), xml);
        Assertions.assertTrue(xml.contains("c_timestamp=\"2026-01-02 10:13:00\""), xml);
    }

    @Test
    public void testNestedRowUsesConfiguredFormats() throws IOException {
        SeaTunnelRowType nestedRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "c_row"},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, ROW_TYPE});
        XmlWriter writer = new XmlWriter(fileSinkConfig, Arrays.asList(0, 1), nestedRowType);
        writer.writeData(new SeaTunnelRow(new Object[] {2, ROW}));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writer.flushAndCloseXmlWriter(output);
        String xml = new String(output.toByteArray(), StandardCharsets.UTF_8);

        // Fields of a nested row are joined with the field delimiter
        Assertions.assertTrue(
                xml.contains("<c_row>1,2026-01-02,10:13:00,2026-01-02 10:13:00</c_row>"), xml);
    }

    private String write() throws IOException {
        XmlWriter writer = new XmlWriter(fileSinkConfig, Arrays.asList(0, 1, 2, 3), ROW_TYPE);
        writer.writeData(ROW);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writer.flushAndCloseXmlWriter(output);
        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }
}
