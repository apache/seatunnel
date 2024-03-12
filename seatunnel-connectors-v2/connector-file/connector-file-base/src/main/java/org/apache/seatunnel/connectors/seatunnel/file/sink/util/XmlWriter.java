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

package org.apache.seatunnel.connectors.seatunnel.file.sink.util;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.List;

/** The XmlWriter class provides functionality to write data in XML format. */
public class XmlWriter {

    private final FileSinkConfig fileSinkConfig;
    private final List<Integer> sinkColumnsIndexInRow;
    private final SeaTunnelRowType seaTunnelRowType;
    private final Document document;
    private final Element rootElement;
    private final String fieldDelimiter;
    private OutputFormat format;

    public XmlWriter(
            FileSinkConfig fileSinkConfig,
            List<Integer> sinkColumnsIndexInRow,
            SeaTunnelRowType seaTunnelRowType) {
        this.fileSinkConfig = fileSinkConfig;
        this.sinkColumnsIndexInRow = sinkColumnsIndexInRow;
        this.seaTunnelRowType = seaTunnelRowType;

        this.fieldDelimiter = fileSinkConfig.getFieldDelimiter();

        setXmlOutputFormat();
        document = DocumentHelper.createDocument();
        rootElement = document.addElement(fileSinkConfig.getXmlRootTag());
    }

    public void writeData(SeaTunnelRow seaTunnelRow) {
        Element rowElement = rootElement.addElement(fileSinkConfig.getXmlRowTag());
        boolean useAttributeFormat = fileSinkConfig.getXmlUseAttrFormat();

        sinkColumnsIndexInRow.stream()
                .map(
                        index ->
                                new AbstractMap.SimpleEntry<>(
                                        seaTunnelRowType.getFieldName(index),
                                        convertToXmlString(
                                                seaTunnelRow.getField(index),
                                                seaTunnelRowType.getFieldType(index))))
                .forEach(
                        entry -> {
                            if (useAttributeFormat) {
                                rowElement.addAttribute(entry.getKey(), entry.getValue());
                            } else {
                                rowElement.addElement(entry.getKey()).addText(entry.getValue());
                            }
                        });
    }

    private String convertToXmlString(Object fieldValue, SeaTunnelDataType<?> fieldType) {
        if (fieldValue == null) {
            return "";
        }

        switch (fieldType.getSqlType()) {
            case STRING:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
            case BOOLEAN:
                return fieldValue.toString();
            case NULL:
                return "";
            case ROW:
                Object[] fields = ((SeaTunnelRow) fieldValue).getFields();
                String[] strings = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    strings[i] =
                            convertToXmlString(
                                    fields[i], ((SeaTunnelRowType) fieldType).getFieldType(i));
                }
                return String.join(fieldDelimiter, strings);
            case MAP:
            case ARRAY:
                return JsonUtils.toJsonString(fieldValue);
            case BYTES:
                return new String((byte[]) fieldValue, StandardCharsets.UTF_8);
            default:
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        "SeaTunnel format not support this data type " + fieldType.getSqlType());
        }
    }

    public void flushAndCloseXmlWriter(OutputStream output) throws IOException {
        XMLWriter xmlWriter = new XMLWriter(output, format);
        xmlWriter.write(document);
        xmlWriter.close();
    }

    private void setXmlOutputFormat() {
        this.format = OutputFormat.createPrettyPrint();
        this.format.setNewlines(true);
        this.format.setNewLineAfterDeclaration(true);
        this.format.setSuppressDeclaration(false);
        this.format.setExpandEmptyElements(false);
        this.format.setEncoding(StandardCharsets.UTF_8.name());
        this.format.setIndent("\t");
    }
}
