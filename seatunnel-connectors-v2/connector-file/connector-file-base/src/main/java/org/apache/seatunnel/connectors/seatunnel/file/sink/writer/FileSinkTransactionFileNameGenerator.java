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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.common.utils.VariablesSubstitute;
import org.apache.seatunnel.connectors.seatunnel.file.config.Constant;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionFileNameGenerator;

import lombok.Data;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Data
public class FileSinkTransactionFileNameGenerator implements TransactionFileNameGenerator {
    private FileFormat fileFormat;

    private String fileNameExpression;

    private String timeFormat;

    public FileSinkTransactionFileNameGenerator(@NonNull FileFormat fileFormat,
                                                String fileNameExpression,
                                                @NonNull String timeFormat) {
        this.fileFormat = fileFormat;
        this.fileNameExpression = fileNameExpression;
        this.timeFormat = timeFormat;
    }

    @Override
    public String generateFileName(String transactionId) {
        if (StringUtils.isBlank(fileNameExpression)) {
            return transactionId + fileFormat.getSuffix();
        }
        DateTimeFormatter df = DateTimeFormatter.ofPattern(timeFormat);
        final String formattedDate = df.format(ZonedDateTime.now());

        final Map<String, String> valuesMap = new HashMap<>(4);
        valuesMap.put("uuid", UUID.randomUUID().toString());
        valuesMap.put("now", formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        valuesMap.put(Constant.TRANSACTION_EXPRESSION, transactionId);
        String substitute = VariablesSubstitute.substitute(fileNameExpression, valuesMap);
        return substitute + fileFormat.getSuffix();
    }
}
