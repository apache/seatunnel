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

package org.apache.seatunnel.connectors.seatunnel.http.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.base.Strings;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpPaginationType;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import org.apache.commons.collections4.MapUtils;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Slf4j
@Setter
public class HttpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    protected final SingleSplitReaderContext context;
    protected final HttpParameter httpParameter;
    protected HttpClientProvider httpClient;
    private final DeserializationCollector deserializationCollector;
    private static final Option[] DEFAULT_OPTIONS = {
        Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
    };
    private JsonPath[] jsonPaths;
    private final JsonField jsonField;
    private final String contentJson;
    private final Configuration jsonConfiguration =
            Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS);
    private boolean noMoreElementFlag = true;
    private Optional<PageInfo> pageInfoOptional = Optional.empty();

    public HttpSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField,
            String contentJson) {
        this.context = context;
        this.httpParameter = httpParameter;
        this.deserializationCollector = new DeserializationCollector(deserializationSchema);
        this.jsonField = jsonField;
        this.contentJson = contentJson;
    }

    public HttpSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField,
            String contentJson,
            PageInfo pageInfo) {
        this.context = context;
        this.httpParameter = httpParameter;
        this.deserializationCollector = new DeserializationCollector(deserializationSchema);
        this.jsonField = jsonField;
        this.contentJson = contentJson;
        this.pageInfoOptional = Optional.ofNullable(pageInfo);
    }

    @Override
    public void open() {
        httpClient = new HttpClientProvider(httpParameter);
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    public void pollAndCollectData(Collector<SeaTunnelRow> output) throws Exception {
        HttpResponse response =
                httpClient.execute(
                        this.httpParameter.getUrl(),
                        this.httpParameter.getMethod().getMethod(),
                        this.httpParameter.getHeaders(),
                        this.httpParameter.getParams(),
                        this.httpParameter.getBody(),
                        this.httpParameter.isKeepParamsAsForm());
        if (response.getCode() >= 200 && response.getCode() <= 207) {
            String content = response.getContent();
            if (!Strings.isNullOrEmpty(content)) {
                if (this.httpParameter.isEnableMultilines()) {
                    StringReader stringReader = new StringReader(content);
                    BufferedReader bufferedReader = new BufferedReader(stringReader);
                    String lineStr;
                    while ((lineStr = bufferedReader.readLine()) != null) {
                        collect(output, lineStr);
                    }
                } else {
                    collect(output, content);
                }
            }
            log.debug(
                    "http client execute success request param:[{}], http response status code:[{}], content:[{}]",
                    httpParameter.getParams(),
                    response.getCode(),
                    response.getContent());
        } else {
            String msg =
                    String.format(
                            "http client execute exception, http response status code:[%s], content:[%s]",
                            response.getCode(), response.getContent());
            throw new HttpConnectorException(HttpConnectorErrorCode.REQUEST_FAILED, msg);
        }
    }

    @VisibleForTesting
    public void updateRequestParam(PageInfo pageInfo, boolean usePlaceholderReplacement) {
        // 1. keep page param as http param
        if (this.httpParameter.isKeepPageParamAsHttpParam()) {
            if (this.httpParameter.getParams() == null) {
                httpParameter.setParams(new HashMap<>());
            }
            // keep page cursor as http param
            if (pageInfo.getPageCursorFieldName() != null && pageInfo.getCursor() != null) {
                this.httpParameter
                        .getParams()
                        .put(pageInfo.getPageCursorFieldName(), pageInfo.getCursor());
            }

            // keep page index as http param
            if (pageInfo.getPageField() != null && pageInfo.getPageIndex() != null) {
                this.httpParameter
                        .getParams()
                        .put(pageInfo.getPageField(), pageInfo.getPageIndex().toString());
            }
            return;
        }
        Long pageValue = pageInfo.getPageIndex();
        String pageField = pageInfo.getPageField();

        // Process headers
        if (MapUtils.isNotEmpty(this.httpParameter.getHeaders())) {
            processPageMap(
                    this.httpParameter.getHeaders(),
                    pageField,
                    pageValue.toString(),
                    usePlaceholderReplacement);

            processPageMap(
                    this.httpParameter.getHeaders(),
                    pageInfo.getPageCursorFieldName(),
                    pageInfo.getCursor(),
                    usePlaceholderReplacement);
        }
        // if not set keepPageParamAsHttpParam, but page field is in params, then set page index as
        if (MapUtils.isNotEmpty(this.httpParameter.getParams())) {

            processPageMap(
                    this.httpParameter.getParams(),
                    pageField,
                    pageValue.toString(),
                    usePlaceholderReplacement);
            processPageMap(
                    this.httpParameter.getParams(),
                    pageInfo.getPageCursorFieldName(),
                    pageInfo.getCursor(),
                    usePlaceholderReplacement);
        }

        // 2. param in body
        if (!Strings.isNullOrEmpty(this.httpParameter.getBody())) {
            String processedBody =
                    processBodyString(
                            this.httpParameter.getBody(),
                            pageField,
                            pageValue,
                            usePlaceholderReplacement);

            // Process cursor if available
            if (pageInfo.getPageCursorFieldName() != null && pageInfo.getCursor() != null) {
                processedBody =
                        processBodyString(
                                processedBody,
                                pageInfo.getPageCursorFieldName(),
                                pageInfo.getCursor(),
                                usePlaceholderReplacement);
            }

            // Update the body string
            this.httpParameter.setBody(processedBody);
        }
    }

    /**
     * Replace placeholder in a string value
     *
     * @param value The string value that may contain a placeholder
     * @param pageField The page field name
     * @param pageValue The page value to replace the placeholder with
     * @return The string with placeholder replaced, or null if no placeholder found
     */
    private String replacePlaceholder(String value, String pageField, Object pageValue) {
        if (value == null || pageField == null || !value.contains("${" + pageField + "}")) {
            return value;
        }

        String placeholder = "${" + pageField + "}";
        int placeholderIndex = value.indexOf(placeholder);
        if (placeholderIndex >= 0) {
            String prefix = value.substring(0, placeholderIndex);
            String suffix = value.substring(placeholderIndex + placeholder.length());
            return prefix + pageValue + suffix;
        }
        return value;
    }

    private void processPageMap(
            Map<String, String> map,
            String pageField,
            String pageValue,
            boolean usePlaceholderReplacement) {
        if (usePlaceholderReplacement) {
            // Placeholder replacement
            Map<String, String> updatedMap = new HashMap<>();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                String replacedValue = replacePlaceholder(value, pageField, pageValue);
                if (replacedValue != null) {
                    updatedMap.put(key, replacedValue);
                }
            }
            map.putAll(updatedMap);
        } else if (pageField != null && map.containsKey(pageField)) {
            // Key-based replacement
            map.put(pageField, pageValue);
        }
    }

    private String processBodyString(
            String bodyString,
            String pageField,
            Object pageValue,
            boolean usePlaceholderReplacement) {
        if (pageField == null || pageValue == null || Strings.isNullOrEmpty(bodyString)) {
            return bodyString;
        }
        if (usePlaceholderReplacement) {
            String unquotedPlaceholder = "${" + pageField + "}";
            if (bodyString.contains(unquotedPlaceholder)) {
                bodyString = bodyString.replace(unquotedPlaceholder, pageValue.toString());
            }

            return bodyString;
        } else {
            // Key-based replacement
            Map<String, Object> bodyMap =
                    JsonUtils.parseObject(bodyString, new TypeReference<Map<String, Object>>() {});
            if (bodyMap != null) {
                processBodyMapRecursively(bodyMap, pageField, pageValue);
                return JsonUtils.toJsonString(bodyMap);
            }
            return bodyString;
        }
    }

    /**
     * Process the body map recursively for key-based parameter replacement.
     *
     * @param bodyMap The body map to process
     * @param pageField The page field name
     * @param pageValue The page value
     */
    private void processBodyMapRecursively(
            Map<String, Object> bodyMap, String pageField, Object pageValue) {
        if (bodyMap.containsKey(pageField)) {
            bodyMap.put(pageField, pageValue);
        }
        for (Map.Entry<String, Object> entry : bodyMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nestedMap = (Map<String, Object>) value;
                processBodyMapRecursively(nestedMap, pageField, pageValue);
            }
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            internalPollNext(output);
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            if (pageInfoOptional.isPresent()) {
                noMoreElementFlag = false;
                PageInfo info = pageInfoOptional.get();
                // cursor pagination
                if (HttpPaginationType.CURSOR.getCode().equals(info.getPageType())) {
                    while (!noMoreElementFlag) {
                        updateRequestParam(info, info.isUsePlaceholderReplacement());
                        pollAndCollectData(output);
                        Thread.sleep(10);
                    }

                } else {
                    // default page number pagination
                    Long pageIndex = info.getPageIndex();
                    while (!noMoreElementFlag) {
                        // increment page
                        info.setPageIndex(pageIndex);
                        // set request param
                        updateRequestParam(info, info.isUsePlaceholderReplacement());
                        pollAndCollectData(output);
                        pageIndex += 1;
                        Thread.sleep(10);
                    }
                }
            } else {
                pollAndCollectData(output);
            }
        } finally {
            if (Boundedness.BOUNDED.equals(context.getBoundedness()) && noMoreElementFlag) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded http source");
                context.signalNoMoreElement();
            } else {
                if (httpParameter.getPollIntervalMillis() > 0) {
                    Thread.sleep(httpParameter.getPollIntervalMillis());
                }
            }
        }
    }

    private void collect(Collector<SeaTunnelRow> output, String data) throws IOException {
        String contentData = data;
        if (contentJson != null) {
            contentData = JsonUtils.stringToJsonNode(getPartOfJson(data)).toString();
        }
        if (jsonField != null && contentJson == null) {
            this.initJsonPath(jsonField);
            contentData = JsonUtils.toJsonNode(parseToMap(decodeJSON(data), jsonField)).toString();
        }
        // page
        if (pageInfoOptional.isPresent()) {
            PageInfo pageInfo = pageInfoOptional.get();

            // cursor pagination
            if (HttpPaginationType.CURSOR.getCode().equals(pageInfo.getPageType())) {
                // get cursor value from response JSON with fileName
                String cursorResponseField = pageInfo.getPageCursorResponseField();
                ReadContext context = JsonPath.using(jsonConfiguration).parse(data);
                List<String> cursorList = context.read(cursorResponseField, List.class);
                String newCursor = null;
                if (cursorList != null && !cursorList.isEmpty()) {
                    newCursor = cursorList.get(0);
                }
                pageInfo.setCursor(newCursor);
                // if not present cursor, then no more data
                noMoreElementFlag = Strings.isNullOrEmpty(newCursor);
            } else {
                // if not set page pagination is default
                // Determine whether the task is completed by specifying the presence of the 'total
                // page' field
                if (pageInfo.getTotalPageSize() > 0) {
                    noMoreElementFlag = pageInfo.getPageIndex() >= pageInfo.getTotalPageSize();
                } else {
                    // no 'total page' configured
                    int readSize = JsonUtils.stringToJsonNode(contentData).size();
                    // if read size < BatchSize : read finish
                    // if read size = BatchSize : read next page.
                    noMoreElementFlag = readSize < pageInfo.getBatchSize();
                }
            }
        }
        deserializationCollector.collect(contentData.getBytes(), output);
    }

    private List<Map<String, String>> parseToMap(List<List<String>> datas, JsonField jsonField) {
        List<Map<String, String>> decodeDatas = new ArrayList<>(datas.size());
        String[] keys = jsonField.getFields().keySet().toArray(new String[] {});

        for (List<String> data : datas) {
            Map<String, String> decodeData = new HashMap<>(jsonField.getFields().size());
            final int[] index = {0};
            data.forEach(
                    field -> {
                        decodeData.put(keys[index[0]], field);
                        index[0]++;
                    });
            decodeDatas.add(decodeData);
        }

        return decodeDatas;
    }

    private List<List<String>> decodeJSON(String data) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        List<List<String>> results = new ArrayList<>(jsonPaths.length);
        for (JsonPath path : jsonPaths) {
            List<String> result = jsonReadContext.read(path);
            results.add(result);
        }
        for (int i = 1; i < results.size(); i++) {
            List<?> result0 = results.get(0);
            List<?> result = results.get(i);
            if (result0.size() != result.size()) {
                throw new HttpConnectorException(
                        HttpConnectorErrorCode.FIELD_DATA_IS_INCONSISTENT,
                        String.format(
                                "[%s](%d) and [%s](%d) the number of parsing records is inconsistent.",
                                jsonPaths[0].getPath(),
                                result0.size(),
                                jsonPaths[i].getPath(),
                                result.size()));
            }
        }

        return dataFlip(results);
    }

    private String getPartOfJson(String data) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        return JsonUtils.toJsonString(jsonReadContext.read(JsonPath.compile(contentJson)));
    }

    private List<List<String>> dataFlip(List<List<String>> results) {

        List<List<String>> datas = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            List<String> result = results.get(i);
            if (i == 0) {
                for (Object o : result) {
                    String val = o == null ? null : o.toString();
                    List<String> row = new ArrayList<>(jsonPaths.length);
                    row.add(val);
                    datas.add(row);
                }
            } else {
                for (int j = 0; j < result.size(); j++) {
                    Object o = result.get(j);
                    String val = o == null ? null : o.toString();
                    List<String> row = datas.get(j);
                    row.add(val);
                }
            }
        }
        return datas;
    }

    private void initJsonPath(JsonField jsonField) {
        jsonPaths = new JsonPath[jsonField.getFields().size()];
        for (int index = 0; index < jsonField.getFields().keySet().size(); index++) {
            jsonPaths[index] =
                    JsonPath.compile(
                            jsonField.getFields().values().toArray(new String[] {})[index]);
        }
    }
}
