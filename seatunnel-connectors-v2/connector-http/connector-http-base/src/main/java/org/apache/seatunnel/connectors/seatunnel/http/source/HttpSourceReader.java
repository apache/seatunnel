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

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import com.google.common.base.Strings;
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
                        this.httpParameter.getBody());
        if (HttpResponse.STATUS_OK == response.getCode()) {
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
            log.info(
                    "http client execute success request param:[{}], http response status code:[{}], content:[{}]",
                    httpParameter.getParams(),
                    response.getCode(),
                    response.getContent());
        } else {
            log.error(
                    "http client execute exception, http response status code:[{}], content:[{}]",
                    response.getCode(),
                    response.getContent());
        }
    }

    private void updateRequestParam(PageInfo pageInfo) {
        if (this.httpParameter.getParams() == null) {
            httpParameter.setParams(new HashMap<>());
        }
        this.httpParameter
                .getParams()
                .put(pageInfo.getPageField(), pageInfo.getPageIndex().toString());
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            if (pageInfoOptional.isPresent()) {
                noMoreElementFlag = false;
                Long pageIndex = 1L;
                while (!noMoreElementFlag) {
                    PageInfo info = pageInfoOptional.get();
                    // increment page
                    info.setPageIndex(pageIndex);
                    // set request param
                    updateRequestParam(info);
                    pollAndCollectData(output);
                    pageIndex += 1;
                }
            } else {
                pollAndCollectData(output);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
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
        if (contentJson != null) {
            data = JsonUtils.stringToJsonNode(getPartOfJson(data)).toString();
        }
        if (jsonField != null) {
            this.initJsonPath(jsonField);
            data = JsonUtils.toJsonNode(parseToMap(decodeJSON(data), jsonField)).toString();
        }
        // page increase
        if (pageInfoOptional.isPresent()) {
            // Determine whether the task is completed by specifying the presence of the 'total
            // page' field
            PageInfo pageInfo = pageInfoOptional.get();
            if (pageInfo.getTotalPageSize() > 0) {
                noMoreElementFlag = pageInfo.getPageIndex() >= pageInfo.getTotalPageSize();
            } else {
                // no 'total page' configured
                int readSize = JsonUtils.stringToJsonNode(data).size();
                // if read size < BatchSize : read finish
                // if read size = BatchSize : read next page.
                noMoreElementFlag = readSize < pageInfo.getBatchSize();
            }
        }
        deserializationCollector.collect(data.getBytes(), output);
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
