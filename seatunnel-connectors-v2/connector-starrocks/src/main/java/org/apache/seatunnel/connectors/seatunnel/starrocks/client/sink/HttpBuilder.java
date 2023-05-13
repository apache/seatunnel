package org.apache.seatunnel.connectors.seatunnel.starrocks.client.sink;

import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

public class HttpBuilder {
    String url;
    Map<String, String> header;
    HttpEntity httpEntity;

    HttpRequestBase httpRequestBase;
    private String methodName;

    public HttpBuilder() {
        header = new HashMap<>();
    }

    public HttpBuilder setUrl(String url) {
        this.url = url;
        return this;
    }

    public HttpBuilder addBeginHeader() {
        header.put("timeout", "600");
        return this;
    }

    public HttpBuilder send() {
        setHttpPut();
        return this;
    }

    public HttpBuilder begin() {
        addBeginHeader();
        setHttpPost();
        return this;
    }

    public HttpBuilder prepare() {
        setHttpPost();
        return this;
    }

    public HttpBuilder commit() {
        setHttpPost();
        return this;
    }

    public HttpBuilder rollback() {
        setHttpPost();
        return this;
    }

    public HttpBuilder getLoadState() {
        header.put("Connection", "close");
        setHttpGet();
        return this;
    }

    public HttpBuilder setEntity(HttpEntity httpEntity) {
        this.httpEntity = httpEntity;
        return this;
    }

    public HttpBuilder setEmptyEntity() {
        try {
            this.httpEntity = new StringEntity("");
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return this;
    }

    public HttpBuilder addProperties(Properties properties) {
        properties.forEach((key, value) -> header.put(String.valueOf(key), String.valueOf(value)));
        return this;
    }

    public HttpBuilder setHttpPost() {
        methodName = HttpPost.METHOD_NAME;
        this.httpRequestBase.setConfig(
                RequestConfig.custom()
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());
        return this;
    }

    public HttpBuilder setHttpGet() {
        methodName = HttpGet.METHOD_NAME;
        this.httpRequestBase = new HttpGet();
        return this;
    }

    public HttpBuilder setHttpPut() {
        methodName = HttpPut.METHOD_NAME;
        this.httpRequestBase = new HttpPut();
        this.httpRequestBase.setConfig(
                RequestConfig.custom()
                        .setExpectContinueEnabled(true)
                        .setRedirectsEnabled(true)
                        .build());
        return this;
    }

    public HttpBuilder setDB(String db) {
        header.put("db", db);
        return this;
    }

    public HttpBuilder setTable(String table) {
        header.put("table", table);
        return this;
    }

    public HttpBuilder setLabel(String label) {
        header.put("label", label);
        return this;
    }

    public HttpBuilder baseAuth(SinkConfig sinkConfig) {
        header.put(
                HttpHeaders.AUTHORIZATION,
                getBasicAuthHeader(sinkConfig.getUsername(), sinkConfig.getPassword()));
        return this;
    }

    private String getBasicAuthHeader(String username, String password) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    public HttpRequestBase build() {
        checkNotNull(url);
        switch (methodName) {
            case HttpPost.METHOD_NAME:
                HttpPost httpPost = new HttpPost(url);
                header.forEach(httpPost::setHeader);
                httpPost.setEntity(httpEntity);
                return httpPost;
            case HttpPut.METHOD_NAME:
                HttpPut httpPut = new HttpPut(url);
                header.forEach(httpPut::setHeader);
                httpPut.setEntity(httpEntity);
                return httpPut;
            default:
                HttpGet httpGet = new HttpGet(url);
                header.forEach(httpGet::setHeader);
                return httpGet;
        }
    }
}
