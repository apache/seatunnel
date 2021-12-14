/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.utils;

import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;

/**
 * 
 * http client 业务逻辑处理类
 * */
public class HttpClientService {
 
	private static Logger logger = LoggerFactory.getLogger(HttpClientService.class);
 
	public static void execAsyncPost(String baseUrl,String postBody, FutureCallback callback)
			throws Exception {
 
		if (baseUrl == null) {
			logger.warn("we don't have base url, check config");
			throw new ConfigException("missing base url");
		}
 
		HttpPost httpPostMethod = new HttpPost(baseUrl);
		CloseableHttpAsyncClient hc = null;
 
		try {
			hc = HttpClientFactory.getInstance().getHttpAsyncClientPool()
					.getAsyncHttpClient();
 
			hc.start();
 
			HttpClientContext localContext = HttpClientContext.create();
			BasicCookieStore cookieStore = new BasicCookieStore();
 
			httpPostMethod = new HttpPost(baseUrl);
 
			if (null != postBody) {
				//logger.info("ExeAsyncReq post url={} and postBody={}", baseUrl,postBody);
				StringEntity entity = new StringEntity(postBody,Charset.forName("UTF-8"));
				logger.debug("ExeAsyncReq post url={} and postBody={} , postEntity={}", baseUrl,postBody,entity.toString());
				httpPostMethod.setEntity(entity);
			}
 
 
			localContext.setAttribute(HttpClientContext.COOKIE_STORE,
					cookieStore);
 
			hc.execute(httpPostMethod, localContext, callback);
 
		} catch (Exception e) {
			e.printStackTrace();
		}
 
	}
	
	
	public static void execAsyncGet(String baseUrl, List<BasicNameValuePair> urlParams, FutureCallback callback)
			throws Exception {
 
		if (baseUrl == null) {
			logger.warn("we don't have base url, check config");
			throw new ConfigException("missing base url");
		}
 
		HttpRequestBase httpMethod = new HttpGet(baseUrl);
		
		CloseableHttpAsyncClient hc = null;
 
		try {
			hc = HttpClientFactory.getInstance().getHttpAsyncClientPool()
					.getAsyncHttpClient();
 
			hc.start();
 
			HttpClientContext localContext = HttpClientContext.create();
			BasicCookieStore cookieStore = new BasicCookieStore();

			if (null != urlParams) {

				String getUrl = EntityUtils.toString(new UrlEncodedFormEntity(urlParams));

				httpMethod.setURI(new URI(httpMethod.getURI().toString() + "?" + getUrl));
			}


			localContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

			hc.execute(httpMethod, localContext, callback);

		} catch (Exception e) {
			e.printStackTrace();
		}
 
	}

	public static class ConfigException extends  Exception{

		public ConfigException(String message) {
			super(message);
		}
	}
 
}
