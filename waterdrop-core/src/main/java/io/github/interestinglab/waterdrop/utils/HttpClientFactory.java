package io.github.interestinglab.waterdrop.utils;
 
/**
 * 
 * httpclient 工厂类
 * */
public class HttpClientFactory {
 
	private static HttpAsyncClient httpAsyncClient = new HttpAsyncClient();

	private static HttpClientFactory httpClientFactory = new HttpClientFactory();
 
	private HttpClientFactory() {
	}
 

	public static HttpClientFactory getInstance() {
 
		return httpClientFactory;
 
	}
 
	public HttpAsyncClient getHttpAsyncClientPool() {
		return httpAsyncClient;
	}
 
 
}

