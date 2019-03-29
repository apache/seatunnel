package io.github.interestinglab.waterdrop.utils;
import org.apache.http.Consts;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.MalformedChallengeException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Lookup;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.impl.auth.*;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;

import javax.net.ssl.SSLContext;
import java.nio.charset.CodingErrorAction;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;

/**
 * 异步的HTTP请求对象，可设置代理
 */
public class HttpAsyncClient {

	private static int socketTimeout = 5000;// 设置等待数据超时时间5秒钟 根据业务调整

	private static int connectTimeout = 2000;// 连接超时

	private static int poolSize = 3000;// 连接池最大连接数

	private static int maxPerRoute = 1500;// 每个主机的并发最多只有1500，如果后端保有一台应用机就配置3000

	// http代理相关参数
	private String host = "baidu.com";
	private int port = 0;

	// 异步httpclient
	private CloseableHttpAsyncClient asyncHttpClient;


	public HttpAsyncClient() {
		try {
			this.asyncHttpClient = createAsyncClient(false);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public CloseableHttpAsyncClient createAsyncClient(boolean proxy)
			throws KeyManagementException, UnrecoverableKeyException,
			NoSuchAlgorithmException, KeyStoreException,
            MalformedChallengeException, IOReactorException {

		SSLContext sslcontext = SSLContexts.createDefault();

//		UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
//				username, password);

		CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//		credentialsProvider.setCredentials(AuthScope.ANY, credentials);

		// 设置协议http和https对应的处理socket链接工厂的对象
		Registry<SchemeIOSessionStrategy> sessionStrategyRegistry = RegistryBuilder
				.<SchemeIOSessionStrategy> create()
				.register("http", NoopIOSessionStrategy.INSTANCE)
				.register("https", new SSLIOSessionStrategy(sslcontext))
				.build();

		// 配置io线程
		IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
				.setIoThreadCount(Runtime.getRuntime().availableProcessors())
				.build();
		// 设置连接池大小
		ConnectingIOReactor ioReactor;
		ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
		PoolingNHttpClientConnectionManager conMgr = new PoolingNHttpClientConnectionManager(
				ioReactor, null, sessionStrategyRegistry, null);

		if (poolSize > 0) {
			conMgr.setMaxTotal(poolSize);
		}

        //每个主机最大的并发量
		if (maxPerRoute > 0) {
			conMgr.setDefaultMaxPerRoute(maxPerRoute);
		} else {
			conMgr.setDefaultMaxPerRoute(10);
		}

        //连接相关配置
		ConnectionConfig connectionConfig = ConnectionConfig.custom()
				.setMalformedInputAction(CodingErrorAction.IGNORE)
				.setUnmappableInputAction(CodingErrorAction.IGNORE)
				.setCharset(Consts.UTF_8).build();

		//请求相关配置
		RequestConfig requestConfig = RequestConfig.custom()
				.setConnectTimeout(connectTimeout)
				.setSocketTimeout(socketTimeout).build();


		Lookup<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder
				.<AuthSchemeProvider> create()
				.register(AuthSchemes.BASIC, new BasicSchemeFactory())
				.register(AuthSchemes.DIGEST, new DigestSchemeFactory())
				.register(AuthSchemes.NTLM, new NTLMSchemeFactory())
				.register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory())
				.register(AuthSchemes.KERBEROS, new KerberosSchemeFactory())
				.build();
		conMgr.setDefaultConnectionConfig(connectionConfig);

		if (proxy) {
			return HttpAsyncClients.custom().setConnectionManager(conMgr)
					.setDefaultCredentialsProvider(credentialsProvider)
					.setDefaultAuthSchemeRegistry(authSchemeRegistry)
					.setProxy(new HttpHost(host, port))
					.setDefaultCookieStore(new BasicCookieStore())
					.setDefaultRequestConfig(requestConfig).build();
		} else {
			return HttpAsyncClients.custom().setConnectionManager(conMgr)
					.setDefaultCredentialsProvider(credentialsProvider)
					.setDefaultAuthSchemeRegistry(authSchemeRegistry)
					.setDefaultCookieStore(new BasicCookieStore()).build();
		}

	}

	public CloseableHttpAsyncClient getAsyncHttpClient() {
		return asyncHttpClient;
	}

	//public CloseableHttpAsyncClient getProxyAsyncHttpClient() { return proxyAsyncHttpClient; }
}

