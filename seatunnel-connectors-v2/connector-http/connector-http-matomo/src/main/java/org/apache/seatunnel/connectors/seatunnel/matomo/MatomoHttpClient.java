/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.matomo;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthOption;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.ContextAwareAuthScheme;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.InvalidCredentialsException;
import org.apache.http.auth.MalformedChallengeException;
import org.apache.http.client.AuthenticationStrategy;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Lookup;
import org.apache.http.config.MessageConstraints;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.routing.RouteInfo;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.cookie.Cookie;
import org.apache.http.cookie.CookieOrigin;
import org.apache.http.cookie.CookieSpec;
import org.apache.http.cookie.CookieSpecProvider;
import org.apache.http.cookie.MalformedCookieException;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.impl.auth.BasicSchemeFactory;
import org.apache.http.impl.auth.DigestSchemeFactory;
import org.apache.http.impl.auth.KerberosScheme;
import org.apache.http.impl.auth.NTLMSchemeFactory;
import org.apache.http.impl.auth.SPNegoScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.ProxyAuthenticationStrategy;
import org.apache.http.impl.client.TargetAuthenticationStrategy;
import org.apache.http.impl.conn.DefaultManagedHttpClientConnection;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.message.BufferedHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.util.Args;
import org.apache.http.util.CharArrayBuffer;

import org.ietf.jgss.GSSException;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * Owns Matomo's HTTP diagnostic boundary without changing native cookie policies, authentication
 * algorithms or system TLS/proxy defaults. Only the small cookie/GSS protocol envelopes that log
 * credentials are quiet; native parsing, matching, cryptography and the execution chain remain.
 */
final class MatomoHttpClient {
    private static final int MAX_PROXY_RESPONSE_BYTES = 2 * 1024 * 1024;

    private MatomoHttpClient() {}

    static CloseableHttpClient create(int timeoutMs) {
        return create(timeoutMs, builder -> {});
    }

    /*
     * The package-private customization seam permits real local target/proxy authentication tests
     * without changing the JVM ProxySelector, Authenticator or production configuration surface.
     */
    static CloseableHttpClient create(int timeoutMs, Consumer<HttpClientBuilder> customize) {
        PoolingHttpClientConnectionManager connections =
                new PoolingHttpClientConnectionManager(
                        RegistryBuilder.<ConnectionSocketFactory>create()
                                .register("http", PlainConnectionSocketFactory.getSocketFactory())
                                .register(
                                        "https",
                                        SSLConnectionSocketFactory.getSystemSocketFactory())
                                .build(),
                        (route, config) ->
                                new DefaultManagedHttpClientConnection(
                                        "matomo-source",
                                        config.getBufferSize(),
                                        config.getFragmentSizeHint(),
                                        null,
                                        null,
                                        config.getMessageConstraints(),
                                        null,
                                        null,
                                        null,
                                        null));
        try {
            connections.setDefaultConnectionConfig(
                    ConnectionConfig.custom()
                            .setMessageConstraints(
                                    MessageConstraints.custom()
                                            .setMaxHeaderCount(100)
                                            .setMaxLineLength(16 * 1024)
                                            .build())
                            .build());
            if ("true".equalsIgnoreCase(System.getProperty("http.keepAlive", "true"))) {
                int max = Integer.parseInt(System.getProperty("http.maxConnections", "5"));
                connections.setDefaultMaxPerRoute(max);
                connections.setMaxTotal(2 * max);
            }
            HttpClientBuilder builder =
                    HttpClients.custom()
                            .useSystemProperties()
                            .setConnectionManager(connections)
                            .disableRedirectHandling()
                            .disableAutomaticRetries()
                            .setDefaultRequestConfig(
                                    RequestConfig.custom()
                                            .setConnectTimeout(timeoutMs)
                                            .setConnectionRequestTimeout(timeoutMs)
                                            .setSocketTimeout(timeoutMs)
                                            .build())
                            // Replace only the two logging interceptors; the builder still supplies
                            // its
                            // native CookieSpec registry/store and every other ordinary protocol
                            // handler.
                            .disableCookieManagement()
                            .addInterceptorFirst(
                                    (HttpRequestInterceptor) MatomoHttpClient::addCookies)
                            .addInterceptorFirst(
                                    (HttpResponseInterceptor) MatomoHttpClient::receiveCookies)
                            .setRequestExecutor(new QuietRequestExecutor())
                            .setTargetAuthenticationStrategy(
                                    new QuietStrategy(TargetAuthenticationStrategy.INSTANCE))
                            .setProxyAuthenticationStrategy(
                                    new QuietStrategy(ProxyAuthenticationStrategy.INSTANCE))
                            .setDefaultAuthSchemeRegistry(
                                    RegistryBuilder.<AuthSchemeProvider>create()
                                            .register(AuthSchemes.BASIC, new BasicSchemeFactory())
                                            .register(AuthSchemes.DIGEST, new DigestSchemeFactory())
                                            .register(AuthSchemes.NTLM, new NTLMSchemeFactory())
                                            .register(
                                                    AuthSchemes.SPNEGO,
                                                    context -> new QuietSpnego())
                                            .register(
                                                    AuthSchemes.KERBEROS,
                                                    context -> new QuietKerberos())
                                            .build());
            customize.accept(builder);
            return builder.build();
        } catch (RuntimeException e) {
            connections.close();
            throw e;
        }
    }

    /*
     * Mirrors only RequestAddCookies' envelope. Cookie parsing/matching/formatting and expiry
     * remain native; omitting its diagnostic calls avoids exposing accepted or expired cookies.
     */
    private static void addCookies(HttpRequest request, HttpContext context) {
        if ("CONNECT".equalsIgnoreCase(request.getRequestLine().getMethod())) {
            return;
        }
        HttpClientContext client = HttpClientContext.adapt(context);
        CookieStore store = client.getCookieStore();
        Lookup<CookieSpecProvider> registry = client.getCookieSpecRegistry();
        HttpHost target = client.getTargetHost();
        RouteInfo route = client.getHttpRoute();
        if (store == null || registry == null || target == null || route == null) {
            return;
        }
        String policy = client.getRequestConfig().getCookieSpec();
        if (policy == null) {
            policy = CookieSpecs.DEFAULT;
        }
        URI uri = null;
        if (request instanceof HttpUriRequest) {
            uri = ((HttpUriRequest) request).getURI();
        } else {
            try {
                uri = new URI(request.getRequestLine().getUri());
            } catch (URISyntaxException ignored) {
                // Native cookie handling falls back to the root path too.
            }
        }
        String path = uri == null ? null : uri.getPath();
        int port = target.getPort() < 0 ? route.getTargetHost().getPort() : target.getPort();
        CookieOrigin origin =
                new CookieOrigin(
                        target.getHostName(),
                        port >= 0 ? port : 0,
                        path == null || path.isEmpty() ? "/" : path,
                        route.isSecure());
        CookieSpecProvider provider = registry.lookup(policy);
        if (provider == null) {
            return;
        }
        CookieSpec spec = provider.create(client);
        List<Cookie> matched = new ArrayList<>();
        Date now = new Date();
        boolean expired = false;
        for (Cookie cookie : store.getCookies()) {
            if (cookie.isExpired(now)) {
                expired = true;
            } else if (spec.match(cookie, origin)) {
                matched.add(cookie);
            }
        }
        if (expired) {
            store.clearExpired(now);
        }
        if (!matched.isEmpty()) {
            for (Header header : spec.formatCookies(matched)) {
                request.addHeader(header);
            }
        }
        if (spec.getVersion() > 0) {
            Header version = spec.getVersionHeader();
            if (version != null) {
                request.addHeader(version);
            }
        }
        context.setAttribute(HttpClientContext.COOKIE_SPEC, spec);
        context.setAttribute(HttpClientContext.COOKIE_ORIGIN, origin);
    }

    /*
     * Retains ResponseProcessCookies' native validation/store and malformed-cookie ignore policy,
     * without printing header values or parser exceptions at DEBUG/WARN.
     */
    private static void receiveCookies(HttpResponse response, HttpContext context) {
        HttpClientContext client = HttpClientContext.adapt(context);
        CookieSpec spec = client.getCookieSpec();
        CookieStore store = client.getCookieStore();
        CookieOrigin origin = client.getCookieOrigin();
        if (spec == null || store == null || origin == null) {
            return;
        }
        storeCookies(response.headerIterator("Set-Cookie"), spec, origin, store);
        if (spec.getVersion() > 0) {
            storeCookies(response.headerIterator("Set-Cookie2"), spec, origin, store);
        }
    }

    private static void storeCookies(
            HeaderIterator headers, CookieSpec spec, CookieOrigin origin, CookieStore store) {
        while (headers.hasNext()) {
            try {
                for (Cookie cookie : spec.parse(headers.nextHeader(), origin)) {
                    try {
                        spec.validate(cookie, origin);
                        store.addCookie(cookie);
                    } catch (MalformedCookieException ignored) {
                        // The native handler rejects this cookie and continues the response.
                    }
                }
            } catch (MalformedCookieException ignored) {
                // Preserve the native malformed-cookie policy without retaining its diagnostics.
            }
        }
    }

    private static AuthScheme unwrap(AuthScheme scheme) {
        return scheme instanceof QuietScheme ? ((QuietScheme) scheme).delegate : scheme;
    }

    private static AuthenticationException safeAuthenticationFailure(
            AuthenticationException failure) {
        return failure instanceof InvalidCredentialsException
                ? new InvalidCredentialsException("Matomo HTTP credentials were rejected")
                : new AuthenticationException("Matomo HTTP authentication failed");
    }

    private static void shutdown(HttpClientConnection connection) {
        try {
            connection.shutdown();
        } catch (IOException ignored) {
            // A failed connection is already discarded; never retain its upstream diagnostics.
        }
    }

    /**
     * Delegates target/proxy decisions and cache callbacks, sanitizing errors before the native
     * authenticator logs them. Cache callbacks keep the original native serializable schemes.
     */
    private static final class QuietStrategy implements AuthenticationStrategy {
        private final AuthenticationStrategy delegate;

        private QuietStrategy(AuthenticationStrategy delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean isAuthenticationRequested(
                HttpHost host, HttpResponse response, HttpContext context) {
            return delegate.isAuthenticationRequested(host, response, context);
        }

        @Override
        public Map<String, Header> getChallenges(
                HttpHost host, HttpResponse response, HttpContext context)
                throws MalformedChallengeException {
            try {
                return delegate.getChallenges(host, response, context);
            } catch (MalformedChallengeException e) {
                throw new MalformedChallengeException("Matomo authentication challenge is invalid");
            }
        }

        @Override
        public Queue<AuthOption> select(
                Map<String, Header> challenges,
                HttpHost host,
                HttpResponse response,
                HttpContext context)
                throws MalformedChallengeException {
            try {
                Queue<AuthOption> selected = new ArrayDeque<>();
                for (AuthOption option : delegate.select(challenges, host, response, context)) {
                    selected.add(
                            new AuthOption(
                                    new QuietScheme(option.getAuthScheme()),
                                    option.getCredentials()));
                }
                return selected;
            } catch (MalformedChallengeException e) {
                throw new MalformedChallengeException("Matomo authentication challenge is invalid");
            }
        }

        @Override
        public void authSucceeded(HttpHost host, AuthScheme scheme, HttpContext context) {
            delegate.authSucceeded(host, unwrap(scheme), context);
        }

        @Override
        public void authFailed(HttpHost host, AuthScheme scheme, HttpContext context) {
            delegate.authFailed(host, unwrap(scheme), context);
        }
    }

    /**
     * Keeps both authentication overloads and native scheme behavior while making diagnostic
     * formatting and exception text independent of server challenges and credential values.
     */
    private static final class QuietScheme implements ContextAwareAuthScheme {
        private final AuthScheme delegate;

        private QuietScheme(AuthScheme delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getSchemeName() {
            return delegate.getSchemeName();
        }

        @Override
        public String getParameter(String name) {
            return delegate.getParameter(name);
        }

        @Override
        public String getRealm() {
            return delegate.getRealm();
        }

        @Override
        public boolean isConnectionBased() {
            return delegate.isConnectionBased();
        }

        @Override
        public boolean isComplete() {
            return delegate.isComplete();
        }

        @Override
        public void processChallenge(Header header) throws MalformedChallengeException {
            try {
                delegate.processChallenge(header);
            } catch (MalformedChallengeException e) {
                throw new MalformedChallengeException("Matomo authentication challenge is invalid");
            }
        }

        @Override
        public Header authenticate(Credentials credentials, HttpRequest request)
                throws AuthenticationException {
            try {
                return delegate.authenticate(credentials, request);
            } catch (AuthenticationException e) {
                throw safeAuthenticationFailure(e);
            }
        }

        @Override
        public Header authenticate(
                Credentials credentials, HttpRequest request, HttpContext context)
                throws AuthenticationException {
            try {
                return delegate instanceof ContextAwareAuthScheme
                        ? ((ContextAwareAuthScheme) delegate)
                                .authenticate(credentials, request, context)
                        : delegate.authenticate(credentials, request);
            } catch (AuthenticationException e) {
                throw safeAuthenticationFailure(e);
            }
        }

        @Override
        public String toString() {
            return "Matomo HTTP authentication";
        }
    }

    /**
     * Retains the native SPNEGO generator and defaults. Only the token-logging envelope is
     * replaced; the protected generator also provides a deterministic outer-protocol regression
     * seam.
     */
    static class QuietSpnego extends SPNegoScheme {
        private final GssExchange exchange = new GssExchange(this::generateToken);

        @Override
        protected void parseChallenge(CharArrayBuffer buffer, int begin, int end) {
            exchange.receive(buffer.substringTrimmed(begin, end));
        }

        @Override
        public Header authenticate(
                Credentials credentials, HttpRequest request, HttpContext context)
                throws AuthenticationException {
            return exchange.authenticate(credentials, request, context, isProxy());
        }

        @Override
        public boolean isComplete() {
            return exchange.complete();
        }
    }

    /**
     * Retains the native Kerberos generator/OID and default canonical host behavior. Its native
     * envelope also sends Negotiate headers, so the quiet envelope preserves that wire contract.
     */
    static class QuietKerberos extends KerberosScheme {
        private final GssExchange exchange = new GssExchange(this::generateToken);

        @Override
        protected void parseChallenge(CharArrayBuffer buffer, int begin, int end) {
            exchange.receive(buffer.substringTrimmed(begin, end));
        }

        @Override
        public Header authenticate(
                Credentials credentials, HttpRequest request, HttpContext context)
                throws AuthenticationException {
            return exchange.authenticate(credentials, request, context, isProxy());
        }

        @Override
        public boolean isComplete() {
            return exchange.complete();
        }
    }

    /**
     * Calls the existing scheme's protected native generator; this boundary never replaces JDK GSS
     * negotiation, credentials or cryptography with a connector implementation.
     */
    @FunctionalInterface
    private interface TokenGenerator {
        byte[] generate(byte[] challenge, String server, Credentials credentials)
                throws GSSException;
    }

    /**
     * Mirrors the four states of the pinned native GSS envelope, with no token/exception logging.
     * Terminal and repeated-challenge behavior remain the native one-attempt behavior.
     */
    private enum GssPhase {
        UNINITIATED,
        CHALLENGE_RECEIVED,
        TOKEN_GENERATED,
        FAILED
    }

    /**
     * Owns only challenge/header state and default host selection around native token generation.
     * The fields are private to one scheme instance and never become connector checkpoint state.
     */
    private static final class GssExchange {
        private final TokenGenerator generator;
        private final Base64 base64 = new Base64(0);
        // One native scheme owns this phase; a second challenge ends its single attempt.
        private GssPhase phase = GssPhase.UNINITIATED;
        // Retained only by this scheme: the incoming challenge becomes the generated token.
        private byte[] token;

        private GssExchange(TokenGenerator generator) {
            this.generator = generator;
        }

        /**
         * Retains native Base64/default-charset decoding and the one-attempt challenge transition.
         * Repeated challenges mark this scheme failed rather than restarting negotiation.
         */
        private void receive(String challenge) {
            if (phase == GssPhase.UNINITIATED) {
                token = Base64.decodeBase64(challenge.getBytes());
                phase = GssPhase.CHALLENGE_RECEIVED;
            } else {
                phase = GssPhase.FAILED;
            }
        }

        private boolean complete() {
            return phase == GssPhase.TOKEN_GENERATED || phase == GssPhase.FAILED;
        }

        /**
         * Selects the native default canonical target/proxy host without a port, then delegates
         * token generation. Both schemes retain Negotiate headers and native failure subclasses;
         * completed calls reuse this scheme's token without logging upstream values.
         */
        private Header authenticate(
                Credentials credentials, HttpRequest request, HttpContext context, boolean proxy)
                throws AuthenticationException {
            Args.notNull(request, "HTTP request");
            if (phase == GssPhase.UNINITIATED || phase == GssPhase.FAILED) {
                throw new AuthenticationException("Matomo GSS authentication is not active");
            }
            if (phase == GssPhase.CHALLENGE_RECEIVED) {
                try {
                    HttpRoute route =
                            (HttpRoute) context.getAttribute(HttpClientContext.HTTP_ROUTE);
                    if (route == null) {
                        throw new AuthenticationException("Connection route is not available");
                    }
                    HttpHost host =
                            proxy && route.getProxyHost() != null
                                    ? route.getProxyHost()
                                    : route.getTargetHost();
                    String server = host.getHostName();
                    try {
                        InetAddress address = InetAddress.getByName(server);
                        String canonical = address.getCanonicalHostName();
                        if (!address.getHostAddress().contentEquals(canonical)) {
                            server = canonical;
                        }
                    } catch (UnknownHostException ignored) {
                        // Preserve the native default canonical-host fallback and stripped port.
                    }
                    token = generator.generate(token, server, credentials);
                    phase = GssPhase.TOKEN_GENERATED;
                } catch (GSSException e) {
                    phase = GssPhase.FAILED;
                    int major = e.getMajor();
                    if (major == GSSException.DEFECTIVE_CREDENTIAL
                            || major == GSSException.CREDENTIALS_EXPIRED
                            || major == GSSException.NO_CRED) {
                        throw new InvalidCredentialsException(
                                "Matomo GSS credentials were rejected");
                    }
                    throw new AuthenticationException("Matomo GSS authentication failed");
                }
            }
            CharArrayBuffer header = new CharArrayBuffer(32);
            header.append(proxy ? "Proxy-Authorization" : "Authorization");
            header.append(": Negotiate ");
            header.append(new String(base64.encode(token)));
            return new BufferedHeader(header);
        }
    }

    /**
     * Delegates native request/continue handling, sanitizing only reason text and admitting proxy
     * refusal bytes before MainClientExec can buffer or consume them without the reader's cap.
     */
    private static final class QuietRequestExecutor extends HttpRequestExecutor {
        private QuietRequestExecutor() {
            super();
        }

        @Override
        public HttpResponse execute(
                HttpRequest request, HttpClientConnection connection, HttpContext context)
                throws IOException, HttpException {
            HttpResponse response = super.execute(request, connection, context);
            StatusLine status = response.getStatusLine();
            response.setStatusLine(
                    new BasicStatusLine(
                            status.getProtocolVersion(),
                            status.getStatusCode(),
                            "Matomo HTTP response"));
            if ("CONNECT".equalsIgnoreCase(request.getRequestLine().getMethod())
                    && status.getStatusCode() > 299
                    && response.getEntity() != null) {
                if (response.getEntity().getContentLength() > MAX_PROXY_RESPONSE_BYTES) {
                    shutdown(connection);
                    throw new ProxyBodyLimitException();
                }
                response.setEntity(new TunnelEntity(response.getEntity(), connection));
            }
            return response;
        }
    }

    /**
     * Identifies a connector-owned fixed proxy-body admission failure without retaining any
     * upstream data, cause or suppressed exception in the diagnostic.
     */
    static final class ProxyBodyLimitException extends IOException {
        private static final long serialVersionUID = 1L;

        private ProxyBodyLimitException() {
            super("Matomo proxy response exceeds the 2 MiB limit");
        }
    }

    /**
     * Bounds native proxy refusal buffering and 407 body consumption, including close/drain and
     * writeTo paths. All content views share one byte counter owned by the HTTP execution thread.
     */
    private static final class TunnelEntity extends HttpEntityWrapper {
        private final HttpClientConnection connection;
        private long consumed;
        private boolean exceeded;

        private TunnelEntity(HttpEntity entity, HttpClientConnection connection) {
            super(entity);
            this.connection = connection;
        }

        @Override
        public InputStream getContent() throws IOException {
            return new FilterInputStream(wrappedEntity.getContent()) {
                @Override
                public int read() throws IOException {
                    ensureBudget();
                    int value = in.read();
                    if (value >= 0) {
                        admit(1);
                    }
                    return value;
                }

                @Override
                public int read(byte[] bytes) throws IOException {
                    return read(bytes, 0, bytes.length);
                }

                @Override
                public int read(byte[] bytes, int offset, int length) throws IOException {
                    if (length == 0) {
                        return 0;
                    }
                    ensureBudget();
                    int permitted = (int) Math.min(length, MAX_PROXY_RESPONSE_BYTES - consumed + 1);
                    int count = in.read(bytes, offset, permitted);
                    if (count > 0) {
                        admit(count);
                    }
                    return count;
                }

                @Override
                public long skip(long count) throws IOException {
                    byte[] buffer = new byte[8192];
                    long skipped = 0;
                    while (skipped < count) {
                        int read = read(buffer, 0, (int) Math.min(buffer.length, count - skipped));
                        if (read < 0) {
                            break;
                        }
                        skipped += read;
                    }
                    return skipped;
                }

                @Override
                public void close() throws IOException {
                    IOException failure = null;
                    try {
                        if (!exceeded) {
                            byte[] buffer = new byte[8192];
                            while (read(buffer) >= 0) {
                                // Drain through this same budget, never beneath the wrapper.
                            }
                        }
                    } catch (IOException e) {
                        shutdown(connection);
                        failure = e;
                    } finally {
                        if (exceeded) {
                            shutdown(connection);
                        }
                        try {
                            in.close();
                        } catch (IOException e) {
                            if (!exceeded && failure == null) {
                                failure =
                                        new IOException(
                                                "Matomo proxy response could not be closed");
                            }
                        }
                    }
                    if (failure != null) {
                        throw failure;
                    }
                }
            };
        }

        @Override
        public void writeTo(OutputStream output) throws IOException {
            Args.notNull(output, "Output stream");
            try (InputStream input = getContent()) {
                byte[] buffer = new byte[8192];
                int count;
                while ((count = input.read(buffer)) >= 0) {
                    output.write(buffer, 0, count);
                }
            }
        }

        private void ensureBudget() throws IOException {
            if (exceeded) {
                throw new ProxyBodyLimitException();
            }
        }

        private void admit(int count) throws IOException {
            consumed += count;
            if (consumed > MAX_PROXY_RESPONSE_BYTES) {
                exceeded = true;
                shutdown(connection);
                throw new ProxyBodyLimitException();
            }
        }
    }
}
