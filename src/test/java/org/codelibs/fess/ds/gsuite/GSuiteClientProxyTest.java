/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.gsuite;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.auth.oauth2.GoogleCredentials;

/**
 * Tests for the proxy authentication header.
 */
public class GSuiteClientProxyTest extends UnitDsTestCase {

    /** A placeholder proxy user name. Never a real account. */
    private static final String PROXY_USER = "proxy-user";

    /** A placeholder proxy password. Never a real credential. */
    private static final String PROXY_SECRET = "not-a-real-secret";

    /** The header name, lower cased because {@link MockLowLevelHttpRequest} records it that way. */
    private static final String HEADER = "proxy-authorization";

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /** A transport that also keeps the low level requests, so their headers can be asserted on. */
    private static class HeaderRecordingTransport extends MockDriveTransport {

        /** The requests that were built, in order. */
        private final List<MockLowLevelHttpRequest> requests = new ArrayList<>();

        @Override
        public LowLevelHttpRequest buildRequest(final String method, final String url) {
            final MockLowLevelHttpRequest request = (MockLowLevelHttpRequest) super.buildRequest(method, url);
            requests.add(request);
            return request;
        }

        /**
         * Returns the value a header carried on the most recent request.
         *
         * @param name The lower cased header name.
         * @return The value, or null when the header was not sent.
         */
        String lastHeaderValue(final String name) {
            return requests.get(requests.size() - 1).getFirstHeaderValue(name);
        }
    }

    /**
     * Builds a client whose credentials are stubbed out, so a test exercises the real
     * {@code createGlobalDrive} and {@code createAdminRequestFactory} without an OAuth token
     * exchange.
     * <p>
     * The local is deliberately not named {@code params}: inside the anonymous subclass body the
     * inherited field of {@link GSuiteClient} would shadow it.
     * </p>
     *
     * @param proxyParams The parameters.
     * @param mockTransport The transport.
     * @return The client.
     */
    private static GSuiteClient newClient(final DataStoreParams proxyParams, final HeaderRecordingTransport mockTransport) {
        return new GSuiteClient(proxyParams, mockTransport) {
            @Override
            protected HttpRequestInitializer createRequestInitializer(final GoogleCredentials googleCredentials) {
                return request -> {
                    // no credentials in tests
                };
            }
        };
    }

    /** The expected header value for the placeholder credentials. */
    private static String expectedHeader() {
        return "Basic " + Base64.getEncoder().encodeToString((PROXY_USER + ":" + PROXY_SECRET).getBytes(StandardCharsets.UTF_8));
    }

    /** The header is only built when a user name is present, and it is standard HTTP Basic. */
    @Test
    public void test_buildProxyAuthorization() {
        assertNull("no user name means no header", GSuiteClient.buildProxyAuthorization(null, PROXY_SECRET));
        assertNull("a blank user name means no header", GSuiteClient.buildProxyAuthorization("  ", PROXY_SECRET));
        assertEquals(expectedHeader(), GSuiteClient.buildProxyAuthorization(PROXY_USER, PROXY_SECRET));
        final String expectedNoPassword =
                "Basic " + Base64.getEncoder().encodeToString((PROXY_USER + ":").getBytes(StandardCharsets.UTF_8));
        assertEquals("a null password is treated as empty", expectedNoPassword, GSuiteClient.buildProxyAuthorization(PROXY_USER, null));
    }

    /** Every outgoing Drive request must carry the header when the credentials are configured. */
    @Test
    public void test_proxyAuthorizationHeaderIsSentOnDriveRequests() {
        final HeaderRecordingTransport mockTransport = new HeaderRecordingTransport();
        mockTransport.queueJson("{\"files\":[{\"id\":\"f1\",\"name\":\"a.txt\"}]}");
        final DataStoreParams params = GSuiteClientApiTest.newParams();
        params.put("proxy_host", "proxy.example.com");
        params.put("proxy_port", "8080");
        params.put("proxy_username", PROXY_USER);
        params.put("proxy_password", PROXY_SECRET);
        final List<String> ids = new ArrayList<>();
        try (GSuiteClient client = newClient(params, mockTransport)) {
            client.getFiles(null, GSuiteClient.ALL_DRIVES, null, "*", file -> ids.add(file.getId()));
        }
        assertEquals(1, ids.size());
        assertEquals(expectedHeader(), mockTransport.lastHeaderValue(HEADER));
    }

    /**
     * The Admin SDK listing goes through the same proxy, so it must carry the credentials too.
     * Without this a {@code crawl_target} of {@code users} fails behind an authenticating proxy
     * while the Drive calls succeed.
     */
    @Test
    public void test_proxyAuthorizationHeaderIsSentOnAdminRequests() {
        final HeaderRecordingTransport mockTransport = new HeaderRecordingTransport();
        mockTransport.queueJson("{\"users\":[{\"primaryEmail\":\"a@example.com\"}]}");
        final DataStoreParams params = GSuiteClientApiTest.newParams();
        params.put("proxy_username", PROXY_USER);
        params.put("proxy_password", PROXY_SECRET);
        try (GSuiteClient client = newClient(params, mockTransport)) {
            assertEquals(1, client.listUsers(null).size());
        }
        assertEquals(expectedHeader(), mockTransport.lastHeaderValue(HEADER));
    }

    /** Without a proxy user name the header must not be sent at all. */
    @Test
    public void test_noProxyAuthorizationHeaderWhenUnset() {
        final HeaderRecordingTransport mockTransport = new HeaderRecordingTransport();
        mockTransport.queueJson("{\"files\":[]}");
        try (GSuiteClient client = newClient(GSuiteClientApiTest.newParams(), mockTransport)) {
            client.getFiles(null, GSuiteClient.ALL_DRIVES, null, "*", file -> fail("no file is expected"));
        }
        assertNull(mockTransport.lastHeaderValue(HEADER));
    }

    /**
     * A per-user client is built from a copy of the same parameters, so it carries the proxy
     * credentials too rather than losing them at the impersonation boundary.
     */
    @Test
    public void test_forUserKeepsTheProxyCredentials() {
        final HeaderRecordingTransport mockTransport = new HeaderRecordingTransport();
        final DataStoreParams params = GSuiteClientApiTest.newParams();
        params.put("proxy_username", PROXY_USER);
        params.put("proxy_password", PROXY_SECRET);
        try (GSuiteClient client = newClient(params, mockTransport)) {
            final GSuiteClient userClient = client.forUser("u1@example.com");
            assertEquals(PROXY_USER, userClient.params.getAsString("proxy_username"));
            assertEquals(PROXY_SECRET, userClient.params.getAsString("proxy_password"));
            assertEquals(expectedHeader(), GSuiteClient.buildProxyAuthorization(userClient.params.getAsString("proxy_username"),
                    userClient.params.getAsString("proxy_password")));
        }
        assertTrue("no request is expected", mockTransport.getRequestedUrls().isEmpty());
    }
}
