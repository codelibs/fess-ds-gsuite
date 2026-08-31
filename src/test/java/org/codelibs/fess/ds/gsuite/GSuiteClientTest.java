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

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Test;

import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.ds.gsuite.UnitDsTestCase;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

public class GSuiteClientTest extends UnitDsTestCase {

    static final String VALID_PRIVATE_KEY =
            "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDOnYxwfP57dOGx\\nxwY7f3yB17RewqW4bRmTEg/uetU9yA+TgYqNG7r/tPr3gzAQN39Q+Hu5dH2J6JEm\\n9kGLh3XTFuQUNZlTV0jBV7/l0tsfaemHNbGnt1GIJITnxgVWAtxnZb6O3G9p6W9p\\nv2v4o5Lz1NBMmyBTAQU/onAGArj6W26GMAeM5ZNNbscugV22AKNtWwN3xHLnd3eJ\\nKzi2CbwrL/XfUGFEyGD6vVUIWmAre2zq1OxcpcWGKTn2IPefrlIceERwCJvZXxQ5\\nQyk0adTMJq5Qfb3lF1cgndG2P3F+Mcxoo2+RrtJUwBQSJC6Aq1D26fps4CJzn1YC\\n6WKiG7xFAgMBAAECggEACYeZPmf5edrAfSljl3NwG/IFwvgh2iGIFDE5VGPMeZLE\\nayaGrC76/0fK6ocdvKW+pM6tMDbYAngcV8p0Z/nZvKB56Re2yHIGbEp+kpxY2HhT\\nWdXnaYeqRkf+7EzFGrw7i7ZU5XRz3BP0/FDkqz1qLf5jFCF0ineJ1S9KEPDntL5V\\nGRyj4uAjsG0aD0R6QpYf2+7rslyPkTgruiXPqkpHZ5cwHROMf8YjZskQPUAsNr+Q\\nr3ovH6rEXEpKo4Jps2JxJniD2V3PsHZ9QX5n4a6jWDfZEy59f4w4UVmQZLM/Ma5/\\nVwdug8BrcHS1MySir1j6s3bPXV2Nm8u8PzTr+F/jsQKBgQDzWHUA6V9ekQ2Rwqul\\ncJ91jehZMHg17NQud+/bn2kLiv1u87Td0+asN8ywozL1LBquMucl7nMw/gGFD2ni\\nlZM3c7dZlNkCkm1yPhAniz+KLP7l5fBhZ/dN/44ZGYP/4qwo0KfELRRjnNqo+9Uc\\nL2RLCEimZaH2fXYdX4xzyh+WUQKBgQDZXB8BAWbzEtMekNIN+RUsbZ7c5l5DI0T3\\nGglZJESHj8fXYCvHQEy4OBeI0p/qS4NDISBOVHXzGxTOLV1FScgPwzoKeiig0AW/\\n4O5q0Yo+heaxsgE5ghUO8hw2seSiP1c5+01/om7tbT/rMe6felFJsJfQoul7mutq\\nD2+Mz1DltQKBgQCm2wt3MY3kIN/GB058pQmhqEkeBr8WcqpWpoR/+gEkGgyGbHKi\\n++4aPjSLFYwWUkSFF4ApISQ4/qH6I8R9ygPkrOKWeRqHyfFjuSyIgNFzpECvUIgP\\nsiL/h3Bew4EgDsPvRIsUV7i4SNAhuHO63MAPNsHh3qQ8iHBZ2a9Lodcg0QKBgC1C\\nHzqIXjVSwB7nLLW4HY6IrMF2Pj5gg6WoCDZFdPd9GrFf1v3AB7l8BHp60M1qN8Ss\\nixuEPqMGCoj7rSYWPM/7aIRx9y+04N2ZKkuXod9u5iAt3k9pJJVeGD3TQLX/1lu+\\nVd6zpcFONDb2yKbwQyjC2nmY0mDoWwhUenepW0DZAoGAUADLxGsgikcSd25IXWuO\\nUsTWHTJioyUl3r2GXt7hZJU085Mu0N1Ib9X2EslNWTTPPBzV6vwEX6gcBrRWsbRl\\n1hmpqtfGq0uB13/1Xu8W2hmn2dQe65y8Ce/ItOTHDdK9BdACKokpR2xbE33fQ77C\\nxdjr8wdB7GHZHw4yDUOxzb8=\\n-----END PRIVATE KEY-----\\n";

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    private DataStoreParams newValidParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, VALID_PRIVATE_KEY);
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "test@example.com");
        return params;
    }

    @Test
    public void testPrivateKey() throws Exception {
        final PrivateKey privateKey = GSuiteClient.parsePrivateKey(VALID_PRIVATE_KEY);
        assertNotNull(privateKey);
        assertEquals("RSA", privateKey.getAlgorithm());
    }

    @Test
    public void testPrivateKeyWithNewlines() throws Exception {
        final String withRealNewlines = VALID_PRIVATE_KEY.replace("\\n", "\n");
        final PrivateKey privateKey = GSuiteClient.parsePrivateKey(withRealNewlines);
        assertNotNull(privateKey);
        assertEquals("RSA", privateKey.getAlgorithm());
    }

    @Test
    public void testConstructorWithMissingPrivateKey() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "test@example.com");
        try {
            new GSuiteClient(params);
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("private_key"));
        }
    }

    @Test
    public void testConstructorWithMissingPrivateKeyId() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, VALID_PRIVATE_KEY);
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "test@example.com");
        try {
            new GSuiteClient(params);
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("private_key_id"));
        }
    }

    @Test
    public void testConstructorWithMissingClientEmail() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, VALID_PRIVATE_KEY);
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        try {
            new GSuiteClient(params);
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("client_email"));
        }
    }

    @Test
    public void testAllDrivesConstant() {
        assertEquals("allDrives", GSuiteClient.ALL_DRIVES);
    }

    @Test
    public void testDefaultConstants() {
        assertEquals(1024 * 1024, GSuiteClient.DEFAULT_MAX_CACHED_CONTENT_SIZE);
        assertEquals("3540", GSuiteClient.DEFAULT_REFRESH_TOKEN_INTERVAL);
        assertEquals(20 * 1000, GSuiteClient.DEFAULT_READ_TIMEOUT_MS);
        assertEquals(20 * 1000, GSuiteClient.DEFAULT_CONNECT_TIMEOUT_MS);
        assertEquals(3600000L, GSuiteClient.JWT_TOKEN_VALIDITY_MS);
        assertEquals("\\\\n|\\n|-----[A-Z ]+-----", GSuiteClient.PEM_CLEANUP_PATTERN);
    }

    @Test
    public void testGetPrivateKey_WithEmptyKey() {
        try {
            GSuiteClient.parsePrivateKey("-----BEGIN PRIVATE KEY-----\\n-----END PRIVATE KEY-----\\n");
            fail("should throw for an empty key");
        } catch (final Exception e) {
            assertTrue(e.getMessage(), e.getMessage().contains("empty"));
        }
    }

    @Test
    public void testGetPrivateKey_WithInvalidBase64() {
        try {
            GSuiteClient.parsePrivateKey("-----BEGIN PRIVATE KEY-----\\n!!!not-base64!!!\\n-----END PRIVATE KEY-----\\n");
            fail("should throw for invalid base64");
        } catch (final Exception e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testRequestInitializerTimeouts() throws Exception {
        final DataStoreParams params = newValidParams();
        params.put(GSuiteClient.READ_TIMEOUT, "1234");
        params.put(GSuiteClient.CONNECT_TIMEOUT, "5678");
        final MockDriveTransport transport = new MockDriveTransport();
        // ServiceAccountCredentials is built with explicit scopes, so it always uses the
        // real OAuth2 assertion flow (never a self-signed JWT), and initialize() blocks
        // on a token fetch through the credentials' own transport.
        transport.queueJson("{\"access_token\":\"test-token\",\"expires_in\":3600,\"token_type\":\"Bearer\"}");
        final GSuiteClient client = new GSuiteClient(params, transport);
        final HttpRequest request = new MockDriveTransport().createRequestFactory().buildGetRequest(new GenericUrl("https://example.com/"));
        client.requestInitializer.initialize(request);
        assertEquals(1234, request.getReadTimeout());
        assertEquals(5678, request.getConnectTimeout());
        client.close();
    }

    @Test
    public void testRequestInitializerDefaultTimeouts() throws Exception {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson("{\"access_token\":\"test-token\",\"expires_in\":3600,\"token_type\":\"Bearer\"}");
        final GSuiteClient client = new GSuiteClient(newValidParams(), transport);
        final HttpRequest request = new MockDriveTransport().createRequestFactory().buildGetRequest(new GenericUrl("https://example.com/"));
        client.requestInitializer.initialize(request);
        assertEquals(GSuiteClient.DEFAULT_READ_TIMEOUT_MS, request.getReadTimeout());
        assertEquals(GSuiteClient.DEFAULT_CONNECT_TIMEOUT_MS, request.getConnectTimeout());
        client.close();
    }

    @Test
    public void testRequestInitializerWithNullHttpTransport() {
        final GSuiteClient client = new GSuiteClient(newValidParams(), new MockDriveTransport());
        assertNotNull(client.credentials);
        assertTrue("credentials should be a ServiceAccountCredentials", client.credentials instanceof ServiceAccountCredentials);
        client.close();
    }

    @Test
    public void testDefaultScopeIsReadOnly() {
        final GSuiteClient client = new GSuiteClient(newValidParams(), new MockDriveTransport());
        final Collection<String> scopes = client.getScopes();
        assertEquals(1, scopes.size());
        assertEquals("https://www.googleapis.com/auth/drive.readonly", scopes.iterator().next());
        client.close();
    }

    @Test
    public void testScopesParameterOverridesDefault() {
        final DataStoreParams params = newValidParams();
        params.put(GSuiteClient.SCOPES,
                "https://www.googleapis.com/auth/drive.readonly, https://www.googleapis.com/auth/admin.directory.user.readonly");
        final GSuiteClient client = new GSuiteClient(params, new MockDriveTransport());
        final List<String> scopes = new ArrayList<>(client.getScopes());
        assertEquals(2, scopes.size());
        assertTrue(scopes.toString(), scopes.contains("https://www.googleapis.com/auth/admin.directory.user.readonly"));
        client.close();
    }

    @Test
    public void testEmptyScopesParameterFallsBackToDefault() {
        final DataStoreParams params = newValidParams();
        params.put(GSuiteClient.SCOPES, "");
        final GSuiteClient client = new GSuiteClient(params, new MockDriveTransport());
        final Collection<String> scopes = client.getScopes();
        assertEquals(1, scopes.size());
        assertEquals("https://www.googleapis.com/auth/drive.readonly", scopes.iterator().next());
        client.close();
    }

    @Test
    public void testBlankScopesParameterFallsBackToDefault() {
        final DataStoreParams params = newValidParams();
        params.put(GSuiteClient.SCOPES, "  ");
        final GSuiteClient client = new GSuiteClient(params, new MockDriveTransport());
        final Collection<String> scopes = client.getScopes();
        assertEquals(1, scopes.size());
        assertEquals("https://www.googleapis.com/auth/drive.readonly", scopes.iterator().next());
        client.close();
    }

    @Test
    public void testScopesParameterWithNoUsableScopeThrows() {
        final DataStoreParams params = newValidParams();
        params.put(GSuiteClient.SCOPES, ",");
        try {
            new GSuiteClient(params, new MockDriveTransport());
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage().contains("scopes"));
        }
    }

    @Test
    public void testImpersonateUserProducesDelegatedCredentials() {
        final DataStoreParams params = newValidParams();
        params.put(GSuiteClient.IMPERSONATE_USER, "admin@example.com");
        final GSuiteClient client = new GSuiteClient(params, new MockDriveTransport());
        final GoogleCredentials credentials = client.credentials;
        assertTrue("credentials should be a ServiceAccountCredentials", credentials instanceof ServiceAccountCredentials);
        assertEquals("admin@example.com", ((ServiceAccountCredentials) credentials).getServiceAccountUser());
        client.close();
    }

    /**
     * Tests that getFiles sends the current shared-drive parameters,
     * not the deprecated Team Drive API ones.
     */
    @Test
    public void testGetFilesSendsModernSharedDriveParameters() {
        final MockDriveTransport transport = new MockDriveTransport();
        transport.queueJson("{\"access_token\":\"test-token\",\"expires_in\":3600,\"token_type\":\"Bearer\"}");
        transport.queueJson("{\"files\":[{\"id\":\"F1\",\"name\":\"a.txt\"}]}");
        final GSuiteClient client = new GSuiteClient(newValidParams(), transport);
        final List<String> ids = new ArrayList<>();
        client.getFiles(null, GSuiteClient.ALL_DRIVES, null, "*", file -> ids.add(file.getId()));
        assertEquals(1, ids.size());
        final String url = transport.getRequestedUrls().get(1);
        assertTrue(url, url.contains("includeItemsFromAllDrives=true"));
        assertTrue(url, url.contains("supportsAllDrives=true"));
        assertFalse(url, url.contains("includeTeamDriveItems"));
        assertFalse(url, url.contains("supportsTeamDrives"));
        client.close();
    }
}
