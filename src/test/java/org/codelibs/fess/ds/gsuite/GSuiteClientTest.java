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

import java.security.PrivateKey;

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

public class GSuiteClientTest extends LastaFluteTestCase {

    private static final String VALID_PRIVATE_KEY =
            "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDOnYxwfP57dOGx\\nxwY7f3yB17RewqW4bRmTEg/uetU9yA+TgYqNG7r/tPr3gzAQN39Q+Hu5dH2J6JEm\\n9kGLh3XTFuQUNZlTV0jBV7/l0tsfaemHNbGnt1GIJITnxgVWAtxnZb6O3G9p6W9p\\nv2v4o5Lz1NBMmyBTAQU/onAGArj6W26GMAeM5ZNNbscugV22AKNtWwN3xHLnd3eJ\\nKzi2CbwrL/XfUGFEyGD6vVUIWmAre2zq1OxcpcWGKTn2IPefrlIceERwCJvZXxQ5\\nQyk0adTMJq5Qfb3lF1cgndG2P3F+Mcxoo2+RrtJUwBQSJC6Aq1D26fps4CJzn1YC\\n6WKiG7xFAgMBAAECggEACYeZPmf5edrAfSljl3NwG/IFwvgh2iGIFDE5VGPMeZLE\\nayaGrC76/0fK6ocdvKW+pM6tMDbYAngcV8p0Z/nZvKB56Re2yHIGbEp+kpxY2HhT\\nWdXnaYeqRkf+7EzFGrw7i7ZU5XRz3BP0/FDkqz1qLf5jFCF0ineJ1S9KEPDntL5V\\nGRyj4uAjsG0aD0R6QpYf2+7rslyPkTgruiXPqkpHZ5cwHROMf8YjZskQPUAsNr+Q\\nr3ovH6rEXEpKo4Jps2JxJniD2V3PsHZ9QX5n4a6jWDfZEy59f4w4UVmQZLM/Ma5/\\nVwdug8BrcHS1MySir1j6s3bPXV2Nm8u8PzTr+F/jsQKBgQDzWHUA6V9ekQ2Rwqul\\ncJ91jehZMHg17NQud+/bn2kLiv1u87Td0+asN8ywozL1LBquMucl7nMw/gGFD2ni\\nlZM3c7dZlNkCkm1yPhAniz+KLP7l5fBhZ/dN/44ZGYP/4qwo0KfELRRjnNqo+9Uc\\nL2RLCEimZaH2fXYdX4xzyh+WUQKBgQDZXB8BAWbzEtMekNIN+RUsbZ7c5l5DI0T3\\nGglZJESHj8fXYCvHQEy4OBeI0p/qS4NDISBOVHXzGxTOLV1FScgPwzoKeiig0AW/\\n4O5q0Yo+heaxsgE5ghUO8hw2seSiP1c5+01/om7tbT/rMe6felFJsJfQoul7mutq\\nD2+Mz1DltQKBgQCm2wt3MY3kIN/GB058pQmhqEkeBr8WcqpWpoR/+gEkGgyGbHKi\\n++4aPjSLFYwWUkSFF4ApISQ4/qH6I8R9ygPkrOKWeRqHyfFjuSyIgNFzpECvUIgP\\nsiL/h3Bew4EgDsPvRIsUV7i4SNAhuHO63MAPNsHh3qQ8iHBZ2a9Lodcg0QKBgC1C\\nHzqIXjVSwB7nLLW4HY6IrMF2Pj5gg6WoCDZFdPd9GrFf1v3AB7l8BHp60M1qN8Ss\\nixuEPqMGCoj7rSYWPM/7aIRx9y+04N2ZKkuXod9u5iAt3k9pJJVeGD3TQLX/1lu+\\nVd6zpcFONDb2yKbwQyjC2nmY0mDoWwhUenepW0DZAoGAUADLxGsgikcSd25IXWuO\\nUsTWHTJioyUl3r2GXt7hZJU085Mu0N1Ib9X2EslNWTTPPBzV6vwEX6gcBrRWsbRl\\n1hmpqtfGq0uB13/1Xu8W2hmn2dQe65y8Ce/ItOTHDdK9BdACKokpR2xbE33fQ77C\\nxdjr8wdB7GHZHw4yDUOxzb8=\\n-----END PRIVATE KEY-----\\n";

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    public void testPrivateKey() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, VALID_PRIVATE_KEY);
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "test@example.com");
        final GSuiteClient.RequestInitializer initializer = new GSuiteClient.RequestInitializer(params, null);
        try {
            final PrivateKey privateKey = initializer.getPrivateKey();
            assertNotNull(privateKey);
            assertEquals("RSA", privateKey.getAlgorithm());
        } catch (final Exception e) {
            fail(e.getMessage());
        }
    }

    public void testPrivateKeyWithNewlines() {
        final String privateKeyWithNewlines = VALID_PRIVATE_KEY.replace("\\n", "\n");
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, privateKeyWithNewlines);
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "test@example.com");
        final GSuiteClient.RequestInitializer initializer = new GSuiteClient.RequestInitializer(params, null);
        try {
            final PrivateKey privateKey = initializer.getPrivateKey();
            assertNotNull(privateKey);
            assertEquals("RSA", privateKey.getAlgorithm());
        } catch (final Exception e) {
            fail(e.getMessage());
        }
    }

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

    public void testRequestInitializerTimeouts() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, VALID_PRIVATE_KEY);
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "test@example.com");
        params.put(GSuiteClient.READ_TIMEOUT, "30000");
        params.put(GSuiteClient.CONNECT_TIMEOUT, "15000");
        final GSuiteClient.RequestInitializer initializer = new GSuiteClient.RequestInitializer(params, null);
        assertNotNull(initializer);
        assertEquals(30000, initializer.readTimeout);
        assertEquals(15000, initializer.connectTimeout);
    }

    public void testRequestInitializerDefaultTimeouts() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, VALID_PRIVATE_KEY);
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "test@example.com");
        final GSuiteClient.RequestInitializer initializer = new GSuiteClient.RequestInitializer(params, null);
        assertNotNull(initializer);
        assertEquals(20000, initializer.readTimeout);
        assertEquals(20000, initializer.connectTimeout);
    }

    public void testRequestInitializerWithNullHttpTransport() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, VALID_PRIVATE_KEY);
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "test@example.com");
        final GSuiteClient.RequestInitializer initializer = new GSuiteClient.RequestInitializer(params, null);
        assertNotNull(initializer);
        assertEquals("test_key_id", initializer.privateKeyId);
        assertEquals("test@example.com", initializer.clientEmail);
    }

    public void testAllDrivesConstant() {
        assertEquals("allDrives", GSuiteClient.ALL_DRIVES);
    }
}
