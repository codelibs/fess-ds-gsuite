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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.FileTypeHelper;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.drive.model.File;

public class GoogleDriveDataStoreLifecycleTest extends UnitDsTestCase {

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

    /**
     * A GSuiteClient that never talks to Google: getFiles simply replays the files it was given.
     * The package-visible transport constructor keeps the credentials off the network.
     */
    private static final class StubGSuiteClient extends GSuiteClient {

        private final List<File> files;

        StubGSuiteClient(final DataStoreParams params, final HttpTransport httpTransport, final List<File> files) {
            super(params, httpTransport);
            this.files = files;
        }

        @Override
        public void getFiles(final String q, final String corpora, final String spaces, final String fields,
                final Consumer<File> consumer) {
            files.forEach(consumer);
        }
    }

    /** Registers the helpers processFile resolves through ComponentUtil. */
    private void registerHelpers() {
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        ComponentUtil.register(new FileTypeHelper(), "fileTypeHelper");
        ComponentUtil.register(new PermissionHelper(), "permissionHelper");
    }

    /** Parameters carrying a syntactically valid service account key. */
    private DataStoreParams newCredentialParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put("private_key", VALID_PRIVATE_KEY);
        params.put("private_key_id", "test_key_id");
        params.put("client_email", "test@example.com");
        return params;
    }

    /** A callback that does nothing. */
    private IndexUpdateCallback newCallback() {
        return new IndexUpdateCallback() {
            @Override
            public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                // no-op
            }

            @Override
            public long getDocumentSize() {
                return 0;
            }

            @Override
            public long getExecuteTime() {
                return 0;
            }

            @Override
            public void commit() {
                // no-op
            }
        };
    }

    private File newFile(final String id) {
        final File file = new File();
        file.setId(id);
        file.setName(id + ".txt");
        file.setMimeType("text/plain");
        file.setWebViewLink("https://drive.google.com/file/d/" + id + "/view");
        file.setSize(Long.valueOf(11L));
        return file;
    }

    @Test
    public void test_getThreadPoolTimeoutSeconds_default() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        assertEquals(60L, dataStore.getThreadPoolTimeoutSeconds(new DataStoreParams()));
    }

    @Test
    public void test_getThreadPoolTimeoutSeconds_custom() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams params = new DataStoreParams();
        params.put("thread_pool_timeout_seconds", "5");
        assertEquals(5L, dataStore.getThreadPoolTimeoutSeconds(params));
    }

    @Test
    public void test_getThreadPoolTimeoutSeconds_invalid() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams params = new DataStoreParams();
        params.put("thread_pool_timeout_seconds", "not-a-number");
        assertEquals(60L, dataStore.getThreadPoolTimeoutSeconds(params));
    }

    @Test
    public void test_threadPoolTimeoutSecondsConstant() {
        assertEquals("thread_pool_timeout_seconds", GoogleDriveDataStore.THREAD_POOL_TIMEOUT_SECONDS);
    }

    @Test
    public void test_processFile_skippedAfterStop() {
        registerHelpers();
        final AtomicReference<Map<String, Object>> capturedResultMap = new AtomicReference<>();
        final AtomicReference<Throwable> capturedError = new AtomicReference<>();
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
                return "hello world";
            }

            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                capturedResultMap.set(resultMap);
                return null;
            }

            @Override
            protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
                    final CrawlerStatsHelper crawlerStatsHelper, final Throwable t) {
                capturedError.set(t);
            }
        };
        dataStore.stop();

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(GoogleDriveDataStore.MAX_SIZE, Long.valueOf(10000000L));
        configMap.put(GoogleDriveDataStore.IGNORE_FOLDER, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.IGNORE_ERROR, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });
        configMap.put(GoogleDriveDataStore.URL_FILTER, null);
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("url", "url");

        dataStore.processFile(null, newCallback(), configMap, new DataStoreParams(), scriptMap, new HashMap<>(), null, newFile("f1"));

        assertNull(capturedError.get());
        assertNull(capturedResultMap.get());
    }

    @Test
    public void test_storeFiles_stopsDispatchingAfterStop() throws Exception {
        registerHelpers();
        final List<String> processed = Collections.synchronizedList(new ArrayList<>());
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
                    final GSuiteClient client, final File file) {
                processed.add(file.getId());
            }
        };
        dataStore.stop();

        final DataStoreParams params = newCredentialParams();
        try (GSuiteClient client = new StubGSuiteClient(params, new MockHttpTransport(), Arrays.asList(newFile("f1"), newFile("f2")))) {
            dataStore.storeFiles(null, newCallback(), new HashMap<>(), params, new HashMap<>(), new HashMap<>(), client);
        }

        assertEquals(0, processed.size());
    }

    @Test
    public void test_storeFiles_dispatchesWhileAlive() throws Exception {
        registerHelpers();
        final List<String> processed = Collections.synchronizedList(new ArrayList<>());
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
                    final GSuiteClient client, final File file) {
                processed.add(file.getId());
            }
        };

        final DataStoreParams params = newCredentialParams();
        try (GSuiteClient client = new StubGSuiteClient(params, new MockHttpTransport(), Arrays.asList(newFile("f1"), newFile("f2")))) {
            dataStore.storeFiles(null, newCallback(), new HashMap<>(), params, new HashMap<>(), new HashMap<>(), client);
        }

        assertEquals(2, processed.size());
    }
}
