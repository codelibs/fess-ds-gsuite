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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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

import com.google.api.services.drive.model.File;

public class GoogleDriveDataStoreScriptContextTest extends UnitDsTestCase {

    private static final String PRIVATE_KEY_VALUE = "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBg-secret-\\n-----END PRIVATE KEY-----\\n";

    /** A placeholder proxy password. Never a real credential. */
    private static final String PROXY_PASSWORD_VALUE = "not-a-real-secret";

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
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

    /** A configMap holding exactly the entries processFile reads, with no URL filter. */
    private Map<String, Object> newConfigMap() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(GoogleDriveDataStore.MAX_SIZE, Long.valueOf(10000000L));
        configMap.put(GoogleDriveDataStore.IGNORE_FOLDER, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.IGNORE_ERROR, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });
        configMap.put(GoogleDriveDataStore.URL_FILTER, null);
        return configMap;
    }

    /** A plain text file with everything processFile needs. */
    private File newFile() {
        final File file = new File();
        file.setId("abc123");
        file.setName("doc.txt");
        file.setMimeType("text/plain");
        file.setWebViewLink("https://drive.google.com/file/d/abc123/view");
        file.setSize(Long.valueOf(11L));
        return file;
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

    @Test
    public void test_processFile_stripsServiceAccountSecretsFromScriptContext() {
        registerHelpers();
        final AtomicReference<Map<String, Object>> capturedResultMap = new AtomicReference<>();
        final AtomicReference<Throwable> capturedError = new AtomicReference<>();
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
                return "hello world";
            }

            @Override
            protected List<String> getFilePermissions(final Map<String, Object> configMap, final DataStoreParams paramMap,
                    final GSuiteClient client, final File file) {
                // This test is not about the ACL: the file only needs a role so that the
                // fail-closed rule does not skip it.
                return Arrays.asList("1owner@example.com");
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

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("private_key", PRIVATE_KEY_VALUE);
        paramMap.put("private_key_id", "key-id-0001");
        paramMap.put("client_email", "svc@project.iam.gserviceaccount.com");
        paramMap.put("proxy_username", "proxy-user");
        paramMap.put("proxy_password", PROXY_PASSWORD_VALUE);
        paramMap.put("max_size", "10000000");
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("digest", "private_key");

        dataStore.processFile(null, newCallback(), newConfigMap(), paramMap, scriptMap, new HashMap<>(), null, newFile());

        assertNull(capturedError.get());
        final Map<String, Object> resultMap = capturedResultMap.get();
        assertNotNull(resultMap);
        assertFalse(resultMap.containsKey("private_key"));
        assertFalse(resultMap.containsKey("private_key_id"));
        assertFalse(resultMap.containsKey("client_email"));
        assertFalse(resultMap.containsKey("proxy_username"));
        assertFalse(resultMap.containsKey("proxy_password"));
        assertFalse(resultMap.values().stream().anyMatch(v -> PRIVATE_KEY_VALUE.equals(v)));
        assertFalse(resultMap.values().stream().anyMatch(v -> "key-id-0001".equals(v)));
        assertFalse(resultMap.values().stream().anyMatch(v -> "svc@project.iam.gserviceaccount.com".equals(v)));
        assertFalse(resultMap.values().stream().anyMatch(v -> PROXY_PASSWORD_VALUE.equals(v)));
        assertFalse(resultMap.values().stream().anyMatch(v -> "proxy-user".equals(v)));
        // Non-secret parameters must survive, so the script context stays usable.
        assertEquals("10000000", resultMap.get("max_size"));
    }

    @Test
    public void test_processFile_scriptReferencingPrivateKeyResolvesToNull() {
        registerHelpers();
        final AtomicReference<Object> capturedDigest = new AtomicReference<>();
        final AtomicReference<Throwable> capturedError = new AtomicReference<>();
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
                return "hello world";
            }

            @Override
            protected List<String> getFilePermissions(final Map<String, Object> configMap, final DataStoreParams paramMap,
                    final GSuiteClient client, final File file) {
                // This test is not about the ACL: the file only needs a role so that the
                // fail-closed rule does not skip it.
                return Arrays.asList("1owner@example.com");
            }

            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                // Mirrors how the real script engine resolves a bare parameter-name expression:
                // a direct key lookup against the evaluation context (resultMap).
                return resultMap.get(template);
            }

            @Override
            protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
                    final CrawlerStatsHelper crawlerStatsHelper, final Throwable t) {
                capturedError.set(t);
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("private_key", PRIVATE_KEY_VALUE);
        paramMap.put("private_key_id", "key-id-0001");
        paramMap.put("client_email", "svc@project.iam.gserviceaccount.com");
        final Map<String, String> scriptMap = new HashMap<>();
        // A crawl script author trying to exfiltrate the service account key, e.g. digest=private_key.
        scriptMap.put("digest", "private_key");

        dataStore.processFile(null, new IndexUpdateCallback() {
            @Override
            public void store(final DataStoreParams p, final Map<String, Object> dataMap) {
                capturedDigest.set(dataMap.get("digest"));
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
        }, newConfigMap(), paramMap, scriptMap, new HashMap<>(), null, newFile());

        assertNull(capturedError.get());
        // The script's "private_key" expression must not resolve to the secret value: the key is
        // gone from the evaluation context entirely, so the lookup misses and nothing is indexed.
        assertNull(capturedDigest.get());
    }

    /**
     * The proxy password is a credential of exactly the same kind as the service account key: a
     * crawl script such as {@code digest=proxy_password} would otherwise write it into the search
     * index, readable by anyone with search access.
     */
    @Test
    public void test_processFile_scriptReferencingProxyPasswordResolvesToNull() {
        registerHelpers();
        final AtomicReference<Object> capturedDigest = new AtomicReference<>();
        final AtomicReference<Throwable> capturedError = new AtomicReference<>();
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
                return "hello world";
            }

            @Override
            protected List<String> getFilePermissions(final Map<String, Object> configMap, final DataStoreParams paramMap,
                    final GSuiteClient client, final File file) {
                // This test is not about the ACL: the file only needs a role so that the
                // fail-closed rule does not skip it.
                return Arrays.asList("1owner@example.com");
            }

            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                // Mirrors how the real script engine resolves a bare parameter-name expression:
                // a direct key lookup against the evaluation context (resultMap).
                return resultMap.get(template);
            }

            @Override
            protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
                    final CrawlerStatsHelper crawlerStatsHelper, final Throwable t) {
                capturedError.set(t);
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("private_key", PRIVATE_KEY_VALUE);
        paramMap.put("private_key_id", "key-id-0001");
        paramMap.put("client_email", "svc@project.iam.gserviceaccount.com");
        paramMap.put("proxy_username", "proxy-user");
        paramMap.put("proxy_password", PROXY_PASSWORD_VALUE);
        final Map<String, String> scriptMap = new HashMap<>();
        // A crawl script author trying to exfiltrate the proxy credentials.
        scriptMap.put("digest", "proxy_password");

        dataStore.processFile(null, new IndexUpdateCallback() {
            @Override
            public void store(final DataStoreParams p, final Map<String, Object> dataMap) {
                capturedDigest.set(dataMap.get("digest"));
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
        }, newConfigMap(), paramMap, scriptMap, new HashMap<>(), null, newFile());

        assertNull(capturedError.get());
        assertNull("the proxy password must be gone from the evaluation context entirely", capturedDigest.get());
    }
}
