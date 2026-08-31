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

import org.codelibs.fess.Constants;
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

public class GoogleDriveDataStoreParamMapTest extends UnitDsTestCase {

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

    @Test
    public void test_processFile_doesNotWriteStatsKeyToSharedParamMap() {
        registerHelpers();
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
                return null;
            }

            @Override
            protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
                    final CrawlerStatsHelper crawlerStatsHelper, final Throwable t) {
                capturedError.set(t);
            }
        };

        final DataStoreParams sharedParamMap = new DataStoreParams();
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("url", "url");

        dataStore.processFile(null, newCallback(null), newConfigMap(), sharedParamMap, scriptMap, new HashMap<>(), null, newFile());

        assertNull(capturedError.get());
        assertFalse(sharedParamMap.containsKey(Constants.CRAWLER_STATS_KEY));
    }

    @Test
    public void test_processFile_callbackGetsItsOwnParamMapCopy() {
        registerHelpers();
        final AtomicReference<Throwable> capturedError = new AtomicReference<>();
        final AtomicReference<DataStoreParams> capturedCallbackParams = new AtomicReference<>();
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
                return null;
            }

            @Override
            protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
                    final CrawlerStatsHelper crawlerStatsHelper, final Throwable t) {
                capturedError.set(t);
            }
        };

        final DataStoreParams sharedParamMap = new DataStoreParams();
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("url", "url");

        dataStore.processFile(null, newCallback(capturedCallbackParams), newConfigMap(), sharedParamMap, scriptMap, new HashMap<>(), null,
                newFile());

        assertNull(capturedError.get());
        assertNotNull(capturedCallbackParams.get());
        assertNotSame(sharedParamMap, capturedCallbackParams.get());
        assertTrue(capturedCallbackParams.get().containsKey(Constants.CRAWLER_STATS_KEY));
    }

    /** A callback that records the DataStoreParams it was handed. */
    private IndexUpdateCallback newCallback(final AtomicReference<DataStoreParams> captured) {
        return new IndexUpdateCallback() {
            @Override
            public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                if (captured != null) {
                    captured.set(paramMap);
                }
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
}
