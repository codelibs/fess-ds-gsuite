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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
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

public class GoogleDriveDataStoreMaxSizeTest extends UnitDsTestCase {

    private final AtomicBoolean downloaded = new AtomicBoolean(false);
    private final AtomicReference<Throwable> capturedError = new AtomicReference<>();

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

    /** A configMap with the given max_size and no URL filter. */
    private Map<String, Object> newConfigMap(final long maxSize) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(GoogleDriveDataStore.MAX_SIZE, Long.valueOf(maxSize));
        configMap.put(GoogleDriveDataStore.IGNORE_FOLDER, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.IGNORE_ERROR, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });
        configMap.put(GoogleDriveDataStore.URL_FILTER, null);
        return configMap;
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

    /** A data store whose content extraction is recorded instead of performed. */
    private GoogleDriveDataStore newDataStore(final String content) {
        return new GoogleDriveDataStore() {
            @Override
            protected String getFileContents(final GSuiteClient client, final File file, final boolean ignoreError) {
                downloaded.set(true);
                return content;
            }

            @Override
            protected List<String> getFilePermissions(final Map<String, Object> configMap, final DataStoreParams paramMap,
                    final GSuiteClient client, final File file) {
                // These tests are about the size checks, not about the ACL: the file only needs a
                // role so that the fail-closed rule does not skip it before the size is checked.
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
    }

    private void run(final GoogleDriveDataStore dataStore, final Map<String, Object> configMap, final File file) {
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("url", "url");
        dataStore.processFile(null, newCallback(), configMap, new DataStoreParams(), scriptMap, new HashMap<>(), null, file);
    }

    @Test
    public void test_processFile_oversizedFileIsNotDownloaded() {
        registerHelpers();
        final File file = new File();
        file.setId("abc123");
        file.setName("huge.bin");
        file.setMimeType("application/octet-stream");
        file.setWebViewLink("https://drive.google.com/file/d/abc123/view");
        file.setSize(Long.valueOf(20000000L));

        run(newDataStore("hello world"), newConfigMap(10000000L), file);

        assertFalse(downloaded.get());
        assertNotNull(capturedError.get());
        assertTrue(capturedError.get() instanceof MaxLengthExceededException);
    }

    @Test
    public void test_processFile_nativeFileIsCheckedAfterExtraction() {
        registerHelpers();
        final File file = new File();
        file.setId("abc123");
        file.setName("doc");
        file.setMimeType("application/vnd.google-apps.document");
        file.setWebViewLink("https://drive.google.com/file/d/abc123/view");
        // Google native formats do not report a size.

        run(newDataStore("0123456789ABC"), newConfigMap(10L), file);

        assertTrue(downloaded.get());
        assertNotNull(capturedError.get());
        assertTrue(capturedError.get() instanceof MaxLengthExceededException);
    }

    @Test
    public void test_processFile_fileWithinLimitIsDownloaded() {
        registerHelpers();
        final File file = new File();
        file.setId("abc123");
        file.setName("small.txt");
        file.setMimeType("text/plain");
        file.setWebViewLink("https://drive.google.com/file/d/abc123/view");
        file.setSize(Long.valueOf(11L));

        run(newDataStore("hello world"), newConfigMap(10000000L), file);

        assertTrue(downloaded.get());
        assertNull(capturedError.get());
    }
}
