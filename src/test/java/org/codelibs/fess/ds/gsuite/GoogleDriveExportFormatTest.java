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

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;

/**
 * Tests for the export target selection driven by the live exportFormats map.
 */
public class GoogleDriveExportFormatTest extends UnitDsTestCase {

    /** The mime type of a Google Doc. */
    private static final String DOCUMENT = "application/vnd.google-apps.document";
    /** The mime type of a Google Form, which has no row in exportFormats. */
    private static final String FORM = "application/vnd.google-apps.form";
    /** Marker recorded when the stub client is asked for a plain download instead of an export. */
    private static final String ALT_MEDIA = "alt=media";

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * A realistic subset of the map about.get returns. Forms and Sites are deliberately absent,
     * exactly as the live API reports them.
     *
     * @return The export formats.
     */
    protected static Map<String, List<String>> newExportFormats() {
        return Map.of(
                DOCUMENT, List.of("application/rtf", "application/vnd.oasis.opendocument.text", "text/html", "application/pdf",
                        "text/plain", "text/markdown", "application/zip", "application/epub+zip"),
                "application/vnd.google-apps.spreadsheet",
                List.of("application/x-vnd.oasis.opendocument.spreadsheet", "text/tab-separated-values", "application/pdf", "text/csv",
                        "application/zip"),
                "application/vnd.google-apps.presentation",
                List.of("application/vnd.oasis.opendocument.presentation", "application/pdf", "text/plain"),
                "application/vnd.google-apps.drawing", List.of("image/svg+xml", "image/png", "application/pdf", "image/jpeg"),
                "application/vnd.google-apps.script", List.of("application/vnd.google-apps.script+json"));
    }

    /**
     * A client that answers with {@link #newExportFormats()} and records which content channel was
     * used: the export target mime type, or {@link #ALT_MEDIA} when the caller fell through to a
     * plain download. Recording the download rather than only throwing is what makes these tests
     * discriminating -- the previous implementation downloaded Forms and Drawings with alt=media,
     * and the resulting failure was swallowed by the ignore-error handling of
     * {@code getFileContents}, leaving nothing else to assert on.
     * <p>
     * The local is deliberately not named {@code params}: inside the anonymous subclass body the
     * inherited field of {@link GSuiteClient} would shadow it.
     * </p>
     *
     * @param requested Receives the export mime type, or {@link #ALT_MEDIA} for a plain download.
     * @param text The text files.export returns.
     * @return The client.
     */
    protected static GSuiteClient newExportingClient(final AtomicReference<String> requested, final String text) {
        final MockDriveTransport mockTransport = new MockDriveTransport();
        return new GSuiteClient(GSuiteClientApiTest.newParams(), mockTransport) {
            @Override
            public Map<String, List<String>> getExportFormats() {
                return newExportFormats();
            }

            @Override
            public String extractFileText(final String id, final String mimeType) {
                requested.set(mimeType);
                return text;
            }

            @Override
            public InputStream getFileInputStream(final String id) {
                requested.set(ALT_MEDIA);
                throw new IllegalStateException("alt=media never works for a native Google type");
            }
        };
    }

    /**
     * A real client whose export runs over {@link MockDriveTransport}, with the export size limit
     * lowered so that a small body trips the guard.
     * <p>
     * The locals are deliberately not named {@code drive} or {@code params}: inside the anonymous
     * subclass body the inherited fields of {@link GSuiteClient} would shadow them.
     * </p>
     *
     * @param mockTransport The transport that replays the export response.
     * @param limit The export size limit, in bytes.
     * @return The client.
     */
    protected static GSuiteClient newSizeLimitedClient(final MockDriveTransport mockTransport, final long limit) {
        final Drive mockDrive = GSuiteClientApiTest.newDrive(mockTransport);
        return new GSuiteClient(GSuiteClientApiTest.newParams(), mockTransport) {
            @Override
            protected Drive getDrive() {
                return mockDrive;
            }

            @Override
            protected long getExportSizeLimit() {
                return limit;
            }

            @Override
            public Map<String, List<String>> getExportFormats() {
                return newExportFormats();
            }
        };
    }

    /**
     * Docs prefer Markdown over plain text because the structure survives.
     */
    @Test
    public void test_selectExportMimeType_documentPrefersMarkdown() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        assertEquals("text/markdown", dataStore.selectExportMimeType(DOCUMENT, newExportFormats()));
    }

    /**
     * When the preferred target is missing, the next preference is used. A tenant without Markdown
     * export must land on plain text rather than on whatever happens to come first in the map.
     */
    @Test
    public void test_selectExportMimeType_fallsBackWithinPreferences() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final Map<String, List<String>> formats = Map.of(DOCUMENT, List.of("application/pdf", "text/plain"));
        assertEquals("text/plain", dataStore.selectExportMimeType(DOCUMENT, formats));
    }

    /**
     * Sheets export as TSV, slides as plain text, drawings as PNG, Apps Script as its JSON bundle.
     */
    @Test
    public void test_selectExportMimeType_otherNativeTypes() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final Map<String, List<String>> formats = newExportFormats();
        assertEquals("text/tab-separated-values", dataStore.selectExportMimeType("application/vnd.google-apps.spreadsheet", formats));
        assertEquals("text/plain", dataStore.selectExportMimeType("application/vnd.google-apps.presentation", formats));
        assertEquals("image/png", dataStore.selectExportMimeType("application/vnd.google-apps.drawing", formats));
        assertEquals("application/vnd.google-apps.script+json",
                dataStore.selectExportMimeType("application/vnd.google-apps.script", formats));
    }

    /**
     * D-16: Forms and Sites have no export format at all. They must resolve to null so the caller
     * indexes the metadata instead of falling through to alt=media, which always fails.
     */
    @Test
    public void test_selectExportMimeType_formsAndSitesHaveNoTarget() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final Map<String, List<String>> formats = newExportFormats();
        assertNull("forms have no export format", dataStore.selectExportMimeType(FORM, formats));
        assertNull("sites have no export format", dataStore.selectExportMimeType("application/vnd.google-apps.site", formats));
        assertNull("folders have no export format", dataStore.selectExportMimeType("application/vnd.google-apps.folder", formats));
    }

    /**
     * A null or empty map must not blow up; it simply means nothing can be exported.
     */
    @Test
    public void test_selectExportMimeType_nullMap() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        assertNull(dataStore.selectExportMimeType(DOCUMENT, null));
        assertNull(dataStore.selectExportMimeType(DOCUMENT, Map.of()));
    }

    /**
     * An unknown native type with an export format still gets the first available target.
     */
    @Test
    public void test_selectExportMimeType_unknownTypeUsesFirstAvailable() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final Map<String, List<String>> formats = Map.of("application/vnd.google-apps.jam", List.of("application/pdf"));
        assertEquals("application/pdf", dataStore.selectExportMimeType("application/vnd.google-apps.jam", formats));
    }

    /**
     * The Apps Script bundle is flattened to "name then source" per script file.
     */
    @Test
    public void test_extractScriptSource() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final String json = "{\"files\":[{\"name\":\"Code\",\"type\":\"server_js\",\"source\":\"function main() {}\"}]}";
        final String result = dataStore.extractScriptSource(json);
        assertTrue("the file name is indexed", result.contains("Code"));
        assertTrue("the source is indexed", result.contains("function main() {}"));
    }

    /**
     * Malformed Apps Script JSON must not abort the file: an empty string is indexed instead.
     */
    @Test
    public void test_extractScriptSource_malformedJson() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        assertEquals("", dataStore.extractScriptSource("not json"));
    }

    /**
     * A Doc is exported as Markdown, so more of its structure survives into the index.
     */
    @Test
    public void test_getFileContents_documentIsExportedAsMarkdown() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final AtomicReference<String> exported = new AtomicReference<>();
        final File file = new File();
        file.setId("d1");
        file.setName("spec");
        file.setMimeType(DOCUMENT);
        try (GSuiteClient client = newExportingClient(exported, "# Heading\n\ntext")) {
            assertEquals("# Heading\n\ntext", dataStore.getFileContents(client, file, true));
        }
        assertEquals("text/markdown", exported.get());
    }

    /**
     * A drawing only exports as an image, so nothing is downloaded and no text is indexed.
     */
    @Test
    public void test_getFileContents_drawingYieldsNoText() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final AtomicReference<String> exported = new AtomicReference<>();
        final File file = new File();
        file.setId("g1");
        file.setName("diagram");
        file.setMimeType("application/vnd.google-apps.drawing");
        try (GSuiteClient client = newExportingClient(exported, "PNG")) {
            assertEquals("", dataStore.getFileContents(client, file, true));
        }
        assertNull("an image export is not worth fetching, and alt=media must not be tried instead", exported.get());
    }

    /**
     * D-16: a Form has no export target, so nothing is exported and nothing is downloaded. The
     * previous implementation fell through to alt=media here, which fails for every native type.
     */
    @Test
    public void test_getFileContents_formIsIndexedAsMetadataOnly() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final AtomicReference<String> exported = new AtomicReference<>();
        final File file = new File();
        file.setId("f1");
        file.setName("survey");
        file.setMimeType(FORM);
        try (GSuiteClient client = newExportingClient(exported, "unused")) {
            assertEquals("", dataStore.getFileContents(client, file, true));
        }
        assertNull("a form must be neither exported nor downloaded with alt=media", exported.get());
    }

    /**
     * A Form is not a failure. It must be indexed with its metadata and must never reach
     * {@code handleProcessingError}, which records the URL in the admin failure list: a domain full
     * of Forms would otherwise fill that list on every single crawl.
     */
    @Test
    public void test_processFile_formDoesNotReachTheFailureHandler() {
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        ComponentUtil.register(new FileTypeHelper(), "fileTypeHelper");
        ComponentUtil.register(new PermissionHelper(), "permissionHelper");

        final AtomicReference<Throwable> capturedError = new AtomicReference<>();
        final AtomicReference<Map<String, Object>> stored = new AtomicReference<>();
        final AtomicReference<String> exported = new AtomicReference<>();

        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected List<String> getFilePermissions(final Map<String, Object> configMap, final DataStoreParams paramMap,
                    final GSuiteClient client, final File file) {
                // This test is about the export target, not about the ACL: the file only needs a
                // role so that the fail-closed rule does not skip it first.
                return Arrays.asList("1owner@example.com");
            }

            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                return resultMap.get(FILE);
            }

            @Override
            protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
                    final CrawlerStatsHelper statsHelper, final Throwable t) {
                capturedError.set(t);
            }
        };

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(GoogleDriveDataStore.MAX_SIZE, Long.valueOf(10000000L));
        configMap.put(GoogleDriveDataStore.IGNORE_FOLDER, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.IGNORE_ERROR, Boolean.FALSE);
        configMap.put(GoogleDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });
        configMap.put(GoogleDriveDataStore.URL_FILTER, null);

        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("file", "file");

        final File file = new File();
        file.setId("f1");
        file.setName("survey");
        file.setMimeType(FORM);
        file.setWebViewLink("https://drive.google.com/open?id=f1");

        try (GSuiteClient client = newExportingClient(exported, "unused")) {
            dataStore.processFile(null, new IndexUpdateCallback() {
                @Override
                public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                    stored.set(dataMap);
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
            }, configMap, new DataStoreParams(), scriptMap, new HashMap<>(), client, file);
        }

        assertNull("a form is not a crawl failure", capturedError.get());
        assertNull("a form must be neither exported nor downloaded with alt=media", exported.get());
        assertNotNull("the form is still indexed with its metadata", stored.get());
    }

    /**
     * D-18: unlike a Form, an export that overruns the size limit is a failure. A native Google file
     * declares no size, so the guard is the only thing standing between the crawl and an unbounded
     * buffer, and indexing the document with empty content would hide the truncation behind a
     * document indistinguishable from one that genuinely has no text. It must reach
     * {@code handleProcessingError} instead, so the operator finds the file in the failure list.
     */
    @Test
    public void test_processFile_oversizedExportIsReportedInsteadOfIndexedEmpty() {
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        ComponentUtil.register(new FileTypeHelper(), "fileTypeHelper");
        ComponentUtil.register(new PermissionHelper(), "permissionHelper");

        final AtomicReference<Throwable> capturedError = new AtomicReference<>();
        final AtomicReference<Map<String, Object>> stored = new AtomicReference<>();

        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected List<String> getFilePermissions(final Map<String, Object> configMap, final DataStoreParams paramMap,
                    final GSuiteClient client, final File file) {
                // This test is about the export size guard, not about the ACL: the file only needs a
                // role so that the fail-closed rule does not skip it first.
                return Arrays.asList("1owner@example.com");
            }

            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                return resultMap.get(FILE);
            }

            @Override
            protected void handleProcessingError(final DataConfig dataConfig, final File file, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, Object> dataMap, final StatsKeyObject statsKey,
                    final CrawlerStatsHelper statsHelper, final Throwable t) {
                capturedError.set(t);
            }
        };

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(GoogleDriveDataStore.MAX_SIZE, Long.valueOf(10000000L));
        configMap.put(GoogleDriveDataStore.IGNORE_FOLDER, Boolean.TRUE);
        // ignore_error defaults to true; an oversized export must be reported even so.
        configMap.put(GoogleDriveDataStore.IGNORE_ERROR, Boolean.TRUE);
        configMap.put(GoogleDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });
        configMap.put(GoogleDriveDataStore.URL_FILTER, null);

        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put("file", "file");

        final File file = new File();
        file.setId("d1");
        file.setName("spec");
        file.setMimeType(DOCUMENT);
        file.setWebViewLink("https://drive.google.com/open?id=d1");

        final MockDriveTransport mockTransport = new MockDriveTransport();
        mockTransport.queue(200, "text/markdown; charset=UTF-8", "0123456789abcdef");
        try (GSuiteClient client = newSizeLimitedClient(mockTransport, 8L)) {
            dataStore.processFile(null, new IndexUpdateCallback() {
                @Override
                public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                    stored.set(dataMap);
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
            }, configMap, new DataStoreParams(), scriptMap, new HashMap<>(), client, file);
        }

        assertNull("an empty document must not be indexed in place of the oversized export", stored.get());
        assertNotNull("an oversized export is a crawl failure", capturedError.get());
        assertTrue(String.valueOf(capturedError.get()), capturedError.get() instanceof MaxLengthExceededException);
        assertTrue(capturedError.get().getMessage(), capturedError.get().getMessage().contains("d1"));
    }
}
