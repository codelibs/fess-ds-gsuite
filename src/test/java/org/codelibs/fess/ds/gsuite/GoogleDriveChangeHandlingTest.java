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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;
import org.codelibs.fesen.opensearch.index.query.BoolQueryBuilder;
import org.codelibs.fesen.opensearch.index.query.QueryBuilder;
import org.codelibs.fesen.opensearch.index.query.TermQueryBuilder;
import org.codelibs.fesen.opensearch.index.query.WildcardQueryBuilder;

import com.google.api.services.drive.model.Change;
import com.google.api.services.drive.model.File;

/**
 * Tests the incremental traversal and the deletion propagation of {@link GoogleDriveDataStore}.
 */
public class GoogleDriveChangeHandlingTest extends UnitDsTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * A data store whose index writes are captured instead of hitting the search engine.
     */
    protected static class CapturingDataStore extends GoogleDriveDataStore {

        /** Every URL deleted by an exact match. */
        protected final List<String> deletedUrls = Collections.synchronizedList(new ArrayList<>());

        /** Every query handed to the indexing helper. */
        protected final List<QueryBuilder> deleteQueries = Collections.synchronizedList(new ArrayList<>());

        /** The ids of the files that reached processFile. */
        protected final Queue<String> processedIds = new ConcurrentLinkedQueue<>();

        /** The crawl state handed out by newCrawlState, or null when the crawl is not incremental. */
        protected DriveCrawlStateTest.CapturingState crawlState;

        @Override
        protected long deleteDocumentByUrl(final String url) {
            deletedUrls.add(url);
            return 1L;
        }

        @Override
        protected long deleteDocumentByQuery(final QueryBuilder queryBuilder) {
            deleteQueries.add(queryBuilder);
            return 1L;
        }

        @Override
        protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
                final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
                final GSuiteClient client, final File file) {
            processedIds.add(file.getId());
        }

        @Override
        protected DriveCrawlState newCrawlState(final DataConfig dataConfig) {
            crawlState = new DriveCrawlStateTest.CapturingState(dataConfig);
            return crawlState;
        }
    }

    /**
     * A client whose change feed and file listing are backed by fixed in-memory data.
     * <p>
     * The constructor parameters are named distinctly from the protected fields of
     * {@link GSuiteClient}, which would otherwise be shadowed inside this body.
     * </p>
     */
    protected static class StubClient extends GSuiteClient {

        /** The names of the Drive calls that were made, in order. */
        protected final List<String> calls = new ArrayList<>();

        /** The files a full listing yields. */
        protected final List<File> listedFiles = new ArrayList<>();

        /** The changes the feed yields. */
        protected final List<Change> feed = new ArrayList<>();

        /** The token changes.getStartPageToken returns. */
        protected String startPageToken = "T-NEW";

        /** The token changes.list returns on its last page. */
        protected String newStartPageToken = "T-NEXT";

        /** The failure changes.list raises, or null. */
        protected RuntimeException changesFailure;

        /** The token changes.list was resumed from. */
        protected String requestedPageToken;

        /**
         * Constructs a new StubClient.
         *
         * @param stubParams The parameters.
         * @param stubTransport The transport.
         */
        protected StubClient(final DataStoreParams stubParams, final MockDriveTransport stubTransport) {
            super(stubParams, stubTransport);
        }

        @Override
        public void getFiles(final String q, final String corpora, final String spaces, final String fields,
                final Consumer<File> consumer) {
            calls.add("getFiles");
            listedFiles.forEach(consumer);
        }

        @Override
        public String getStartPageToken(final String driveId) {
            calls.add("getStartPageToken");
            return startPageToken;
        }

        @Override
        public String getChanges(final String pageToken, final String driveId, final Consumer<Change> consumer) {
            calls.add("getChanges");
            requestedPageToken = pageToken;
            if (changesFailure != null) {
                throw changesFailure;
            }
            feed.forEach(consumer);
            return newStartPageToken;
        }
    }

    /**
     * Builds a data config with the given raw handler parameter and a config id.
     *
     * @param handlerParameter The raw handler parameter.
     * @return The data config.
     */
    protected static DataConfig newDataConfig(final String handlerParameter) {
        final DataConfig dataConfig = new DataConfig();
        dataConfig.setId("1");
        dataConfig.setHandlerParameter(handlerParameter);
        return dataConfig;
    }

    /**
     * Builds a stub client bound to a mock transport that is never actually used.
     *
     * @return The client.
     */
    protected static StubClient newStubClient() {
        return new StubClient(GSuiteClientApiTest.newParams(), new MockDriveTransport());
    }

    /**
     * Returns the {@code crawl_signature} line that matches the parameters {@link #runStoreFiles}
     * crawls with.
     * <p>
     * A stored token is only resumed when the signature proves the configuration has not changed
     * since the token was taken, so a fixture that exercises the change feed has to carry the current
     * one. A fixture without it is the unsigned state an older configuration is left in, which
     * deliberately falls back to a full crawl.
     * </p>
     *
     * @return The handler parameter line, without a trailing newline.
     */
    protected static String currentSignatureLine() {
        return DriveCrawlState.CRAWL_SIGNATURE + "=" + new GoogleDriveDataStore().buildCrawlSignature(GSuiteClientApiTest.newParams());
    }

    /**
     * Runs a legacy scope crawl against the stub client.
     *
     * @param dataStore The data store.
     * @param dataConfig The data configuration.
     * @param client The stub client.
     */
    protected static void runStoreFiles(final CapturingDataStore dataStore, final DataConfig dataConfig, final StubClient client) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(GoogleDriveDataStore.CRAWL_TARGET, GoogleDriveDataStore.TARGET_LEGACY);
        dataStore.storeFiles(dataConfig, null, configMap, client.params, new HashMap<>(), new HashMap<>(), client);
    }

    /**
     * A removed change carries no file at all, so the document is dropped by a query that pairs the
     * config id with a wildcard on the indexed URL. The config id filter is what keeps a Drive file id
     * that also appears in another data store's URL out of the deletion.
     */
    @Test
    public void test_deleteFileById_scopesTheWildcardByConfigId() {
        final CapturingDataStore dataStore = new CapturingDataStore();
        final DataConfig dataConfig = newDataConfig("incremental=true");
        dataStore.deleteFileById(dataConfig, "F1");

        assertTrue("no URL can be guessed from an id alone", dataStore.deletedUrls.isEmpty());
        assertEquals(1, dataStore.deleteQueries.size());
        final BoolQueryBuilder query = (BoolQueryBuilder) dataStore.deleteQueries.get(0);
        assertEquals(2, query.filter().size());

        final TermQueryBuilder configIdFilter = (TermQueryBuilder) query.filter().get(0);
        assertEquals(ComponentUtil.getFessConfig().getIndexFieldConfigId(), configIdFilter.fieldName());
        assertEquals(dataConfig.getConfigId(), configIdFilter.value());

        final WildcardQueryBuilder urlFilter = (WildcardQueryBuilder) query.filter().get(1);
        assertEquals(ComponentUtil.getFessConfig().getIndexFieldUrl(), urlFilter.fieldName());
        assertEquals("*F1*", urlFilter.value());
    }

    /**
     * A trashed change still carries the file, so the exact URL that was indexed is known and no
     * wildcard is needed. The indexed URL is the webViewLink, which is mime type dependent and cannot
     * be rebuilt from the id.
     */
    @Test
    public void test_deleteFile_usesTheIndexedUrl() {
        final CapturingDataStore dataStore = new CapturingDataStore();
        final File file = new File();
        file.setId("F2");
        file.setWebViewLink("https://docs.google.com/document/d/F2/edit");
        dataStore.deleteFile(new HashMap<>(), new DataStoreParams(), file);

        assertEquals(List.of("https://docs.google.com/document/d/F2/edit"), dataStore.deletedUrls);
        assertTrue("an exact URL needs no wildcard", dataStore.deleteQueries.isEmpty());
    }

    /**
     * A blank file id must be ignored rather than turned into a wildcard that matches everything.
     */
    @Test
    public void test_deleteFileById_ignoresBlankId() {
        final CapturingDataStore dataStore = new CapturingDataStore();
        final DataConfig dataConfig = newDataConfig("incremental=true");
        dataStore.deleteFileById(dataConfig, null);
        dataStore.deleteFileById(dataConfig, "  ");
        assertTrue(dataStore.deleteQueries.isEmpty());
        assertTrue(dataStore.deletedUrls.isEmpty());
    }

    /**
     * Without a config id the wildcard cannot be scoped, so nothing is deleted: an unscoped
     * {@code *id*} would reach the documents of every other data store.
     */
    @Test
    public void test_deleteFileById_requiresAConfigId() {
        final CapturingDataStore dataStore = new CapturingDataStore();
        dataStore.deleteFileById(null, "F1");
        dataStore.deleteFileById(new DataConfig(), "F1");
        assertTrue(dataStore.deleteQueries.isEmpty());
        assertTrue(dataStore.deletedUrls.isEmpty());
    }

    /**
     * Both removed=true and trashed=true mean "drop it from the index".
     */
    @Test
    public void test_isDeletion() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();

        final Change removed = new Change();
        removed.setFileId("F1");
        removed.setRemoved(Boolean.TRUE);
        assertTrue("removed", dataStore.isDeletion(removed));

        final Change trashed = new Change();
        trashed.setFileId("F2");
        final File trashedFile = new File();
        trashedFile.setId("F2");
        trashedFile.setTrashed(Boolean.TRUE);
        trashed.setFile(trashedFile);
        assertTrue("trashed", dataStore.isDeletion(trashed));

        final Change updated = new Change();
        updated.setFileId("F3");
        final File updatedFile = new File();
        updatedFile.setId("F3");
        updatedFile.setTrashed(Boolean.FALSE);
        updated.setFile(updatedFile);
        assertFalse("an ordinary update", dataStore.isDeletion(updated));

        final Change driveChange = new Change();
        driveChange.setChangeType("drive");
        assertFalse("a drive level change with no file is not a deletion", dataStore.isDeletion(driveChange));
    }

    /**
     * The fallback URL is the one getUrl produces when webViewLink is missing, so deletion and
     * indexing agree on it.
     */
    @Test
    public void test_getFallbackUrl_matchesGetUrl() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final File file = new File();
        file.setId("F4");
        assertEquals(dataStore.getFallbackUrl("F4"), dataStore.getUrl(new HashMap<>(), new DataStoreParams(), file));
    }

    /**
     * A non-incremental configuration keeps the full listing and never touches a crawl state.
     */
    @Test
    public void test_storeFiles_fullCrawlWhenNotIncremental() {
        final CapturingDataStore dataStore = new CapturingDataStore();
        final StubClient client = newStubClient();
        client.listedFiles.add(new File().setId("f1"));
        try {
            runStoreFiles(dataStore, newDataConfig("crawl_target=legacy"), client);
            assertEquals(List.of("getFiles"), client.calls);
            assertNull("no token is taken by a full crawl", dataStore.crawlState);
            assertEquals(1, dataStore.processedIds.size());
        } finally {
            client.close();
        }
    }

    /**
     * A stored token is resumed: updates are indexed, trashed files are deleted by their indexed URL,
     * removed files by the config scoped wildcard, and the token returned on the last page is saved.
     */
    @Test
    public void test_storeFiles_incrementalAppliesTheChangeFeed() {
        final CapturingDataStore dataStore = new CapturingDataStore();
        final StubClient client = newStubClient();

        final Change updated = new Change();
        updated.setFileId("f1");
        updated.setFile(new File().setId("f1").setWebViewLink("https://docs.google.com/document/d/f1/edit"));
        client.feed.add(updated);

        final Change trashed = new Change();
        trashed.setFileId("f2");
        trashed.setFile(new File().setId("f2").setTrashed(Boolean.TRUE).setWebViewLink("https://docs.google.com/spreadsheets/d/f2/edit"));
        client.feed.add(trashed);

        final Change removed = new Change();
        removed.setFileId("f3");
        removed.setRemoved(Boolean.TRUE);
        client.feed.add(removed);

        try {
            runStoreFiles(dataStore,
                    newDataConfig("incremental=true\nstart_page_tokens={\"user:legacy\":\"T1\"}\n" + currentSignatureLine()), client);

            assertEquals(List.of("getChanges"), client.calls);
            assertEquals("T1", client.requestedPageToken);
            assertEquals(List.of("f1"), new ArrayList<>(dataStore.processedIds));
            assertEquals(List.of("https://docs.google.com/spreadsheets/d/f2/edit"), dataStore.deletedUrls);
            assertEquals(1, dataStore.deleteQueries.size());
            assertEquals("T-NEXT", dataStore.crawlState.getToken("user:legacy"));
            assertEquals("the state is written back once", 1, dataStore.crawlState.updates.get());
        } finally {
            client.close();
        }
    }

    /**
     * The first incremental run has no anchor, so it must crawl everything and take a token, not
     * index nothing and declare success. The token is taken before the listing, so a change made
     * while the listing runs is replayed by the next run rather than lost.
     */
    @Test
    public void test_storeFiles_firstIncrementalRunCrawlsFullyAndAnchors() {
        final CapturingDataStore dataStore = new CapturingDataStore();
        final StubClient client = newStubClient();
        client.listedFiles.add(new File().setId("f1"));
        try {
            runStoreFiles(dataStore, newDataConfig("incremental=true"), client);

            assertEquals(List.of("getStartPageToken", "getFiles"), client.calls);
            assertEquals(1, dataStore.processedIds.size());
            assertEquals("T-NEW", dataStore.crawlState.getToken("user:legacy"));
        } finally {
            client.close();
        }
    }

    /**
     * A change feed failure discards the stored token and resynchronizes the scope from scratch.
     */
    @Test
    public void test_storeFiles_changeFeedFailureFallsBackToAFullCrawl() {
        final CapturingDataStore dataStore = new CapturingDataStore();
        final StubClient client = newStubClient();
        client.changesFailure = new DataStoreException("boom");
        client.listedFiles.add(new File().setId("f1"));
        try {
            runStoreFiles(dataStore,
                    newDataConfig("incremental=true\nstart_page_tokens={\"user:legacy\":\"T1\"}\n" + currentSignatureLine()), client);

            assertEquals(List.of("getChanges", "getStartPageToken", "getFiles"), client.calls);
            assertEquals(1, dataStore.processedIds.size());
            assertEquals("T-NEW", dataStore.crawlState.getToken("user:legacy"));
        } finally {
            client.close();
        }
    }
}
