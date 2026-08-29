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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.junit.jupiter.api.Test;

import com.google.api.services.drive.model.Change;
import com.google.api.services.drive.model.File;

/**
 * Tests the crawl scope signature that guards the stored start page tokens.
 * <p>
 * The signature is deliberately a function of the <em>configuration</em> only, never of the
 * discovered domain state: a signature over the enumerated drives or users would discard every
 * scope's token whenever any single drive or user appears or disappears, which turns every
 * subsequent run into a full crawl. Scope set changes need no signature, because a scope key with
 * no stored token already routes to a full listing in {@code storeScope}.
 * </p>
 */
public class GoogleDriveCrawlSignatureTest extends UnitDsTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * A state whose write back is suppressed, so no test touches the config index.
     */
    protected static class SilentState extends DriveCrawlState {

        /**
         * Constructs a new SilentState.
         *
         * @param dataConfig The data configuration.
         */
        protected SilentState(final DataConfig dataConfig) {
            super(dataConfig);
        }

        @Override
        protected void updateDataConfig(final DataConfig dataConfig) {
            // captured, never persisted in tests
        }
    }

    /**
     * A client whose scope traversal is backed by fixed in-memory data.
     * <p>
     * The fields are named distinctly from the {@code protected} fields of {@link GSuiteClient}
     * ({@code drive}, {@code httpTransport}, {@code params}, {@code credentials},
     * {@code requestInitializer}) so that nothing here shadows one of them.
     * </p>
     */
    protected static final class ScopeRecordingClient extends GSuiteClient {

        /** Drive ids handed to getFilesInDrive, i.e. the full listing path. */
        protected final List<String> fullyListedDrives = new ArrayList<>();

        /** Page tokens handed to getChanges, i.e. the incremental path. */
        protected final List<String> resumedTokens = new ArrayList<>();

        /**
         * Constructs a new ScopeRecordingClient.
         *
         * @param clientParams The parameters for the client.
         */
        protected ScopeRecordingClient(final DataStoreParams clientParams) {
            super(clientParams, new MockDriveTransport());
        }

        @Override
        public void getDrives(final Consumer<com.google.api.services.drive.model.Drive> consumer) {
            consumer.accept(new com.google.api.services.drive.model.Drive().setId("drive1").setName("Sales"));
        }

        @Override
        public void getFilesInDrive(final String driveId, final String q, final String fields, final Consumer<File> consumer) {
            fullyListedDrives.add(driveId);
            consumer.accept(new File().setId("f1"));
        }

        @Override
        public String getChanges(final String pageToken, final String driveId, final Consumer<Change> consumer) {
            resumedTokens.add(pageToken);
            return "NEWTOKEN";
        }

        @Override
        public String getStartPageToken(final String driveId) {
            return "ANCHOR";
        }
    }

    /**
     * Builds a data config with the given raw handler parameter.
     *
     * @param handlerParameter The raw handler parameter.
     * @return The data config.
     */
    protected static DataConfig newDataConfig(final String handlerParameter) {
        final DataConfig dataConfig = new DataConfig();
        dataConfig.setHandlerParameter(handlerParameter);
        return dataConfig;
    }

    /**
     * Builds a parameter map carrying every scope parameter plus the mandatory credentials.
     *
     * @return The parameters.
     */
    protected static DataStoreParams newParams() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(GSuiteClient.PRIVATE_KEY_PARAM, GSuiteClientTest.VALID_PRIVATE_KEY);
        paramMap.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        paramMap.put(GSuiteClient.CLIENT_EMAIL_PARAM, "svc@example.iam.gserviceaccount.com");
        paramMap.put("crawl_target", "shared_drives");
        paramMap.put("impersonate_user", "admin@example.com");
        paramMap.put("user_query", "isSuspended=false");
        paramMap.put("query", "mimeType!='application/vnd.google-apps.folder'");
        paramMap.put("corpora", "allDrives");
        paramMap.put("spaces", "drive");
        return paramMap;
    }

    /**
     * Copies the parameters with one entry replaced.
     *
     * @param key The parameter key to replace.
     * @param value The new value.
     * @return The modified copy.
     */
    protected static DataStoreParams paramsWith(final String key, final String value) {
        final DataStoreParams paramMap = newParams();
        paramMap.put(key, value);
        return paramMap;
    }

    /**
     * The signature must be one line of hex so it fits a single handlerParameter entry.
     */
    @Test
    public void test_buildCrawlSignature_isSingleLineHex() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final String signature = dataStore.buildCrawlSignature(newParams());
        assertEquals("SHA-256 hex", 64, signature.length());
        assertTrue("hex only", signature.matches("[0-9a-f]{64}"));
    }

    /**
     * The same configuration must yield the same signature, whatever order the map was filled in.
     */
    @Test
    public void test_buildCrawlSignature_isStable() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        assertEquals(dataStore.buildCrawlSignature(newParams()), dataStore.buildCrawlSignature(newParams()));

        final DataStoreParams reordered = new DataStoreParams();
        reordered.put("spaces", "drive");
        reordered.put("query", "mimeType!='application/vnd.google-apps.folder'");
        reordered.put("corpora", "allDrives");
        reordered.put("user_query", "isSuspended=false");
        reordered.put("impersonate_user", "admin@example.com");
        reordered.put("crawl_target", "shared_drives");
        assertEquals("insertion order must not matter", dataStore.buildCrawlSignature(newParams()),
                dataStore.buildCrawlSignature(reordered));
    }

    /**
     * Surrounding whitespace on the crawl target must not change the signature: getCrawlTarget trims
     * it, so " both" and "both" select the very same traversal.
     */
    @Test
    public void test_buildCrawlSignature_trimsCrawlTarget() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        assertEquals(dataStore.buildCrawlSignature(paramsWith("crawl_target", "both")),
                dataStore.buildCrawlSignature(paramsWith("crawl_target", " both ")));
    }

    /**
     * Every parameter that decides which files a scope yields must change the signature.
     */
    @Test
    public void test_buildCrawlSignature_changesWithEveryScopeParameter() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final String base = dataStore.buildCrawlSignature(newParams());
        assertFalse("crawl_target", base.equals(dataStore.buildCrawlSignature(paramsWith("crawl_target", "both"))));
        assertFalse("impersonate_user", base.equals(dataStore.buildCrawlSignature(paramsWith("impersonate_user", "other@example.com"))));
        assertFalse("user_query", base.equals(dataStore.buildCrawlSignature(paramsWith("user_query", "orgUnitPath=/Sales"))));
        assertFalse("query", base.equals(dataStore.buildCrawlSignature(paramsWith("query", "trashed=false"))));
        assertFalse("corpora", base.equals(dataStore.buildCrawlSignature(paramsWith("corpora", "user"))));
        assertFalse("spaces", base.equals(dataStore.buildCrawlSignature(paramsWith("spaces", "appDataFolder"))));
    }

    /**
     * No credential may be an input. The signature is written into handlerParameter, which is stored
     * in the config index and rendered in the admin UI, so rotating a key must not move it: if it
     * did, the value would be derived from the secret.
     */
    @Test
    public void test_buildCrawlSignature_ignoresEverySecretParameter() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final String base = dataStore.buildCrawlSignature(newParams());
        for (final String secretKey : GoogleDriveDataStore.SECRET_PARAM_KEYS) {
            assertEquals("'" + secretKey + "' must not be an input to the signature", base,
                    dataStore.buildCrawlSignature(paramsWith(secretKey, "rotated-" + secretKey)));
        }
    }

    /**
     * A parameter that does not narrow the crawl scope must not invalidate the tokens.
     */
    @Test
    public void test_buildCrawlSignature_ignoresNonScopeParameters() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final String base = dataStore.buildCrawlSignature(newParams());
        assertEquals("number_of_threads", base, dataStore.buildCrawlSignature(paramsWith("number_of_threads", "8")));
        assertEquals("max_size", base, dataStore.buildCrawlSignature(paramsWith("max_size", "1")));
    }

    /**
     * An unchanged configuration keeps the stored tokens.
     */
    @Test
    public void test_applyCrawlSignature_keepsTokensWhenCompatible() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DataStoreParams paramMap = newParams();
        final String signature = dataStore.buildCrawlSignature(paramMap);
        final DriveCrawlState state =
                new SilentState(newDataConfig("start_page_tokens={\"drive:d1\":\"111\"}\ncrawl_signature=" + signature));
        dataStore.applyCrawlSignature(state, paramMap);
        assertEquals("111", state.getToken("drive:d1"));
    }

    /**
     * A changed configuration must discard every token so the run degrades to a full crawl.
     */
    @Test
    public void test_applyCrawlSignature_discardsTokensWhenIncompatible() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DriveCrawlState state = new SilentState(newDataConfig("start_page_tokens={\"drive:d1\":\"111\"}\ncrawl_signature=STALE"));
        final DataStoreParams paramMap = newParams();
        dataStore.applyCrawlSignature(state, paramMap);
        assertNull("the stale token is gone", state.getToken("drive:d1"));
        assertTrue("the new signature is recorded", state.isCompatible(dataStore.buildCrawlSignature(paramMap)));
    }

    /**
     * An absent signature is the state left by a version that never wrote one. It is unreadable, not
     * proven equal, so it must discard rather than resume.
     */
    @Test
    public void test_applyCrawlSignature_discardsTokensWhenSignatureAbsent() {
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore();
        final DriveCrawlState state = new SilentState(newDataConfig("start_page_tokens={\"drive:d1\":\"111\"}"));
        dataStore.applyCrawlSignature(state, newParams());
        assertNull("an unsigned token set must not be resumed", state.getToken("drive:d1"));
    }

    /**
     * A null state must be tolerated so a full crawl never has to guard the call.
     */
    @Test
    public void test_applyCrawlSignature_nullState() {
        new GoogleDriveDataStore().applyCrawlSignature(null, newParams());
    }

    /**
     * Runs storeFiles over one shared drive against the recording client.
     *
     * @param dataConfig The data configuration carrying the persisted state.
     * @param paramMap The parameters for the data store.
     * @param client The recording client.
     */
    protected void runStoreFiles(final DataConfig dataConfig, final DataStoreParams paramMap, final ScopeRecordingClient client) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(GoogleDriveDataStore.CRAWL_TARGET, "shared_drives");
        final GoogleDriveDataStore dataStore = new GoogleDriveDataStore() {
            @Override
            protected DriveCrawlState newCrawlState(final DataConfig config) {
                return new SilentState(config);
            }

            @Override
            protected void processFile(final DataConfig config, final IndexUpdateCallback callback, final Map<String, Object> configs,
                    final DataStoreParams params, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
                    final GSuiteClient gsuiteClient, final File file) {
                // indexing is out of scope here
            }
        };
        dataStore.storeFiles(dataConfig, null, configMap, paramMap, new HashMap<>(), new HashMap<>(), client);
    }

    /**
     * End to end: a stale signature in the stored state must make the real crawl list the drive in
     * full instead of resuming its change feed.
     */
    @Test
    public void test_storeFiles_staleSignatureForcesAFullCrawl() {
        final DataStoreParams paramMap = newParams();
        final ScopeRecordingClient client = new ScopeRecordingClient(paramMap);
        try {
            runStoreFiles(newDataConfig("incremental=true\nstart_page_tokens={\"drive:drive1\":\"OLDTOKEN\"}\ncrawl_signature=STALE"),
                    paramMap, client);
            assertTrue("the change feed must not be resumed under a stale signature", client.resumedTokens.isEmpty());
            assertEquals("the drive must be listed in full", List.of("drive1"), client.fullyListedDrives);
        } finally {
            client.close();
        }
    }

    /**
     * End to end: a matching signature must let the real crawl resume the change feed.
     */
    @Test
    public void test_storeFiles_matchingSignatureResumesTheChangeFeed() {
        final DataStoreParams paramMap = newParams();
        final String signature = new GoogleDriveDataStore().buildCrawlSignature(paramMap);
        final ScopeRecordingClient client = new ScopeRecordingClient(paramMap);
        try {
            runStoreFiles(
                    newDataConfig("incremental=true\nstart_page_tokens={\"drive:drive1\":\"OLDTOKEN\"}\ncrawl_signature=" + signature),
                    paramMap, client);
            assertEquals("the change feed must be resumed", List.of("OLDTOKEN"), client.resumedTokens);
            assertTrue("no full listing is needed", client.fullyListedDrives.isEmpty());
        } finally {
            client.close();
        }
    }
}
