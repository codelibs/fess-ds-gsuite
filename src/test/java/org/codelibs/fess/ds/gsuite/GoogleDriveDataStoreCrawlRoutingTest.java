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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.drive.model.File;

/**
 * Tests the crawl_target routing and the file ID de-duplication of {@link GoogleDriveDataStore}.
 */
public class GoogleDriveDataStoreCrawlRoutingTest extends UnitDsTestCase {

    /** Ids of the files handed to processFile, in submission order. */
    private ConcurrentLinkedQueue<String> processed;

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        processed = new ConcurrentLinkedQueue<>();
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    /**
     * A client whose Drive traversal methods are backed by fixed in-memory data.
     */
    private static final class StubClient extends GSuiteClient {

        /** Files returned by getFiles, i.e. the legacy and per-user path. */
        private final List<File> userFiles;

        /** Files returned by getFilesInDrive, i.e. the shared drive path. */
        private final List<File> driveFiles;

        /** Emails returned by listUsers. */
        private final List<String> users;

        /** Every corpora value getFiles was called with. */
        private final List<String> corporaCalls = new ArrayList<>();

        StubClient(final DataStoreParams params, final MockHttpTransport transport, final List<File> userFiles, final List<File> driveFiles,
                final List<String> users) {
            super(params, transport);
            this.userFiles = userFiles;
            this.driveFiles = driveFiles;
            this.users = users;
        }

        @Override
        public void getFiles(final String q, final String corpora, final String spaces, final String fields,
                final Consumer<File> consumer) {
            corporaCalls.add(corpora);
            userFiles.forEach(consumer);
        }

        @Override
        public void getFilesInDrive(final String driveId, final String q, final String fields, final Consumer<File> consumer) {
            driveFiles.forEach(consumer);
        }

        @Override
        public void getDrives(final Consumer<com.google.api.services.drive.model.Drive> consumer) {
            consumer.accept(new com.google.api.services.drive.model.Drive().setId("drive1").setName("Sales"));
        }

        @Override
        public List<String> listUsers(final String userQuery) {
            return users;
        }

        @Override
        public GSuiteClient forUser(final String userEmail) {
            return this;
        }
    }

    /**
     * Builds a data store that records the file IDs reaching processFile instead of indexing them.
     * @return The data store.
     */
    private GoogleDriveDataStore newRecordingDataStore() {
        return new GoogleDriveDataStore() {
            @Override
            protected void processFile(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
                    final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
                    final GSuiteClient client, final File file) {
                processed.add(file.getId());
            }
        };
    }

    /**
     * Builds the minimum parameter set the client constructor requires.
     * @return The parameters.
     */
    private DataStoreParams newParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put(GSuiteClient.PRIVATE_KEY_PARAM, GSuiteClientTest.VALID_PRIVATE_KEY);
        params.put(GSuiteClient.PRIVATE_KEY_ID_PARAM, "test_key_id");
        params.put(GSuiteClient.CLIENT_EMAIL_PARAM, "svc@example.iam.gserviceaccount.com");
        params.put("impersonate_user", "admin@example.com");
        return params;
    }

    /**
     * Runs storeFiles with the given crawl target against the stub client.
     * @param crawlTarget The crawl target.
     * @param client The stub client.
     */
    private void runStoreFiles(final String crawlTarget, final StubClient client) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(GoogleDriveDataStore.CRAWL_TARGET, crawlTarget);
        newRecordingDataStore().storeFiles(new DataConfig(), null, configMap, client.params, new HashMap<>(), new HashMap<>(), client);
    }

    @Test
    public void test_storeFiles_legacyUsesGetFiles() {
        final MockHttpTransport transport = new MockHttpTransport();
        final StubClient client = new StubClient(newParams(), transport, Arrays.asList(new File().setId("f1"), new File().setId("f2")),
                new ArrayList<>(), new ArrayList<>());
        try {
            runStoreFiles("legacy", client);
            assertEquals(2, processed.size());
            assertEquals(1, client.corporaCalls.size());
            assertEquals("allDrives", client.corporaCalls.get(0));
        } finally {
            client.close();
        }
    }

    @Test
    public void test_storeFiles_sharedDrivesUsesGetFilesInDrive() {
        final MockHttpTransport transport = new MockHttpTransport();
        final StubClient client = new StubClient(newParams(), transport, new ArrayList<>(),
                Arrays.asList(new File().setId("d1"), new File().setId("d2")), new ArrayList<>());
        try {
            runStoreFiles("shared_drives", client);
            assertEquals(2, processed.size());
            assertTrue(client.corporaCalls.isEmpty());
        } finally {
            client.close();
        }
    }

    @Test
    public void test_storeFiles_usersImpersonatesEachUser() {
        final MockHttpTransport transport = new MockHttpTransport();
        final StubClient client = new StubClient(newParams(), transport, Arrays.asList(new File().setId("u1")), new ArrayList<>(),
                Arrays.asList("a@example.com", "b@example.com"));
        try {
            runStoreFiles("users", client);
            // The same file is visible to both users, so it must be indexed once.
            assertEquals(1, processed.size());
            assertEquals(2, client.corporaCalls.size());
            assertEquals("user", client.corporaCalls.get(0));
            assertEquals("user", client.corporaCalls.get(1));
        } finally {
            client.close();
        }
    }

    @Test
    public void test_storeFiles_bothDeduplicatesAcrossPaths() {
        final MockHttpTransport transport = new MockHttpTransport();
        final StubClient client =
                new StubClient(newParams(), transport, Arrays.asList(new File().setId("shared1"), new File().setId("mine1")),
                        Arrays.asList(new File().setId("shared1")), Arrays.asList("a@example.com"));
        try {
            runStoreFiles("both", client);
            final List<String> ids = new ArrayList<>(processed);
            assertEquals(2, ids.size());
            assertTrue(ids.contains("shared1"));
            assertTrue(ids.contains("mine1"));
        } finally {
            client.close();
        }
    }
}
