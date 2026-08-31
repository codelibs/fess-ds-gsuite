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

import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests the {@code crawl_target} validation of {@link GoogleDriveDataStore}.
 */
public class GoogleDriveDataStoreCrawlTargetTest extends UnitDsTestCase {

    /** A scopes value that grants both Drive and Admin SDK directory reads. */
    private static final String DRIVE_AND_DIRECTORY_SCOPES =
            "https://www.googleapis.com/auth/drive.readonly, https://www.googleapis.com/auth/admin.directory.user.readonly";

    /** The data store under test. */
    private GoogleDriveDataStore dataStore;

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new GoogleDriveDataStore();
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Test
    public void test_getCrawlTarget_defaultsToSharedDrives() {
        final DataStoreParams params = new DataStoreParams();
        params.put("impersonate_user", "admin@example.com");
        assertEquals("shared_drives", dataStore.getCrawlTarget(params));
    }

    @Test
    public void test_getCrawlTarget_legacyDoesNotRequireImpersonateUser() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", "legacy");
        assertEquals("legacy", dataStore.getCrawlTarget(params));
    }

    @Test
    public void test_getCrawlTarget_users() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", "users");
        params.put("impersonate_user", "admin@example.com");
        params.put("scopes", DRIVE_AND_DIRECTORY_SCOPES);
        assertEquals("users", dataStore.getCrawlTarget(params));
    }

    @Test
    public void test_getCrawlTarget_both() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", "both");
        params.put("impersonate_user", "admin@example.com");
        params.put("scopes", DRIVE_AND_DIRECTORY_SCOPES);
        assertEquals("both", dataStore.getCrawlTarget(params));
    }

    @Test
    public void test_getCrawlTarget_trimsValue() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", " users ");
        params.put("impersonate_user", "admin@example.com");
        params.put("scopes", DRIVE_AND_DIRECTORY_SCOPES);
        assertEquals("users", dataStore.getCrawlTarget(params));
    }

    @Test
    public void test_getCrawlTarget_rejectsUnknownValue() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", "everything");
        params.put("impersonate_user", "admin@example.com");
        try {
            dataStore.getCrawlTarget(params);
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("crawl_target"));
            assertTrue(e.getMessage(), e.getMessage().contains("everything"));
        }
    }

    @Test
    public void test_getCrawlTarget_requiresImpersonateUserForSharedDrives() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", "shared_drives");
        try {
            dataStore.getCrawlTarget(params);
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("impersonate_user"));
        }
    }

    @Test
    public void test_getCrawlTarget_requiresImpersonateUserByDefault() {
        try {
            dataStore.getCrawlTarget(new DataStoreParams());
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("impersonate_user"));
        }
    }

    /**
     * The default scope list is Drive only, so {@code users} can never enumerate the directory.
     * Reject it at startup instead of failing with an opaque 403 once the crawl has begun.
     */
    @Test
    public void test_getCrawlTarget_usersRequiresAdminDirectoryScope() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", "users");
        params.put("impersonate_user", "admin@example.com");
        try {
            dataStore.getCrawlTarget(params);
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("scopes"));
            assertTrue(e.getMessage(), e.getMessage().contains("https://www.googleapis.com/auth/admin.directory.user.readonly"));
            assertTrue(e.getMessage(), e.getMessage().contains("users"));
        }
    }

    @Test
    public void test_getCrawlTarget_bothRequiresAdminDirectoryScope() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", "both");
        params.put("impersonate_user", "admin@example.com");
        try {
            dataStore.getCrawlTarget(params);
            fail("Expected DataStoreException");
        } catch (final DataStoreException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("scopes"));
            assertTrue(e.getMessage(), e.getMessage().contains("https://www.googleapis.com/auth/admin.directory.user.readonly"));
        }
    }

    /** The scope check follows the same resolution as the client, so whitespace is trimmed. */
    @Test
    public void test_getCrawlTarget_usersAcceptsUntrimmedAdminDirectoryScope() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", "users");
        params.put("impersonate_user", "admin@example.com");
        params.put("scopes",
                " https://www.googleapis.com/auth/admin.directory.user.readonly , https://www.googleapis.com/auth/drive.readonly ");
        assertEquals("users", dataStore.getCrawlTarget(params));
    }

    /** Shared drives use the Drive API alone, so the directory scope is not required there. */
    @Test
    public void test_getCrawlTarget_sharedDrivesDoesNotRequireAdminDirectoryScope() {
        final DataStoreParams params = new DataStoreParams();
        params.put("crawl_target", "shared_drives");
        params.put("impersonate_user", "admin@example.com");
        assertEquals("shared_drives", dataStore.getCrawlTarget(params));
    }
}
